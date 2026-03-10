from __future__ import annotations

import argparse
import csv
import logging
import os
import re
from pathlib import Path

from db_code import db_users as acl
from db_code.db_utils import ensure_table, get_connection, load_env, upsert_rows
from db_code.ddl.tables import make_family_table_ddl
from db_code.ddl.views import make_entries_view_ddl, make_presence_view_ddl
from db_code.parsing.normalize import _clean_cell, normalize_sample_number

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV = PROJECT_ROOT / ".env"

FT_TABLE = "ft_icr_ms"
FT_MODES = {
    "appipos": "APPIpos",
    "esineg": "ESIneg",
    "esipos": "ESIpos",
}

ISOTOPE_TABLE_BY_KIND = {
    "co2_werte": "isotope_co2_werte",
    "hd_werte": "isotope_hd_werte",
}

ISOTOPE_TABLE_BY_KIND_NORM = {
    _norm: table for _norm, table in ((re.sub(r"[^a-z0-9]+", "", k.lower()), v) for k, v in ISOTOPE_TABLE_BY_KIND.items())
}

FRACTION_MAP = {
    "wo": "whole crude oil",
    "wholeoil": "whole crude oil",
    "whole_oil": "whole crude oil",
    "aliphatics": "aliphatic",
    "aliphatic": "aliphatic",
}

INTERNAL_TABLES = {
    "schema_migrations",
    "subjects",
    "group_members",
    "sample_acl",
    "projects",
    "sample_projects",
    "project_acl",
    "alkanes",
    "wo",
    "whole_oil_gc",
}

SKIP_OPERATOR_DIRS = {
    "final",
    "masslists",
    "smartformula",
}


def _configure_logging(level_name: str = "INFO") -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")


def _norm_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _find_token_index(parts: tuple[str, ...], token: str) -> int | None:
    target = _norm_token(token)
    for idx, part in enumerate(parts):
        if _norm_token(part) == target:
            return idx
    return None


def _extract_ft_mode(path: str) -> str | None:
    p = Path(path)
    for part in p.parts:
        key = _norm_token(part)
        if key in FT_MODES:
            return FT_MODES[key]

    m = re.search(r"_(APPIpos|ESIneg|ESIpos)\.csv$", p.name, flags=re.IGNORECASE)
    if m:
        return FT_MODES.get(_norm_token(m.group(1)))
    return None


def _extract_ft_operator(path: str) -> str:
    p = Path(path)
    mode = _extract_ft_mode(path)
    if not mode:
        return "unknown"

    parts = p.parts
    idx = _find_token_index(parts, mode)
    if idx is None:
        return "unknown"

    for cand in parts[idx + 1 : idx + 5]:
        if cand.lower() in SKIP_OPERATOR_DIRS:
            continue
        if cand.lower().endswith(".csv"):
            continue
        return cand
    return "unknown"


def _extract_isotope_kind(path: str) -> str | None:
    p = Path(path)
    for part in p.parts:
        key = _norm_token(part)
        if key in ISOTOPE_TABLE_BY_KIND_NORM:
            return key
    return None


def _extract_isotope_operator(path: str) -> str:
    p = Path(path)
    kind = _extract_isotope_kind(path)
    if kind:
        idx = _find_token_index(p.parts, kind)
        if idx is not None and idx + 1 < len(p.parts):
            next_part = p.parts[idx + 1]
            if not next_part.lower().endswith(".csv"):
                return next_part

    stem = p.stem
    prefix = stem.split("_", 1)[0].strip()
    return prefix or "unknown"


def _extract_fraction_from_filename(path: str) -> str | None:
    stem = Path(path).stem
    m = re.search(r"_([^_]+)_combined", stem, flags=re.IGNORECASE)
    if not m:
        return None
    token = _norm_token(m.group(1))
    return FRACTION_MAP.get(token, m.group(1).strip().lower())


def _list_samplenumber_tables(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.table_name
            FROM information_schema.tables t
            WHERE t.table_schema = 'public'
              AND t.table_type = 'BASE TABLE'
                            AND NOT t.table_name LIKE 'pg_%%'
                            AND NOT t.table_name LIKE 'sql_%%'
              AND t.table_name <> ALL(%s)
              AND EXISTS (
                  SELECT 1
                  FROM information_schema.columns c
                  WHERE c.table_schema = 'public'
                    AND c.table_name = t.table_name
                    AND c.column_name = 'samplenumber'
              )
            ORDER BY t.table_name
            """,
            (list(INTERNAL_TABLES),),
        )
        return [r[0] for r in cur.fetchall()]


def _ensure_family_tables_and_views(conn, tables: list[str]) -> None:
    for table in sorted(set(tables)):
        ensure_table(conn, make_family_table_ddl(table, table))
        ensure_table(conn, make_entries_view_ddl(table=table, json_col=table, view_name=f"{table}_entries"))


def _ensure_presence_and_acl(conn) -> None:
    table_names = _list_samplenumber_tables(conn)
    if table_names:
        with conn.cursor() as cur:
            cur.execute("DROP VIEW IF EXISTS public.analysis_presence_simple CASCADE;")
        conn.commit()
        ensure_table(
            conn,
            make_presence_view_ddl(
                view_name="analysis_presence_simple",
                tables=table_names,
            ),
        )

    acl.ensure_identity_schema(conn)
    acl.ensure_rls_for_tables(conn, table_names)
    acl.ensure_project(conn, "open", title="Open data")
    acl.ensure_subject_group(conn, "open")
    acl.grant_project_access(
        conn,
        project_id="open",
        subject_id="open",
        as_group=True,
        can_read=True,
        can_write=False,
    )
    linked = acl.link_samples_to_project(conn, project_id="open", tables=table_names)
    logger.info("Linked %d sample keys to project 'open'", linked)


def _to_entry(
    *,
    raw_sample: str,
    operator: str,
    data: dict,
    sample_type: str | None,
    fraction: str | None,
    instrument: str | None,
    data_type: str | None,
    method: str | None,
    notes: str | None,
) -> dict:
    return {
        "raw_sample": raw_sample,
        "name": operator,
        "measured_by": operator,
        "type": sample_type,
        "date": None,
        "fraction": fraction,
        "instrument": instrument,
        "data_type": data_type,
        "method": method,
        "notes": notes,
        "data": data,
    }


def _discover_ft_files(root_dir: str) -> list[str]:
    root = Path(root_dir)
    if not root.exists():
        raise FileNotFoundError(root_dir)

    out: list[str] = []
    for p in root.rglob("*.csv"):
        parts_norm = {_norm_token(part) for part in p.parts}
        if "final" not in parts_norm:
            continue
        mode = _extract_ft_mode(str(p))
        if not mode:
            continue
        if "signallist" not in p.name.lower():
            continue
        out.append(str(p))
    return sorted(set(out))


def _build_ft_rows(csv_path: str) -> list[dict]:
    sample = normalize_sample_number(Path(csv_path).name)
    if not sample:
        logger.warning("Skipping FT file without sample id in filename: %s", csv_path)
        return []

    mode = _extract_ft_mode(csv_path)
    operator = _extract_ft_operator(csv_path)
    if not mode:
        logger.warning("Skipping FT file without acquisition mode: %s", csv_path)
        return []

    entries: list[dict] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            payload: dict[str, object] = {}
            for k, v in row.items():
                if k is None:
                    continue
                key = str(k).strip()
                if not key:
                    continue
                cleaned = _clean_cell(v)
                if cleaned is None:
                    continue
                payload[key] = cleaned

            if not payload:
                continue

            entry = _to_entry(
                raw_sample=sample,
                operator=operator,
                data=payload,
                sample_type="ft_icr_ms",
                fraction=None,
                instrument="FT-ICR-MS",
                data_type=mode,
                method=mode,
                notes=Path(csv_path).name,
            )
            entries.append(entry)

    if not entries:
        return []

    return [{"samplenumber": sample, FT_TABLE: {"entries": entries}}]


def _discover_isotope_files(root_dir: str, kind: str | None = None) -> list[str]:
    root = Path(root_dir)
    if not root.exists():
        raise FileNotFoundError(root_dir)

    kind_norm = _norm_token(kind) if kind else None

    out: list[str] = []
    for p in root.rglob("*combined*.csv"):
        iso_kind = _extract_isotope_kind(str(p))
        if not iso_kind:
            continue
        if kind_norm and iso_kind != kind_norm:
            continue
        out.append(str(p))
    return sorted(set(out))


def _build_isotope_rows(csv_path: str) -> tuple[str | None, list[dict]]:
    iso_kind = _extract_isotope_kind(csv_path)
    if not iso_kind:
        return None, []

    table = ISOTOPE_TABLE_BY_KIND_NORM[iso_kind]
    operator = _extract_isotope_operator(csv_path)
    fraction = _extract_fraction_from_filename(csv_path)

    grouped: dict[str, dict] = {}
    order: list[str] = []

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header: list[str] | None = None

        for raw in reader:
            if not raw:
                continue
            cleaned_row = [str(c or "").strip() for c in raw]
            if not any(cleaned_row):
                continue

            first_nonempty = next((x for x in cleaned_row if x), "")
            if first_nonempty.lower() == "samplenumber":
                header = cleaned_row
                continue

            if header is None:
                continue

            mapped: dict[str, object] = {}
            for idx, col in enumerate(header):
                if not col:
                    continue
                value = cleaned_row[idx] if idx < len(cleaned_row) else ""
                mapped[col] = value

            sample_raw = None
            for key in list(mapped.keys()):
                if key.lower() == "samplenumber":
                    sample_raw = str(mapped.pop(key)).strip()
                    break

            if not sample_raw:
                continue

            sample = normalize_sample_number(sample_raw)
            if not sample:
                continue

            payload: dict[str, object] = {}
            for k, v in mapped.items():
                cleaned = _clean_cell(v)
                if cleaned is None:
                    continue
                payload[str(k).strip()] = cleaned

            if not payload:
                continue

            entry = _to_entry(
                raw_sample=sample_raw,
                operator=operator,
                data=payload,
                sample_type="isotope",
                fraction=fraction,
                instrument=iso_kind.upper(),
                data_type="Isotope",
                method="Isotope",
                notes=Path(csv_path).name,
            )

            if sample not in grouped:
                grouped[sample] = {"samplenumber": sample, table: {"entries": [entry]}}
                order.append(sample)
            else:
                grouped[sample][table]["entries"].append(entry)

    rows = [grouped[s] for s in order]
    return table, rows


def _upsert_family_rows(conn, table: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    return upsert_rows(
        conn,
        table=f"public.{table}",
        rows=rows,
        conflict_cols=["samplenumber"],
        update_cols=[table],
        json_cols=[table],
        commit=True,
    )


def ingest_ft(*, root_dir: str, single_file: str | None = None, dry_run: bool = False) -> int:
    files = [single_file] if single_file else _discover_ft_files(root_dir)
    logger.info("FT files discovered: %d", len(files))
    for p in files[:20]:
        logger.info("  - %s", p)
    if len(files) > 20:
        logger.info("  ... and %d more", len(files) - 20)

    if dry_run:
        return 0

    with get_connection() as conn:
        _ensure_family_tables_and_views(conn, [FT_TABLE])
        total = 0
        for p in files:
            rows = _build_ft_rows(p)
            total += _upsert_family_rows(conn, FT_TABLE, rows)
        _ensure_presence_and_acl(conn)
    logger.info("FT ingest complete, upserted rows: %d", total)
    return total


def ingest_isotope(
    *,
    root_dir: str,
    single_file: str | None = None,
    kind: str | None = None,
    dry_run: bool = False,
) -> int:
    if single_file:
        files = [single_file]
    else:
        files = _discover_isotope_files(root_dir, kind=kind)

    logger.info("Isotope files discovered: %d", len(files))
    for p in files[:20]:
        logger.info("  - %s", p)
    if len(files) > 20:
        logger.info("  ... and %d more", len(files) - 20)

    if dry_run:
        return 0

    with get_connection() as conn:
        _ensure_family_tables_and_views(conn, list(ISOTOPE_TABLE_BY_KIND.values()))
        total = 0
        for p in files:
            table, rows = _build_isotope_rows(p)
            if not table:
                continue
            total += _upsert_family_rows(conn, table, rows)
        _ensure_presence_and_acl(conn)
    logger.info("Isotope ingest complete, upserted rows: %d", total)
    return total


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest FT-ICR-MS and Isotope datasets")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ft = sub.add_parser("ft", help="Ingest FT-ICR-MS Signallist CSVs")
    p_ft.add_argument("--root-dir", default="/Users/anya/Coding/Database/FT-DataForDatabase")
    p_ft.add_argument("--file", default=None)
    p_ft.add_argument("--dry-run", action="store_true")

    p_iso = sub.add_parser("isotope", help="Ingest Isotope combined CSVs")
    p_iso.add_argument("--root-dir", default="/Users/anya/Coding/Database/FT-DataForDatabase")
    p_iso.add_argument("--file", default=None)
    p_iso.add_argument("--kind", choices=["co2_werte", "hd_werte"], default=None)
    p_iso.add_argument("--dry-run", action="store_true")

    p_all = sub.add_parser("all", help="Ingest FT-ICR-MS and Isotope datasets")
    p_all.add_argument("--root-dir", default="/Users/anya/Coding/Database/FT-DataForDatabase")
    p_all.add_argument("--dry-run", action="store_true")

    return parser


def main() -> int:
    _configure_logging(os.getenv("LOG_LEVEL", "INFO"))
    load_env(os.getenv("ENV_FILE", str(DEFAULT_ENV)))

    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "ft":
        ingest_ft(root_dir=args.root_dir, single_file=args.file, dry_run=args.dry_run)
        return 0

    if args.command == "isotope":
        ingest_isotope(
            root_dir=args.root_dir,
            single_file=args.file,
            kind=args.kind,
            dry_run=args.dry_run,
        )
        return 0

    if args.command == "all":
        ingest_ft(root_dir=args.root_dir, dry_run=args.dry_run)
        ingest_isotope(root_dir=args.root_dir, dry_run=args.dry_run)
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
