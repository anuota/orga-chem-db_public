"""
db_utils.py — data access & ingestion utilities
- Dependency-inverted services (ConnectionProvider, Upserter)
- CSV ingestion using a Strategy (parsing policy is pluggable)
- JSON entries merging both app-side and DB-side
"""

import csv
import json
import logging
import os
from typing import (Any, Callable, Dict, Iterable, List, Protocol, Sequence,
                    runtime_checkable)

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

from db_code.parsing.csv_multiheader import \
    rows_from_multiheader_csv_grouped  # <-- re-export for tests
from db_code.parsing.csv_multiheader import (CsvParseStrategy,
                                             MultiHeaderEntriesStrategy)
from db_code.parsing.normalize import \
    check_fraction_consistency  # <-- re-export for tests
from db_code.parsing.normalize import (_clean_cell, normalize_analysis,
                                       normalize_sample_number)

logger = logging.getLogger(__name__)


def get_connection():
    """Get a new psycopg2 connection using environment variables directly."""
    return psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "postgres"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD"),
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
    )


# Load .env file (defaults to ".env" but can be overridden)
def load_env(env_file: str = ".env", *, override: bool | None = None):
    """
    Load environment variables from the given .env file.

    By default, DO NOT override variables that are already set in the process
    (e.g., those passed by Docker Compose). You can force overriding by:
      - passing override=True, or
      - setting environment variable DOTENV_OVERRIDE=true/1/yes.
    """
    # Decide override policy: env var wins if not explicitly provided
    if override is None:
        ov_env = os.getenv("DOTENV_OVERRIDE", "").strip().lower()
        override = ov_env in {"1", "true", "yes", "y"}
    load_dotenv(dotenv_path=env_file, override=override)


# --- in db_utils.py ---


# in db_utils.py
def ensure_table(conn, ddl: str) -> None:
    """Execute a CREATE TABLE IF NOT EXISTS DDL."""
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def _dedupe_rows(
    rows: list[dict], conflict_cols: list[str], json_cols: list[str] | None
) -> list[dict]:
    """
    Deduplicate incoming rows by the conflict key tuple. For duplicates:
    - For JSON family columns shaped like {"entries": [ ... ]}, we union-distinct entries
      using a fast canonical JSON text key.
    - For other JSON columns (plain dicts), later values override earlier keys.
    - For non-JSON columns, keep the latest non-None value.
    Keeps the first-seen order of unique keys.
    """
    json_set = set(json_cols or [])
    seen: dict[tuple, dict] = {}
    order: list[tuple] = []

    def _is_blank(v: object) -> bool:
        return v is None or (isinstance(v, str) and v.strip() == "")

    def _entry_sig(entry: dict) -> tuple:
        """
        Signature for semantic duplicate detection.
        We intentionally ignore `instrument` and `notes` so entries that differ only
        by those metadata fields collapse into one logical measurement row.
        """
        data = entry.get("data") if isinstance(entry.get("data"), dict) else entry.get("data")
        try:
            data_key = json.dumps(data, sort_keys=True, ensure_ascii=False)
        except Exception:
            data_key = str(data)
        return (
            str(entry.get("raw_sample") or ""),
            str(entry.get("type") or ""),
            str(entry.get("date") or ""),
            str(entry.get("fraction") or ""),
            str(entry.get("name") or ""),
            str(entry.get("measured_by") or ""),
            str(entry.get("data_type") or ""),
            data_key,
        )

    def _merge_entries(existing_entry: dict, incoming_entry: dict) -> dict:
        merged = dict(existing_entry)
        # Prefer richer metadata from either side; only fill blanks.
        for fld in (
            "raw_sample",
            "type",
            "date",
            "fraction",
            "name",
            "measured_by",
            "data_type",
            "instrument",
            "notes",
        ):
            if _is_blank(merged.get(fld)) and not _is_blank(incoming_entry.get(fld)):
                merged[fld] = incoming_entry.get(fld)
        if _is_blank(merged.get("data")) and not _is_blank(incoming_entry.get("data")):
            merged["data"] = incoming_entry.get("data")
        return merged

    def _merge_family_json(old_v, new_v):
        # Both are dicts? Try to union their entries arrays if present
        if isinstance(old_v, dict) and isinstance(new_v, dict):
            old_entries = (
                old_v.get("entries") if isinstance(old_v.get("entries"), list) else []
            )
            new_entries = (
                new_v.get("entries") if isinstance(new_v.get("entries"), list) else []
            )
            # Semantic dedupe for dict entries + fallback distinct for non-dict entries.
            by_sig: dict[tuple, dict] = {}
            sig_order: list[tuple] = []
            other_keys = set()
            other_entries = []
            for e in old_entries + new_entries:
                if isinstance(e, dict):
                    sig = _entry_sig(e)
                    if sig in by_sig:
                        by_sig[sig] = _merge_entries(by_sig[sig], e)
                    else:
                        by_sig[sig] = dict(e)
                        sig_order.append(sig)
                else:
                    try:
                        k = json.dumps(e, sort_keys=True, ensure_ascii=False)
                    except Exception:
                        k = str(e)
                    if k not in other_keys:
                        other_keys.add(k)
                        other_entries.append(e)

            merged = [by_sig[s] for s in sig_order] + other_entries
            return {"entries": merged}
        # Fallbacks
        return new_v if new_v is not None else old_v

    for r in rows:
        key = tuple(r.get(c) for c in conflict_cols)
        if key in seen:
            existing = seen[key]
            for k, v in r.items():
                if k in conflict_cols:
                    continue
                if k in json_set:
                    old_v = existing.get(k)
                    # Family JSON with 'entries' array -> union
                    if (
                        isinstance(old_v, dict)
                        and isinstance(v, dict)
                        and (
                            isinstance(old_v.get("entries"), list)
                            or isinstance(v.get("entries"), list)
                        )
                    ):
                        existing[k] = _merge_family_json(old_v, v)
                    # Plain dicts: shallow merge (later overrides)
                    elif isinstance(old_v, dict) and isinstance(v, dict):
                        merged = {**old_v, **v}
                        existing[k] = merged
                    elif v is not None:
                        existing[k] = v
                else:
                    if v is not None:
                        existing[k] = v
        else:
            seen[key] = dict(r)
            order.append(key)

    return [seen[k] for k in order]


def upsert_rows(
    conn,
    table: str,
    rows: list[dict],
    conflict_cols: list[str],
    update_cols: list[str] = None,
    json_cols: list[str] = None,
    commit: bool = True,
) -> int:

    if not rows:
        return 0

    rows = _dedupe_rows(rows, conflict_cols, list(json_cols or []))
    all_cols = list(rows[0].keys())

    if update_cols is None:
        update_cols = [c for c in all_cols if c not in conflict_cols]
    else:
        update_cols = [c for c in update_cols if c != "notes"]

    json_cols_set = set(json_cols or [])

    # Support schema-qualified table names like "public.hopanes"
    if "." in table:
        schema_name, table_name = table.split(".", 1)
        table_ident = sql.SQL(".").join(
            [sql.Identifier(schema_name), sql.Identifier(table_name)]
        )
    else:
        table_ident = sql.Identifier(table)

    cols_ident = [sql.Identifier(c) for c in all_cols]
    conflict_ident = [sql.Identifier(c) for c in conflict_cols]

    # Build per-column SET expressions; special handling for json family columns
    set_items = []
    for c in update_cols:
        if c in json_cols_set:
            # references: existing {table}.{col} and EXCLUDED.{col}
            existing_ref = sql.Composed([table_ident, sql.SQL("."), sql.Identifier(c)])
            excluded_ref = sql.Composed([sql.SQL("EXCLUDED."), sql.Identifier(c)])
            # {col} = jsonb_build_object('entries', DISTINCT( old.entries U new.entries ))
            merge_expr = sql.SQL(
                "{col} = jsonb_build_object('entries', ("
                "  SELECT COALESCE(jsonb_agg(d.elem), '[]'::jsonb)"
                "  FROM ("
                "    SELECT DISTINCT ON ("
                "      COALESCE(elem->>'raw_sample',''),"
                "      COALESCE(elem->>'type',''),"
                "      COALESCE(elem->>'date',''),"
                "      COALESCE(elem->>'fraction',''),"
                "      COALESCE(elem->>'name',''),"
                "      COALESCE(elem->>'measured_by',''),"
                "      COALESCE(elem->>'data_type',''),"
                "      COALESCE(elem->'data','{{}}'::jsonb)"
                "    ) elem"
                "    FROM ("
                "      SELECT jsonb_array_elements(COALESCE({old}->'entries','[]'::jsonb)) AS elem"
                "      UNION ALL"
                "      SELECT jsonb_array_elements(COALESCE({new}->'entries','[]'::jsonb)) AS elem"
                "    ) s"
                "    ORDER BY"
                "      COALESCE(elem->>'raw_sample',''),"
                "      COALESCE(elem->>'type',''),"
                "      COALESCE(elem->>'date',''),"
                "      COALESCE(elem->>'fraction',''),"
                "      COALESCE(elem->>'name',''),"
                "      COALESCE(elem->>'measured_by',''),"
                "      COALESCE(elem->>'data_type',''),"
                "      COALESCE(elem->'data','{{}}'::jsonb),"
                "      (CASE WHEN COALESCE(elem->>'instrument','') <> '' THEN 8 ELSE 0 END"
                "       + CASE WHEN COALESCE(elem->>'name','') <> '' THEN 4 ELSE 0 END"
                "       + CASE WHEN COALESCE(elem->>'measured_by','') <> '' THEN 2 ELSE 0 END"
                "       + CASE WHEN COALESCE(elem->>'notes','') <> '' THEN 1 ELSE 0 END) DESC"
                "  ) d"
                "))"
            ).format(col=sql.Identifier(c), old=existing_ref, new=excluded_ref)
            set_items.append(merge_expr)
        else:
            set_items.append(
                sql.Composed(
                    [sql.Identifier(c), sql.SQL(" = EXCLUDED."), sql.Identifier(c)]
                )
            )

    set_clause = sql.SQL(", ").join(set_items)

    insert_sql = sql.SQL(
        """
        INSERT INTO {table} ({cols})
        VALUES %s
        ON CONFLICT ({conflict_cols})
        DO UPDATE SET {set_clause}
    """
    ).format(
        table=table_ident,
        cols=sql.SQL(", ").join(cols_ident),
        conflict_cols=sql.SQL(", ").join(conflict_ident),
        set_clause=set_clause,
    )

    json_cols_set = set(json_cols or [])
    values = []
    for r in rows:
        vals = []
        for c in all_cols:
            v = r[c]
            if c in json_cols_set and v is not None:
                v = Json(v)
            vals.append(v)
        values.append(tuple(vals))

    # NOTE: no "with conn:" here
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, values)

    if commit:
        conn.commit()

    return len(values)


def df_to_rows(
    df: pd.DataFrame,
    column_map: Dict[str, str],
    constants: Dict[str, Any] | None = None,
    defaults: Dict[str, Any] | None = None,
    transforms: Dict[str, Callable[[Any], Any]] | None = None,
    include_only: Iterable[str] | None = None,
) -> List[dict]:
    """
    Convert a DataFrame into a list of dict rows matching DB column names.

    Parameters
    ----------
    df : DataFrame
        Source data.
    column_map : {df_col -> db_col}
        Maps DataFrame column names to DB column names.
    constants : {db_col -> value}
        Constant values to add to every row (e.g., measured_by="...").
    defaults : {db_col -> default_value}
        Default values used when a mapped value is missing/NaN.
    transforms : {db_col -> function(value) -> value}
        Per-target-column transformers applied after mapping and defaulting.
    include_only : iterable[str] | None
        If provided, only keep these DB columns (useful to drop extras cleanly).
    """
    constants = constants or {}
    defaults = defaults or {}
    transforms = transforms or {}

    # Reindex safely: only use columns that exist in df
    present_map = {
        df_col: db_col for df_col, db_col in column_map.items() if df_col in df.columns
    }

    rows = []
    # Work row-wise without copying the entire DF unnecessarily
    for _, s in df.iterrows():
        out = {}

        # 1) mapped columns
        for df_col, db_col in present_map.items():
            val = s[df_col]

            # Pandas NaN/NA handling
            if pd.isna(val):
                val = defaults.get(db_col, None)

            # Transform if requested
            if db_col in transforms and val is not None:
                val = transforms[db_col](val)

            out[db_col] = val

        # 2) constants
        for db_col, value in constants.items():
            v = value
            if db_col in transforms and v is not None:
                v = transforms[db_col](v)
            out[db_col] = v

        # 3) apply defaults for any missing keys from the map/defaults
        for db_col, default_val in defaults.items():
            if db_col not in out or out[db_col] is None:
                v = default_val
                if db_col in transforms and v is not None:
                    v = transforms[db_col](v)
                out[db_col] = v

        # 4) optionally filter to include_only
        if include_only is not None:
            out = {k: v for k, v in out.items() if k in include_only}

        rows.append(out)

    return rows
