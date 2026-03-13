"""Upload service – parse user-uploaded CSV data and ingest into the database.

Supports three data families:
  • GC   – compound tables (hopanes, steranes, …)
  • FT   – FT-ICR-MS signal-list CSVs
  • Isotope – CO₂ Werte / HD Werte combined tables

Each handler:
  1. Validates and parses the CSV payload.
  2. Builds rows in the standard JSONB-entry format.
  3. Upserts into the database via the shared ``upsert_rows`` function.

File-system writes are intentionally **not** performed here; the caller
(API route) decides whether / how to persist the raw CSV on disk.
"""
from __future__ import annotations

import csv
import io
import logging
import re
from pathlib import Path

from db_code.db_utils import upsert_rows
from db_code.infra.db_conn import PsycopgEnvConnectionProvider
from db_code.parsing.header_normalize import normalize_data_payload
from db_code.parsing.normalize import (
    _clean_cell,
    _parse_date_like,
    check_fraction_consistency,
    extract_base_fraction_notes,
    normalize_analysis,
    normalize_sample_number,
    normalize_type_label,
)
from db_code.services.special_ingest import (
    FT_MODES,
    FT_TABLE,
    ISOTOPE_TABLE_BY_KIND,
    _to_entry,
)

logger = logging.getLogger(__name__)

_conn_provider = PsycopgEnvConnectionProvider()

# Re-use metadata column detection sets from csv_multiheader
_TYPE_KEYS = {"type", "typ", "probeart", "material", "matrix", "gesteinsart", "probenart"}
_NAME_KEYS = {"name"}
_OPERATOR_KEYS = {"operator", "measured_by", "measuredby"}
_DATE_KEYS = {"date", "measured_at", "datum"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_csv_text(text: str) -> tuple[list[str], list[list[str]]]:
    """Return (header, data_rows) from CSV text.  Strips BOM if present."""
    text = text.lstrip("\ufeff")
    reader = csv.reader(io.StringIO(text))
    header: list[str] | None = None
    data: list[list[str]] = []
    for raw in reader:
        if not raw or all((c or "").strip() == "" for c in raw):
            continue
        cleaned = [str(c or "").strip() for c in raw]
        first = next((c for c in cleaned if c), "")
        if first.lower() == "samplenumber":
            header = cleaned
            continue
        if header is None:
            header = cleaned
            continue
        data.append(cleaned)
    if header is None:
        raise ValueError("CSV contains no header row (expected SampleNumber as first column).")
    return header, data


def expected_columns(table: str) -> list[str]:
    """Return the data column names currently stored in the DB for *table*.

    Queries the ``{table}_entries`` view and collects all keys from the
    ``data`` JSONB field.  Returns an alphabetically-sorted list.
    """
    with _conn_provider.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT data FROM public.{table}_entries"  # noqa: S608 – table is validated upstream
            )
            keys: set[str] = set()
            for (data_val,) in cur:
                if isinstance(data_val, dict):
                    keys.update(data_val.keys())
        conn.rollback()
    return sorted(keys)


# ---------------------------------------------------------------------------
# GC upload
# ---------------------------------------------------------------------------

def parse_gc_upload(
    csv_text: str,
    table: str,
    *,
    instrument: str | None = None,
    data_type: str = "Area",
) -> list[dict]:
    """Parse user-uploaded GC CSV and return rows ready for ``upsert_rows``.

    The returned list has the shape expected by the shared upserter::

        [{"samplenumber": "G000392", "<table>": {"entries": [entry, ...]}}]
    """
    header, data_rows = _parse_csv_text(csv_text)
    analysis_key = normalize_analysis(table)

    temp_rows: list[dict] = []
    for row in data_rows:
        mapped: dict[str, object] = {}
        for i, col in enumerate(header):
            if not col:
                continue
            val = _clean_cell(row[i]) if i < len(row) else None
            if val is not None:
                mapped[col] = val

        # Extract metadata columns
        sample_raw = None
        sample_name = None
        measured_by = None
        type_val = None
        date_val = None

        for k in list(mapped.keys()):
            lk = k.lower()
            if lk == "samplenumber":
                sample_raw = mapped.pop(k)
            elif lk in _NAME_KEYS:
                sample_name = mapped.pop(k)
            elif lk in _OPERATOR_KEYS:
                measured_by = mapped.pop(k)
            elif lk in _TYPE_KEYS:
                type_val = normalize_type_label(mapped.pop(k))
            elif lk in _DATE_KEYS:
                date_val = mapped.pop(k)

        if sample_raw is None:
            continue

        _match, _explicit, _inferred, _code = check_fraction_consistency(
            sample_raw, analysis_key,
        )
        base_id, fraction_label, notes = extract_base_fraction_notes(
            sample_raw, analysis_key,
        )
        if not base_id:
            continue

        if _explicit is not None:
            fraction_label = _explicit
        elif fraction_label is None and _inferred is not None:
            fraction_label = _inferred

        data_payload = normalize_data_payload(analysis_key, dict(mapped))

        entry = {
            "raw_sample": sample_raw,
            "name": sample_name,
            "measured_by": measured_by or "unknown",
            "type": type_val,
            "date": _parse_date_like(date_val),
            "fraction": fraction_label,
            "instrument": instrument,
            "data_type": data_type,
            "notes": notes,
            "data": data_payload,
        }
        temp_rows.append({"samplenumber": base_id, table: entry})

    # Group by samplenumber
    grouped: dict[str, dict] = {}
    order: list[str] = []
    for r in temp_rows:
        sn = r["samplenumber"]
        entry = r[table]
        if sn not in grouped:
            grouped[sn] = {"samplenumber": sn, table: {"entries": [entry]}}
            order.append(sn)
        else:
            grouped[sn][table]["entries"].append(entry)

    return [grouped[sn] for sn in order]


def ingest_gc_upload(
    csv_text: str,
    table: str,
    *,
    instrument: str | None = None,
    data_type: str = "Area",
) -> int:
    """Parse + upsert GC data.  Returns number of upserted rows."""
    rows = parse_gc_upload(csv_text, table, instrument=instrument, data_type=data_type)
    if not rows:
        return 0
    with _conn_provider.get_connection() as conn:
        count = upsert_rows(
            conn,
            table=f"public.{table}",
            rows=rows,
            conflict_cols=["samplenumber"],
            update_cols=[table],
            json_cols=[table],
            commit=True,
        )
    return count


# ---------------------------------------------------------------------------
# FT-ICR-MS upload
# ---------------------------------------------------------------------------

def parse_ft_upload(
    csv_text: str,
    mode: str,
    operator: str,
    filename: str,
) -> list[dict]:
    """Parse a user-uploaded FT signal-list CSV and return DB rows.

    *mode* is the acquisition mode key (``appipos``, ``esineg``, ``esipos``).
    """
    mode_label = FT_MODES.get(mode)
    if not mode_label:
        raise ValueError(f"Unknown FT mode '{mode}'. Expected one of: {list(FT_MODES)}")

    sample = normalize_sample_number(filename)
    if not sample:
        raise ValueError(
            f"Cannot extract sample number (GXXXXXX) from filename '{filename}'."
        )

    # Read the CSV and compute summary stats (same logic as _build_ft_rows)
    reader = csv.DictReader(io.StringIO(csv_text.lstrip("\ufeff")))
    peak_count = 0
    sn_values: list[float] = []
    mass_values: list[float] = []

    for row in reader:
        has_data = False
        for k, v in row.items():
            if k is None:
                continue
            key = str(k).strip()
            if not key:
                continue
            cleaned = _clean_cell(v)
            if cleaned is None:
                continue
            has_data = True
            if key == "signalNoise_ratio" and isinstance(cleaned, (int, float)):
                sn_values.append(float(cleaned))
            elif key == "observedExactMass_ion" and isinstance(cleaned, (int, float)):
                mass_values.append(float(cleaned))
        if has_data:
            peak_count += 1

    if peak_count == 0:
        raise ValueError("CSV contains no data rows.")

    summary: dict[str, object] = {
        "peak_count": peak_count,
        "source_file": filename,
    }
    if sn_values:
        summary["min_signal_to_noise"] = round(min(sn_values), 2)
        summary["max_signal_to_noise"] = round(max(sn_values), 2)
    if mass_values:
        summary["min_mass"] = round(min(mass_values), 4)
        summary["max_mass"] = round(max(mass_values), 4)

    entry = _to_entry(
        raw_sample=sample,
        operator=operator,
        data=summary,
        sample_type="ft_icr_ms",
        fraction=None,
        instrument="FT-ICR-MS",
        data_type=mode_label,
        method=mode_label,
        notes=filename,
    )
    return [{"samplenumber": sample, FT_TABLE: {"entries": [entry]}}]


def ingest_ft_upload(
    csv_text: str,
    mode: str,
    operator: str,
    filename: str,
) -> int:
    """Parse + upsert FT data.  Returns number of upserted rows."""
    rows = parse_ft_upload(csv_text, mode, operator, filename)
    if not rows:
        return 0
    with _conn_provider.get_connection() as conn:
        count = upsert_rows(
            conn,
            table=f"public.{FT_TABLE}",
            rows=rows,
            conflict_cols=["samplenumber"],
            update_cols=[FT_TABLE],
            json_cols=[FT_TABLE],
            commit=True,
        )
    return count


# ---------------------------------------------------------------------------
# Isotope upload
# ---------------------------------------------------------------------------

def parse_isotope_upload(
    csv_text: str,
    kind: str,
    operator: str,
) -> tuple[str, list[dict]]:
    """Parse a user-uploaded isotope CSV.

    *kind* is ``co2_werte`` or ``hd_werte``.
    Returns ``(table_name, rows)``.
    """
    table = ISOTOPE_TABLE_BY_KIND.get(kind)
    if not table:
        raise ValueError(f"Unknown isotope kind '{kind}'. Expected one of: {list(ISOTOPE_TABLE_BY_KIND)}")

    header, data_rows = _parse_csv_text(csv_text)

    grouped: dict[str, dict] = {}
    order: list[str] = []

    for row in data_rows:
        mapped: dict[str, object] = {}
        for i, col in enumerate(header):
            if not col:
                continue
            val = row[i] if i < len(row) else ""
            mapped[col] = val

        sample_raw = None
        for k in list(mapped.keys()):
            if k.lower() == "samplenumber":
                sample_raw = str(mapped.pop(k)).strip()
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
            fraction=None,
            instrument=kind.upper(),
            data_type="Isotope",
            method="Isotope",
            notes="upload",
        )

        if sample not in grouped:
            grouped[sample] = {"samplenumber": sample, table: {"entries": [entry]}}
            order.append(sample)
        else:
            grouped[sample][table]["entries"].append(entry)

    return table, [grouped[s] for s in order]


def ingest_isotope_upload(
    csv_text: str,
    kind: str,
    operator: str,
) -> int:
    """Parse + upsert isotope data.  Returns number of upserted rows."""
    table, rows = parse_isotope_upload(csv_text, kind, operator)
    if not rows:
        return 0
    with _conn_provider.get_connection() as conn:
        count = upsert_rows(
            conn,
            table=f"public.{table}",
            rows=rows,
            conflict_cols=["samplenumber"],
            update_cols=[table],
            json_cols=[table],
            commit=True,
        )
    return count
