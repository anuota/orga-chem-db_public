import logging
import re
from typing import Protocol, runtime_checkable

from .normalize import (_clean_cell, _parse_date_like,
                        check_fraction_consistency,
                        extract_base_fraction_notes, normalize_analysis,
                        normalize_type_label)

from .header_normalize import normalize_data_payload


logger = logging.getLogger(__name__)


def _open_csv(path: str):
    """Open a CSV file, trying utf-8-sig first then falling back to cp1252."""
    try:
        f = open(path, "r", encoding="utf-8-sig", newline="")
        f.read(512)  # probe for decode errors
        f.seek(0)
        return f
    except UnicodeDecodeError:
        return open(path, "r", encoding="cp1252", newline="")

DATA_META_KEYS = {"samplenumber", "name", "type", "date", "measured_at"}
TYPE_KEYS = {
    "type",
    "typ",
    "Type",
    "probeart",
    "material",
    "matrix",
    "gesteinsart",
    "probenart",
}
NAME_KEYS = {"name"}
OPERATOR_KEYS = {"operator", "measured_by", "measuredby"}
DATE_KEYS = {"date", "measured_at", "datum"}

def _norm_synonym_token(s: str | None) -> str | None:
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    t = t.replace("–", "-").replace("—", "-")   # унификация тире
    t = " ".join(t.split())                      # схлопнуть множественные пробелы
    return t.lower()

def collect_data_keys_from_csv(csv_path: str) -> set[str]:
    """Scan a CSV (multi-header tolerant) and return the union of data column keys
    excluding metadata keys (SampleNumber/Name/Type/Date/Measured_At).
    """
    import csv
    import os

    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)
    keys: set[str] = set()
    header: list[str] | None = None
    with _open_csv(csv_path) as f:
        reader = csv.reader(f)
        for raw in reader:
            if not raw or all((c or "").strip() == "" for c in raw):
                continue
            cleaned = [str(c or "").strip() for c in raw]
            first_nonempty = next((c for c in cleaned if c != ""), "")
            if first_nonempty.lower() == "samplenumber":
                header = cleaned
                continue
            if header is None:
                header = cleaned
                continue
            mapped: dict[str, object] = {}
            for i, colname in enumerate(header):
                if not colname:
                    continue
                if i < len(cleaned):
                    v = cleaned[i]
                    if v is None or str(v).strip() == "":
                        continue
                    mapped[colname] = v
            for k in list(mapped.keys()):
                if k.lower() not in DATA_META_KEYS:
                    keys.add(k)
    return keys


@runtime_checkable
class CsvParseStrategy(Protocol):
    def build_rows(
        self, csv_path: str, json_col: str, analysis: str | None = None,
        *, instrument: str | None = None, data_type: str | None = None,
    ) -> list[dict]: ...

class _AlkaneNameResolver:
    """
    Подтягивает synonym -> canonical_name из ref-таблиц и кэширует.
    Активен только для анализа 'alkanes'.
    """
    _cache: dict[str, str] | None = None

    @classmethod
    def _load_map(cls) -> dict[str, str]:
            """
            Placeholder for synonym->canonical map loader.
            No direct DB access here; actual mapping is done later during ingest.
            """
            return {}

    @classmethod
    def resolve(cls, header: str, *, analysis: str | None) -> tuple[str, str]:
        """
        Вернёт (resolved_key, original_header).
        Для не alkanes возвращает (header, header).
        """
        if not header:
            return header, header
        if analysis != "alkanes":
            return header, header
        if cls._cache is None:
            cls._cache = cls._load_map()

        syn = _norm_synonym_token(header)
        if syn and syn in cls._cache:
            return cls._cache[syn], header

        # быстрые варианты без пробелов/дефисов — иногда так лежат в словаре
        if syn:
            alt = syn.replace(" ", "").replace("-", "")
            if alt in cls._cache:
                return cls._cache[alt], header

        return header, header
    
class MultiHeaderEntriesStrategy:
    """Parses CSVs that may contain repeated header rows.
    If a row starts with 'SampleNumber' (case-insensitive), it's considered a new header block.
    If no explicit header has been set yet, the *first non-empty row* is used as the header.
    Each data row becomes one JSON `entry` under its canonical base sample (GXXXXXX).
    """

    def build_rows(
        self, csv_path: str, json_col: str, analysis: str | None = None,
        *, instrument: str | None = None, data_type: str | None = None,
    ) -> list[dict]:
        return rows_from_multiheader_csv_grouped(
            csv_path, json_col, analysis,
            instrument=instrument, data_type=data_type,
        )


def _find_sample_from_mapped(d: dict[str, object]) -> tuple[str | None, str | None]:
    """Return (key, value) of the first mapping whose value looks like a G-number."""
    for k, v in d.items():
        s = str(v) if v is not None else ""
        if re.search(r"(G\d{6})", s, flags=re.IGNORECASE):
            return k, v
    return None, None


def rows_from_multiheader_csv_grouped(
    csv_path: str, json_col: str, analysis: str | None = None,
    *, instrument: str | None = None, data_type: str | None = None,
) -> list[dict]:
    """
    Produce one DB row per samplenumber.
    JSON: {json_col}: { "entries": [ {raw_sample, measured_by, type, date, fraction, notes, data}, ... ] }
    Robustness improvements:
      - If no header has been set yet, the first non-empty row becomes the header (helps files like EBFAs with 'Area,Name,Type,...').
      - If there's no 'SampleNumber' column, we try to find a column whose value contains a G-number and use that as the sample field.
      - We add `raw_sample` into each entry so DISTINCT JSON merges do not collapse different fractions (e.g., -1/-2/-3) into one.
      - Fraction precedence: explicit code > inferred-by-analysis > default.
    """
    import csv
    import os

    if not os.path.exists(csv_path):
        logger.error(f"File not found: {csv_path}")
        raise FileNotFoundError(f"File not found: {csv_path}")

    temp_rows: list[dict] = []
    with _open_csv(csv_path) as f:
        reader = csv.reader(f)
        header: list[str] | None = None

        for raw in reader:
            if not raw or all((c or "").strip() == "" for c in raw):
                continue

            # Clean the row values once
            cleaned_row = [str(c or "").strip() for c in raw]

            # If we see an explicit SampleNumber header, (re)start a header block
            first_nonempty = next((c for c in cleaned_row if c != ""), "")
            if first_nonempty.lower() == "samplenumber":
                header = cleaned_row
                continue

            # If no header yet, treat this row as the header (handles EBFAs etc.)
            if header is None:
                header = cleaned_row
                continue

            # Map row to dict using current header
            mapped: dict[str, object] = {}
            for i, colname in enumerate(header):
                val = cleaned_row[i] if i < len(cleaned_row) else None
                val = _clean_cell(val)
                if val is None:
                    continue
                if colname != "":
                    mapped[colname] = val

            # Extract identifiers (case-insensitive)
            sample_raw = None
            sample_name = None
            measured_by = None
            type_val = None
            date_val = None

            for k in list(mapped.keys()):
                lk = k.lower()
                if lk == "samplenumber":
                    sample_raw = mapped.pop(k)
                elif lk in NAME_KEYS:
                    sample_name = mapped.pop(k)
                elif lk in OPERATOR_KEYS:
                    measured_by = mapped.pop(k)
                elif lk in TYPE_KEYS:
                    type_val = normalize_type_label(mapped.pop(k))
                elif lk in DATE_KEYS:
                    date_val = mapped.pop(k)

            # Fallback: if we still didn't capture a type, try fuzzy-ish header match
            if type_val is None:
                for k in list(mapped.keys()):
                    lk = k.lower()
                    if (lk in TYPE_KEYS) or ("typ" in lk) or ("type" in lk):
                        type_val = normalize_type_label(mapped.pop(k))
                        break

            # Fallback: find a column whose value looks like a G-number (e.g., 'Area' in EBFAs)
            if sample_raw is None:
                k_guess, v_guess = _find_sample_from_mapped(mapped)
                if v_guess is not None:
                    sample_raw = mapped.pop(k_guess)

            # Derive base id + fraction/notes, and enforce explicit > inferred precedence
            analysis_key = normalize_analysis(analysis or json_col)
            _match, _explicit, _inferred, _code = check_fraction_consistency(
                sample_raw, analysis_key
            )
            base_id, fraction_label, notes = extract_base_fraction_notes(
                sample_raw, analysis_key
            )
            if not base_id:
                continue
            if _explicit is not None:
                fraction_label = _explicit
            elif fraction_label is None and _inferred is not None:
                fraction_label = _inferred

            entry = {
                "raw_sample": sample_raw,  # provenance, keeps entries distinct across fractions
                "name": sample_name,
                "measured_by": measured_by if measured_by is not None else "unknown",
                "type": type_val,
                "date": _parse_date_like(date_val),
                "fraction": fraction_label,
                "instrument": instrument,
                "data_type": data_type,
                "notes": notes,
                # Hybrid canonicalization for n-alkanes: map headers to canonical keys and keep the original
                "data": (lambda _m: (
                    normalize_data_payload(analysis_key, _m)
                ))(dict(mapped)),
             }
                        
            temp_rows.append(
                {
                    "samplenumber": base_id,
                    json_col: entry,
                }
            )

    # Group by samplenumber, aggregate entries
    grouped: dict[str, dict] = {}
    order: list[str] = []
    for r in temp_rows:
        sn = r["samplenumber"]
        entry = r[json_col]
        if sn not in grouped:
            grouped[sn] = {
                "samplenumber": sn,
                json_col: {"entries": [entry]},
            }
            order.append(sn)
        else:
            grouped[sn][json_col]["entries"].append(entry)

    logger.debug(f"grouped {len(order)} samples from {csv_path}")
    return [grouped[sn] for sn in order]
