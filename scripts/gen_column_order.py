#!/usr/bin/env python3
"""Generate app/data/column_order.json from the original CSV headers.

Run from the project root:
    PYTHONPATH=. python scripts/gen_column_order.py

Reads CSVs from ORG_CHEM_DATA_DIR (or ORG_CHEM_FT_ROOT/GC-DataForDatabase)
and writes a JSON mapping table_name -> [col1, col2, ...] in the
original CSV header order.
"""
import csv
import json
import os
import sys
from glob import glob
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db_code.parsing.csv_multiheader import DATA_META_KEYS, OPERATOR_KEYS
from db_code.parsing.normalize import derive_table_from_filename
from db_code.parsing.header_normalize import normalize_data_payload

_SKIP_KEYS = DATA_META_KEYS | OPERATOR_KEYS

_FT_ROOT = os.environ.get(
    "ORG_CHEM_FT_ROOT",
    str(Path(__file__).resolve().parent.parent.parent / "FT-DataForDatabase"),
)
DATA_DIR = os.environ.get(
    "ORG_CHEM_DATA_DIR",
    os.path.join(_FT_ROOT, "GC-DataForDatabase"),
)

OUT_PATH = Path(__file__).resolve().parent.parent / "app" / "data" / "column_order.json"


def _ordered_data_keys(csv_path: str) -> list[str]:
    """Read the first header row and return data column names in order."""
    keys: list[str] = []
    seen: set[str] = set()
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for raw in reader:
            if not raw or all((c or "").strip() == "" for c in raw):
                continue
            cleaned = [str(c or "").strip() for c in raw]
            first = next((c for c in cleaned if c != ""), "")
            if first.lower() == "samplenumber" or not keys:
                # This is a header row
                for col in cleaned:
                    if col and col.lower() not in _SKIP_KEYS and col not in seen:
                        seen.add(col)
                        keys.append(col)
                break
    return keys


def _normalize_column_list(table: str, cols: list[str]) -> list[str]:
    """Apply the same header normalisation used during data ingest."""
    fake = {c: True for c in cols}
    try:
        normed = normalize_data_payload(table, fake)
    except Exception:
        normed = fake
    return [k for k in normed]


def main() -> None:
    patterns = [
        os.path.join(DATA_DIR, "*combined (Area).csv"),
        os.path.join(DATA_DIR, "*combined.csv"),
        os.path.join(DATA_DIR, "* (Area).csv"),
        os.path.join(DATA_DIR, "* (Concentration).csv"),
        os.path.join(DATA_DIR, "* (concentration).csv"),
    ]
    files: list[str] = []
    for p in patterns:
        files.extend(glob(p))

    result: dict[str, list[str]] = {}
    for f in sorted(set(files)):
        table = derive_table_from_filename(f)
        if not table:
            continue
        raw_cols = _ordered_data_keys(f)
        cols = _normalize_column_list(table, raw_cols)
        if table not in result:
            result[table] = cols
        else:
            existing = set(result[table])
            for k in cols:
                if k not in existing:
                    result[table].append(k)
                    existing.add(k)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {OUT_PATH}")
    for t in sorted(result):
        print(f"  {t}: {len(result[t])} columns")


if __name__ == "__main__":
    main()
