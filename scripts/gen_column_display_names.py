#!/usr/bin/env python3
"""Generate column_display_names.json from column_names CSVs.

For each GC compound table, maps DB column keys to GFZ abbreviation
display names by positionally aligning the "DB key" row with the
"GFZ abbreviation / final abbrev" row in the corresponding CSV.

Usage:
    python scripts/gen_column_display_names.py
"""
import csv
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
COL_NAMES_DIR = BASE / "app" / "data" / "column_names"
COL_ORDER_PATH = BASE / "app" / "data" / "column_order.json"
OUTPUT_PATH = BASE / "app" / "data" / "column_display_names.json"

# Table → CSV configuration
# db_key_row:  row index whose cells match DB column keys
# display_row: row index whose cells contain GFZ abbreviation display names
# label_col:   True if col 0 is a label/description column (data starts at col 1)
CONFIGS: dict[str, dict] = {
    # --- Well-structured CSVs with "final abbrev" rows ---
    "alkanes": {
        "csv": "n_alkanes_different names_new.csv",
        "db_key_row": 0,
        "display_row": 7,  # "final abbrev"
        "label_col": True,
    },
    "whole_oil": {
        "csv": "whole oil names.csv",
        "db_key_row": 0,
        "display_row": 7,  # "final abbrev"
        "label_col": True,
    },
    "alcohols": {
        "csv": "alcohols names.csv",
        "db_key_row": 0,
        "display_row": 4,  # "final abbrev"
        "label_col": True,
    },
    "ebfas": {
        "csv": "EBFAs names.csv",
        "db_key_row": 0,
        "display_row": 4,  # "final abbrev"
        "label_col": True,
    },
    "fames": {
        "csv": "FAMEs names.csv",
        "db_key_row": 0,
        "display_row": 4,  # "final abbrev"
        "label_col": True,
    },
    "archaeolipids": {
        "csv": "archaelipids names.csv",
        "db_key_row": 1,
        "display_row": 4,  # "final abbrev"
        "label_col": True,
    },
    "norcholestanes": {
        "csv": "norcholestanes names.csv",
        "db_key_row": 0,
        "display_row": 4,  # "final abbrev"
        "label_col": True,
    },
    # --- Steranes: no label column, all cols are data ---
    "steranes": {
        "csv": "steranes names.csv",
        "db_key_row": 4,
        "display_row": 9,  # "Ster-" prefixed abbreviations
        "label_col": False,
    },
    # --- GCMS CSVs with explicit "GFZ abbreviation" rows ---
    "phenanthrenes": {
        "csv": "GCMS_aromatic-fraction_C0-3_phenanthrenes-names.csv",
        "db_key_row": 2,
        "display_row": 10,  # "GFZ abbreviation"
        "label_col": True,
    },
    "naphthalenes": {
        "csv": "GCMS_aromatic-fraction_C0-5_naphthalenes-names.csv",
        "db_key_row": 0,
        "display_row": 13,  # "GFZ abbreviation"
        "label_col": True,
    },
    # thiophenes: thiophenes names.csv row 0 = DB keys, no abbreviation row
    # fluorenes: DB keys (3MF, 2MF, etc.) are already the correct names
    "carbazoles": {
        "csv": "GCMS_resin(NSO)-fraction_C0-3_alkylcarbazoles_benzocarbazoles_names.csv",
        "db_key_row": 0,
        "display_row": 5,  # "Sec32 abbreviation"
        "label_col": True,
    },
    "alkylbenzenes": {
        "csv": "alkylbenzenes names.csv",
        "db_key_row": 1,
        "display_row": 13,  # "Sec 3.2 abbreviation"
        "label_col": True,
    },
}


def build_mapping(table: str, cfg: dict, db_keys: set[str]) -> dict[str, str]:
    """Build {db_key: display_name} for one table."""
    csv_path = COL_NAMES_DIR / cfg["csv"]
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.reader(f))

    db_row = rows[cfg["db_key_row"]]
    disp_row = rows[cfg["display_row"]]
    start = 1 if cfg["label_col"] else 0

    # lowercase → original DB key for case-insensitive fallback
    lower_map: dict[str, str] = {}
    for k in db_keys:
        lk = k.strip().lower()
        if lk not in lower_map:
            lower_map[lk] = k

    mapping: dict[str, str] = {}
    ncols = max(len(db_row), len(disp_row))
    for i in range(start, ncols):
        raw = db_row[i].strip() if i < len(db_row) else ""
        display = disp_row[i].strip() if i < len(disp_row) else ""
        if not raw:
            continue

        # Resolve to actual DB key (exact or case-insensitive)
        if raw in db_keys:
            orig = raw
        elif raw.lower() in lower_map:
            orig = lower_map[raw.lower()]
        else:
            continue

        if display and display != orig:
            mapping[orig] = display

    return mapping


def main():
    col_order = json.loads(COL_ORDER_PATH.read_text(encoding="utf-8"))

    result: dict[str, dict[str, str]] = {}
    for table, cfg in sorted(CONFIGS.items()):
        db_keys = set(col_order.get(table, []))
        if not db_keys:
            print(f"  SKIP {table}: no keys in column_order.json")
            continue
        mapping = build_mapping(table, cfg, db_keys)
        if mapping:
            result[table] = mapping
        total = len(db_keys)
        mapped = len(mapping)
        print(f"  {table}: {mapped}/{total} keys mapped")

    OUTPUT_PATH.write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"\nWrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
