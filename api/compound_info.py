"""Compound information loader.

Parses transposed CSVs where rows are metadata fields and columns are
individual compounds.  Builds a lookup keyed by abbreviation so compound
info pages can be served quickly.
"""
from __future__ import annotations

import csv
import os
from functools import lru_cache
from pathlib import Path

_DATA_DIR = Path(__file__).resolve().parent.parent / "app" / "data" / "compound_info"
_GRAPHICS_DIR = _DATA_DIR / "structures_graphics"

# Mapping from graphics subdirectory to a set of PNG basenames (no ext)
_GRAPHICS_SUBDIRS = [
    "n-alkanes pictures",
    "sterane pictures",
    "alkylbenzenes names",
]


def _parse_transposed_csv(path: Path) -> list[dict[str, str]]:
    """Parse a transposed CSV into a list of compound dicts.

    Each row in the CSV is a field (e.g. compound_name, abbrev1, CAS, …).
    Columns after the first are individual compounds.
    Returns one dict per compound with keys being the field names from
    column 0.
    """
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        return []

    n_compounds = len(rows[0]) - 1  # first cell is the field label
    compounds: list[dict[str, str]] = [{} for _ in range(n_compounds)]

    for row in rows:
        if not row or not row[0].strip():
            continue
        field = row[0].strip()
        # Skip comment-like rows
        if field.startswith("Here ") or field.startswith("INChI for") or "not" in field.lower() and "checked" in field.lower():
            continue
        for i, val in enumerate(row[1:], start=0):
            if i >= n_compounds:
                break
            compounds[i][field] = val.strip() if val else ""

    return compounds


def _normalize_abbrev(abbrev: str) -> str:
    return abbrev.strip().lower().replace(" ", "").replace("-", "")


@lru_cache(maxsize=1)
def _graphics_index() -> dict[str, str]:
    """Build a mapping from normalized PNG basename → relative web path."""
    idx: dict[str, str] = {}
    for subdir in _GRAPHICS_SUBDIRS:
        d = _GRAPHICS_DIR / subdir
        if not d.is_dir():
            continue
        for png in d.iterdir():
            if png.suffix.lower() == ".png":
                key = _normalize_abbrev(png.stem)
                # Store relative path from structures_graphics/
                idx[key] = f"{subdir}/{png.name}"
    return idx


@lru_cache(maxsize=1)
def load_all_compounds() -> list[dict]:
    """Load and merge compound info from all CSV files.

    Each compound gets:
      - compound_name, abbrev1, abbrev2, compound_class, compound_group,
        method1, method2, InChI, CAS, formula, peak, iontrace, …
      - structure_graphic_path (relative path under structures_graphics/ or "")
      - source_file (which CSV it came from)
    """
    csv_files = [
        ("GCFID_GCMS_alkanes_final-names_SP.csv", "alkanes"),
        ("GCFID_wholeOil-names_final_SP.csv", "whole_oil"),
        ("steranes names_final_SP.csv", "steranes"),
    ]

    gfx = _graphics_index()
    all_compounds: list[dict] = []
    seen_abbrevs: set[str] = set()

    for fname, source_tag in csv_files:
        path = _DATA_DIR / fname
        if not path.is_file():
            continue
        for c in _parse_transposed_csv(path):
            name = c.get("compound_name") or c.get("compund_name") or ""
            abbrev = c.get("abbrev1", "")
            if not abbrev and not name:
                continue

            norm = _normalize_abbrev(abbrev) if abbrev else _normalize_abbrev(name)
            if norm in seen_abbrevs:
                continue
            seen_abbrevs.add(norm)

            # Try to find a matching structure graphic
            graphic_path = ""
            for candidate in (abbrev, c.get("abbrev2", ""), name):
                if candidate:
                    nk = _normalize_abbrev(candidate)
                    if nk in gfx:
                        graphic_path = gfx[nk]
                        break

            compound_class = (
                c.get("compound_class")
                or c.get("compound_class1")
                or c.get("compound_group")
                or ""
            )

            entry = {
                "compound_name": name,
                "abbrev1": abbrev,
                "abbrev2": c.get("abbrev2", ""),
                "compound_class": compound_class,
                "compound_class2": c.get("compound_class2", ""),
                "method1": c.get("method1", ""),
                "method2": c.get("method2", ""),
                "method1_iontrace": c.get("method2-iontrace-mz") or c.get("method1_iontrace-mz", ""),
                "method2_iontrace": c.get("method2_iontrace-mz", ""),
                "inchi": c.get("InChI", ""),
                "cas": c.get("CAS", ""),
                "formula": c.get("formula", ""),
                "peak": c.get("peak") or c.get("Peak", ""),
                "structure_graphic": graphic_path,
                "source": source_tag,
            }
            all_compounds.append(entry)

    return all_compounds


@lru_cache(maxsize=1)
def compound_index() -> dict[str, dict]:
    """Return a dict keyed by normalized abbrev1 → compound entry."""
    idx: dict[str, dict] = {}
    for c in load_all_compounds():
        key = _normalize_abbrev(c["abbrev1"]) if c["abbrev1"] else _normalize_abbrev(c["compound_name"])
        idx[key] = c
    return idx


def graphics_abs_path(rel_path: str) -> Path | None:
    """Resolve a relative graphics path to an absolute filesystem path."""
    if not rel_path:
        return None
    p = _GRAPHICS_DIR / rel_path
    return p if p.is_file() else None
