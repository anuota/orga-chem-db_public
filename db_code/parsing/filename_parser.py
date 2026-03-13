"""
Parse GC combined CSV filenames to extract measurement metadata.

Filename examples:
    Alcohols_combined (Area).csv
    GCFID-WO_combined (Area).csv
    GCFID-aliphatic-alkanes_combined (Concentration).csv
    GCMRMMS-aliphatic_steranes_combined (Area).csv
    GCMS-NSOsilyl_fattyAcids_combined (Area).csv
    Norcholestanes_combined (Area).csv
    GCMS-aromatic_aromaticSteroids (Area).csv

Returns a dict with keys: instrument, fraction, method, data_type
"""

from __future__ import annotations

import logging
import os
import re

from .normalize import normalize_analysis

logger = logging.getLogger(__name__)

# Known instrument prefixes (case-insensitive match)
KNOWN_INSTRUMENTS = {"gcfid", "gcms", "gcmrmms"}

# Known chromatographic fraction prefixes in filenames (case-insensitive)
KNOWN_FRACTIONS_FILE = {"aliphatic", "aromatic", "nso", "nsosilyl"}


def parse_gc_filename(filename: str) -> dict[str, str | None]:
    """
    Parse a GC combined CSV filename and extract measurement metadata.

    Returns dict with:
        instrument  - e.g. 'GCFID', 'GCMS', 'GCMRMMS', or None
        fraction    - e.g. 'aliphatic', 'aromatic', 'NSO', 'NSOsilyl', or None
        method      - canonical method name (e.g. 'steranes', 'alkanes', 'whole_oil')
        data_type   - 'Area' or 'Concentration', or None
    """
    stem = os.path.basename(filename)

    # 1. Strip file extension
    stem = re.sub(r"\.csv$", "", stem, flags=re.IGNORECASE)

    # 2. Extract data_type from trailing parentheses: (Area) or (Concentration)
    data_type: str | None = None
    m = re.search(r"\((\w+)\)\s*$", stem)
    if m:
        dt = m.group(1).strip()
        if dt.lower() in ("area", "concentration"):
            data_type = dt.lower().capitalize()  # 'Area' or 'Concentration'
        stem = stem[: m.start()].strip()

    # 3. Remove '_combined' / ' combined' suffix
    stem = re.sub(r"[_ ]?combined\s*$", "", stem, flags=re.IGNORECASE).strip().rstrip("_- ")

    # 4. Try to extract instrument from first token (split on '-')
    instrument: str | None = None
    parts = stem.split("-", 1)
    if parts[0].strip().lower() in KNOWN_INSTRUMENTS:
        instrument = parts[0].strip().upper()
        remainder = parts[1].strip() if len(parts) > 1 else ""
    else:
        remainder = stem

    # 5. Try to extract fraction from next token
    fraction: str | None = None
    if remainder:
        # Split on first '-' or '_' to check fraction
        sub = re.split(r"[-_]", remainder, maxsplit=1)
        if sub[0].strip().lower() in KNOWN_FRACTIONS_FILE:
            fraction = sub[0].strip()
            method_raw = sub[1].strip() if len(sub) > 1 else ""
        else:
            method_raw = remainder
    else:
        method_raw = ""

    # 6. Normalize method to canonical table name
    if method_raw:
        method = normalize_analysis(method_raw)
    else:
        method = "unknown"
        logger.warning("Could not extract method from filename: %s", filename)

    return {
        "instrument": instrument,
        "fraction": fraction,
        "method": method,
        "data_type": data_type,
    }
