"""Shared constants, helpers, and DB utilities used across api modules."""
from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path

from fastapi import Request

from db_code.infra.db_conn import PsycopgEnvConnectionProvider
from db_code.db_users import set_session_user

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALLOWED_TABLES = {
    "hopanes",
    "steranes",
    "alkanes",
    "naphthalenes",
    "phenanthrenes",
    "diamondoids",
    "terpanes",
    "thiophenes",
    "carbazoles",
    "alcohols",
    "fatty_acids",
    "ebfas",
    "etherlipids",
    "archaeolipids",
    "whole_oil",
    "fames",
    "fluorenes",
    "biphenyls",
    "aromatic_steroids",
    "norcholestanes",
    "alkylbenzenes",
    "ft_icr_ms",
    "isotope_co2_werte",
    "isotope_hd_werte",
}

LAB_METHODS = {
    "ft_icr_ms",
    "isotope_co2_werte",
    "isotope_hd_werte",
}

METHOD_LABELS = {
    "ft_icr_ms": "FT-ICR-MS",
    "isotope_co2_werte": "Isotope CO2 Werte",
    "isotope_hd_werte": "Isotope HD Werte",
}

PRESENCE_METHOD_LABELS = {
    "ft_icr_ms_appipos": "FT-ICR-MS APPIpos",
    "ft_icr_ms_esineg": "FT-ICR-MS ESIneg",
    "ft_icr_ms_esipos": "FT-ICR-MS ESIpos",
}

TABLE_ALIASES = {
    "n_alkanes_isoprenoids": "alkanes",
    "wo": "whole_oil",
    "whole_oil_gc": "whole_oil",
    "ft_icrms": "ft_icr_ms",
    "ft-icr-ms": "ft_icr_ms",
    "co2_werte": "isotope_co2_werte",
    "hd_werte": "isotope_hd_werte",
    "isotope_co2": "isotope_co2_werte",
    "isotope_hd": "isotope_hd_werte",
}

FT_MODE_LABELS = {
    "appipos": "APPIpos",
    "esineg": "ESIneg",
    "esipos": "ESIpos",
}

FT_MODE_TO_VIRTUAL = {
    "APPIpos": "ft_icr_ms_appipos",
    "ESIneg": "ft_icr_ms_esineg",
    "ESIpos": "ft_icr_ms_esipos",
}


def canonical_ft_mode(value: str | None) -> str | None:
    """Normalize an FT-ICR-MS ionization mode string to its canonical label."""
    if not value:
        return None
    key = re.sub(r"[^a-z0-9]+", "", str(value).lower())
    return FT_MODE_LABELS.get(key, str(value))


MATRIX_META_FIELDS = ["instrument", "fraction", "data_type", "name", "measured_by", "date"]

# Ordered method groups for the Data Explorer sidebar
METHOD_GROUPS: list[dict] = [
    {
        "label": "FT-ICR-MS",
        "methods": ["ft_icr_ms"],
    },
    {
        "label": "GC Compounds",
        "methods": [
            "hopanes",
            "steranes",
            "alkanes",
            "naphthalenes",
            "phenanthrenes",
            "diamondoids",
            "terpanes",
            "thiophenes",
            "carbazoles",
            "alcohols",
            "fatty_acids",
            "ebfas",
            "etherlipids",
            "archaeolipids",
            "whole_oil",
            "fames",
            "fluorenes",
            "biphenyls",
            "aromatic_steroids",
            "norcholestanes",
            "alkylbenzenes",
        ],
    },
    {
        "label": "Isotopes",
        "methods": ["isotope_co2_werte", "isotope_hd_werte"],
    },
]
MATRIX_META_LABELS = {
    "instrument": "Instrument",
    "fraction": "Fraction",
    "data_type": "Data Type",
    "name": "Name",
    "measured_by": "Operator",
    "date": "Date",
}

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def canonical_table_name(name: str) -> str:
    n = (name or "").strip()
    return TABLE_ALIASES.get(n, n)


def method_label(name: str) -> str:
    method = canonical_table_name(name)
    if method in METHOD_LABELS:
        return METHOD_LABELS[method]
    return method.replace("_", " ").title()


@lru_cache(maxsize=1)
def _column_order_map() -> dict[str, list[str]]:
    """Load column_order.json (generated from original CSV headers)."""
    p = Path(__file__).resolve().parent.parent / "app" / "data" / "column_order.json"
    if p.is_file():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}


def csv_column_order(table: str, keys: set[str]) -> list[str]:
    """Return *keys* ordered to match the original CSV header order.

    Keys present in column_order.json come first (in their CSV order),
    then any remaining keys (from DB but not in the JSON) are appended
    alphabetically.
    """
    order_map = _column_order_map()
    csv_order = order_map.get(table, [])
    ordered = [k for k in csv_order if k in keys]
    remaining = sorted(keys - set(ordered))
    return ordered + remaining


@lru_cache(maxsize=1)
def _display_name_map() -> dict[str, dict[str, str]]:
    """Load column_display_names.json (DB key → GFZ abbreviation)."""
    p = Path(__file__).resolve().parent.parent / "app" / "data" / "column_display_names.json"
    if p.is_file():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}


def column_display_name(table: str, key: str) -> str:
    """Return the GFZ abbreviation display name for a DB column key.

    Falls back to the raw *key* when no mapping exists.
    """
    canon = canonical_table_name(table)
    dn_map = _display_name_map()
    return dn_map.get(canon, {}).get(key, key)


# ---------------------------------------------------------------------------
# Database – RLS-aware query runner
# ---------------------------------------------------------------------------

_conn_provider = PsycopgEnvConnectionProvider()


def run_query_with_rls(sql_text: str, request: Request, params: tuple | list | None = None):
    user_id = getattr(request.state, "user", os.getenv("DEV_USER", "open"))
    with _conn_provider.get_connection() as conn, conn.cursor() as cur:
        set_session_user(cur, user_id)
        cur.execute(sql_text, params)
        cols = [d.name for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.rollback()
    return cols, rows
