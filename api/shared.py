"""Shared constants, helpers, and DB utilities used across api modules."""
from __future__ import annotations

import os

from fastapi import Request

from db_code.infra.db_conn import PsycopgEnvConnectionProvider
from db_code.db_users import set_session_user

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALLOWED_TABLES = {
    "hopanes",
    "steranes",
    "n_alkanes_isoprenoids",
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
    "alkanes": "n_alkanes_isoprenoids",
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

MATRIX_META_FIELDS = ["instrument", "fraction", "data_type", "name", "measured_by", "date"]
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


# ---------------------------------------------------------------------------
# Database – RLS-aware query runner
# ---------------------------------------------------------------------------

_conn_provider = PsycopgEnvConnectionProvider()


def run_query_with_rls(sql_text: str, request: Request):
    user_id = getattr(request.state, "user", os.getenv("DEV_USER", "open"))
    with _conn_provider.get_connection() as conn, conn.cursor() as cur:
        set_session_user(cur, user_id)
        cur.execute(sql_text)
        cols = [d.name for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.rollback()
    return cols, rows
