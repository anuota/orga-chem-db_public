"""Tests for presence route helper functions."""
from __future__ import annotations

import pytest

from api.routes.presence import (
    canonical_presence_col,
    presence_alias_cols,
    presence_method_category,
    presence_method_label,
    presence_method_link,
)


# ---------------------------------------------------------------------------
# presence_method_label
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name, expected",
    [
        ("ft_icr_ms_appipos", "FT-ICR-MS APPIpos"),
        ("ft_icr_ms_esineg", "FT-ICR-MS ESIneg"),
        ("ft_icr_ms_esipos", "FT-ICR-MS ESIpos"),
        ("hopanes", "Hopanes"),
        ("alkanes", "N Alkanes Isoprenoids"),  # alias resolved
    ],
)
def test_presence_method_label(name, expected):
    assert presence_method_label(name) == expected


# ---------------------------------------------------------------------------
# presence_method_category
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name, expected",
    [
        ("ft_icr_ms", "ft"),
        ("ft_icr_ms_appipos", "ft"),
        ("isotope_co2_werte", "isotope"),
        ("isotope_hd_werte", "isotope"),
        ("hopanes", "gc"),
        ("steranes", "gc"),
    ],
)
def test_presence_method_category(name, expected):
    assert presence_method_category(name) == expected


# ---------------------------------------------------------------------------
# presence_method_link
# ---------------------------------------------------------------------------

def test_presence_method_link_ft():
    assert presence_method_link("ft_icr_ms") == "/web/labdata/ft-icr-ms"
    assert presence_method_link("ft_icr_ms_appipos") == "/web/labdata/ft-icr-ms"


def test_presence_method_link_gc():
    assert presence_method_link("hopanes") == "/web/matrix/hopanes"


# ---------------------------------------------------------------------------
# canonical_presence_col
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "col, expected",
    [
        ("has_alkanes", "has_n_alkanes_isoprenoids"),
        ("has_wo", "has_whole_oil"),
        ("has_hopanes", "has_hopanes"),
        ("samplenumber", "samplenumber"),  # non has_ col unchanged
    ],
)
def test_canonical_presence_col(col, expected):
    assert canonical_presence_col(col) == expected


# ---------------------------------------------------------------------------
# presence_alias_cols
# ---------------------------------------------------------------------------

def test_presence_alias_cols_with_aliases():
    cols = presence_alias_cols("has_n_alkanes_isoprenoids")
    assert "has_n_alkanes_isoprenoids" in cols
    assert "has_alkanes" in cols


def test_presence_alias_cols_no_alias():
    cols = presence_alias_cols("has_hopanes")
    assert cols == ["has_hopanes"]


def test_presence_alias_cols_non_has_col():
    assert presence_alias_cols("samplenumber") == ["samplenumber"]
