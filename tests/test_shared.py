"""Tests for api.shared — canonical_table_name, method_label, constants."""
from __future__ import annotations

import pytest

from api.shared import (
    ALLOWED_TABLES,
    TABLE_ALIASES,
    canonical_table_name,
    method_label,
)


# ---------------------------------------------------------------------------
# canonical_table_name
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("hopanes", "hopanes"),
        ("alkanes", "alkanes"),
        ("wo", "whole_oil"),
        ("whole_oil_gc", "whole_oil"),
        ("ft_icrms", "ft_icr_ms"),
        ("ft-icr-ms", "ft_icr_ms"),
        ("co2_werte", "isotope_co2_werte"),
        ("hd_werte", "isotope_hd_werte"),
        ("steranes", "steranes"),       # identity — not an alias
        ("  hopanes  ", "hopanes"),      # whitespace trimmed
    ],
)
def test_canonical_table_name(raw, expected):
    assert canonical_table_name(raw) == expected


# ---------------------------------------------------------------------------
# method_label
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name, expected",
    [
        ("ft_icr_ms", "FT-ICR-MS"),
        ("isotope_co2_werte", "Isotope CO2 Werte"),
        ("isotope_hd_werte", "Isotope HD Werte"),
        ("hopanes", "Hopanes"),
        ("alkanes", "Alkanes"),
        # aliases resolve first
        ("n_alkanes_isoprenoids", "Alkanes"),
        ("wo", "Whole Oil"),
    ],
)
def test_method_label(name, expected):
    assert method_label(name) == expected


# ---------------------------------------------------------------------------
# Constants sanity
# ---------------------------------------------------------------------------

def test_all_aliases_resolve_to_allowed():
    """Every TABLE_ALIASES target must be in ALLOWED_TABLES."""
    for alias, canon in TABLE_ALIASES.items():
        assert canon in ALLOWED_TABLES, f"Alias '{alias}' -> '{canon}' not in ALLOWED_TABLES"
