"""Tests for db_code.services.special_ingest pure helpers."""
from __future__ import annotations

import pytest

from db_code.services.special_ingest import (
    _extract_ft_mode,
    _extract_ft_operator,
    _extract_isotope_kind,
    _norm_token,
)


# ---------------------------------------------------------------------------
# _norm_token
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("APPIpos", "appipos"),
        ("ESI-neg", "esineg"),
        ("Final", "final"),
        ("  Some_Dir  ", "somedir"),
    ],
)
def test_norm_token(raw, expected):
    assert _norm_token(raw) == expected


# ---------------------------------------------------------------------------
# _extract_ft_mode
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "path, expected",
    [
        ("/data/APPIpos/operator/Final/Signallist_G000420_APPIpos.csv", "APPIpos"),
        ("/data/ESIneg/Someone/Final/Signallist_G001234_ESIneg.csv", "ESIneg"),
        ("/data/ESIpos/Someone/Final/Signallist_G005678_ESIpos.csv", "ESIpos"),
        ("/data/unknown/file.csv", None),
    ],
)
def test_extract_ft_mode(path, expected):
    assert _extract_ft_mode(path) == expected


# ---------------------------------------------------------------------------
# _extract_ft_operator
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "path, expected",
    [
        ("/data/APPIpos/Huiwen Yue/Final/Signallist_G000420_APPIpos.csv", "Huiwen Yue"),
        ("/data/ESIneg/John/Final/Signallist_G001234_ESIneg.csv", "John"),
    ],
)
def test_extract_ft_operator(path, expected):
    assert _extract_ft_operator(path) == expected


def test_extract_ft_operator_unknown_when_no_mode():
    assert _extract_ft_operator("/unrelated/path/file.csv") == "unknown"


# ---------------------------------------------------------------------------
# _extract_isotope_kind
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "path, expected",
    [
        ("/data/CO2_Werte/operator/combined.csv", "co2werte"),
        ("/data/HD_Werte/operator/combined.csv", "hdwerte"),
        ("/data/random/file.csv", None),
    ],
)
def test_extract_isotope_kind(path, expected):
    assert _extract_isotope_kind(path) == expected
