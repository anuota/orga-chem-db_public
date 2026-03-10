"""Tests for lab route helper functions."""
from __future__ import annotations

from pathlib import Path

import pytest

from api.routes.lab import _canonical_ft_mode, _ft_measurement_code_from_notes, _safe_measurement_date


# ---------------------------------------------------------------------------
# _canonical_ft_mode
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("APPIpos", "APPIpos"),
        ("appipos", "APPIpos"),
        ("ESIneg", "ESIneg"),
        ("esineg", "ESIneg"),
        ("ESIpos", "ESIpos"),
        ("  ESIpos  ", "ESIpos"),
        (None, None),
        ("", None),
        ("unknown_mode", "unknown_mode"),
    ],
)
def test_canonical_ft_mode(raw, expected):
    assert _canonical_ft_mode(raw) == expected


# ---------------------------------------------------------------------------
# _ft_measurement_code_from_notes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "notes, fallback, exp_code, exp_mode",
    [
        ("Signallist_G000420ACM_APPIpos.csv", "G000420", "G000420ACM", "APPIpos"),
        ("Signallist_G005393_ESIneg.csv", "G005393", "G005393", "ESIneg"),
        ("random_notes.csv", "G001234", "G001234", None),
        (None, "G007777", "G007777", None),
    ],
)
def test_ft_measurement_code_from_notes(notes, fallback, exp_code, exp_mode):
    code, mode = _ft_measurement_code_from_notes(notes, fallback)
    assert code == exp_code
    assert mode == exp_mode


# ---------------------------------------------------------------------------
# _safe_measurement_date
# ---------------------------------------------------------------------------

def test_safe_measurement_date_with_raw_value():
    assert _safe_measurement_date("2024-06-01", None) == "2024-06-01"


def test_safe_measurement_date_none_no_file():
    assert _safe_measurement_date(None, None) is None


def test_safe_measurement_date_empty_no_file():
    assert _safe_measurement_date("", None) is None


def test_safe_measurement_date_from_file(tmp_path):
    p = tmp_path / "signal.csv"
    p.write_text("data")
    result = _safe_measurement_date(None, p)
    assert result is not None
    assert "UTC" in result


def test_safe_measurement_date_missing_file():
    result = _safe_measurement_date(None, Path("/nonexistent/file.csv"))
    assert result is None
