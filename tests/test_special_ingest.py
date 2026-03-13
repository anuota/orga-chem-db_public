"""Tests for db_code.services.special_ingest pure helpers."""
from __future__ import annotations

import os
import tempfile

import pytest

from db_code.services.special_ingest import (
    _build_ft_rows,
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


# ---------------------------------------------------------------------------
# _build_ft_rows  (summary-only ingest)
# ---------------------------------------------------------------------------

_SAMPLE_CSV = (
    "signalNumber,observedExactMass_ion,observedAbundance_ion,signalNoise_ratio,"
    "calculatedExactMass_ion,sumFormula,carbon_12,hydrogen_1\n"
    "68,259.1515,2660678,13.4,259.1515,C17 H23 S,17,23\n"
    "80,261.1672,2490942,12.4,261.1672,C17 H25 S,17,25\n"
    "132,271.1515,2693407,6.1,271.1515,C18 H23 S,18,23\n"
)


def test_build_ft_rows_returns_summary():
    with tempfile.TemporaryDirectory() as root:
        mode_dir = os.path.join(root, "APPIpos", "Tester", "Final")
        os.makedirs(mode_dir)
        csv_path = os.path.join(mode_dir, "Signallist_G000420_APPIpos.csv")
        with open(csv_path, "w") as f:
            f.write(_SAMPLE_CSV)

        rows = _build_ft_rows(csv_path, root_dir=root)

    assert len(rows) == 1
    row = rows[0]
    assert row["samplenumber"] == "G000420"

    entries = row["ft_icr_ms"]["entries"]
    assert len(entries) == 1, "Should be one summary entry, not one per signal"

    data = entries[0]["data"]
    assert data["peak_count"] == 3
    assert data["min_signal_to_noise"] == 6.1
    assert data["max_signal_to_noise"] == 13.4
    assert data["min_mass"] == 259.1515
    assert data["max_mass"] == 271.1515
    assert "APPIpos" in data["source_file"]
    assert entries[0]["method"] == "APPIpos"
    assert entries[0]["instrument"] == "FT-ICR-MS"


def test_build_ft_rows_no_sample_in_filename():
    with tempfile.TemporaryDirectory() as root:
        mode_dir = os.path.join(root, "APPIpos", "Tester", "Final")
        os.makedirs(mode_dir)
        csv_path = os.path.join(mode_dir, "Signallist_nosample_APPIpos.csv")
        with open(csv_path, "w") as f:
            f.write(_SAMPLE_CSV)

        rows = _build_ft_rows(csv_path, root_dir=root)
    assert rows == []
