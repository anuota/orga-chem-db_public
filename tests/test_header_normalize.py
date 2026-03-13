"""Tests for header normalization across Area / Concentration CSV naming."""
from __future__ import annotations

from unittest.mock import patch

from db_code.parsing.header_normalize import normalize_data_payload


# ── Steranes: strip Ster- prefix ──────────────────────────────────────────

def test_steranes_strips_ster_prefix():
    data = {"Ster-27dbaS": 1.0, "Ster-27dbaR": 2.0, "Ster-28aaaS": 3.0}
    out = normalize_data_payload("steranes", data)
    assert out == {"27dbaS": 1.0, "27dbaR": 2.0, "28aaaS": 3.0}


def test_steranes_strips_ster_without_hyphen():
    """Ster28dabS (missing hyphen) should also be normalised."""
    data = {"Ster28dabS": 5.0}
    out = normalize_data_payload("steranes", data)
    assert out == {"28dabS": 5.0}


def test_steranes_passthrough_area_names():
    """Area file names (no Ster- prefix) pass through unchanged."""
    data = {"27dbaS": 1.0, "27dbaR": 2.0}
    out = normalize_data_payload("steranes", data)
    assert out == {"27dbaS": 1.0, "27dbaR": 2.0}


# ── Whole Oil: concentration → Area canonical names ───────────────────────

def test_wo_maps_concentration_names():
    data = {
        "Benzene": 10.0,
        "Toloene": 20.0,
        "Pr.": 30.0,
        "Ph.": 40.0,
        "nC17": 50.0,  # unchanged
    }
    out = normalize_data_payload("whole_oil", data)
    assert out == {
        "Benzol": 10.0,
        "Tol": 20.0,
        "Pri": 30.0,
        "Phy": 40.0,
        "nC17": 50.0,
    }


def test_wo_drops_operator_name():
    data = {"Ahmad": "Christian", "nC17": 50.0}
    out = normalize_data_payload("whole_oil", data)
    assert "Ahmad" not in out
    assert out == {"nC17": 50.0}


def test_wo_separator_convention():
    data = {"2,2-DMB": 1.0, "1,1-DMCP": 2.0, "2,5-DMHex": 3.0}
    out = normalize_data_payload("whole_oil", data)
    assert out == {"2.2DMB": 1.0, "1.1DMCP": 2.0, "2.5DMHex": 3.0}


def test_wo_abbreviation_diffs():
    data = {"1,cis,3-DMCP": 1.0, "ISD": 2.0, "3MOct": 3.0, "iC9": 4.0}
    out = normalize_data_payload("whole_oil", data)
    assert out == {"1C3DMCP": 1.0, "IS 2.2.4TMP": 2.0, "3MO": 3.0, "C9so": 4.0}


def test_wo_area_names_passthrough():
    """Area-style names that are already canonical pass unchanged."""
    data = {"Benzol": 10.0, "Tol": 20.0, "Pri": 30.0, "2.2DMB": 40.0}
    out = normalize_data_payload("whole_oil", data)
    assert out == data


# ── N-Alkanes: static fallback for concentration variants ─────────────────

@patch("db_code.parsing.header_normalize._load_synonym_map", return_value={})
def test_alkanes_concentration_fallback(mock_syn):
    data = {
        "5aAndrostane": 1.0,
        "nC17+Pristan": 2.0,
        "nC18*": 3.0,
        "phytan*": 4.0,
        "nC20": 5.0,  # unchanged
    }
    out = normalize_data_payload("alkanes", data)
    assert out["5a-Androstane"] == 1.0
    assert out["nC17+Pristane"] == 2.0
    assert out["nC18"] == 3.0
    assert out["Phytane"] == 4.0
    assert out["nC20"] == 5.0


@patch("db_code.parsing.header_normalize._load_synonym_map", return_value={})
def test_alkanes_area_names_passthrough(mock_syn):
    """Area names not in synonym DB pass through unchanged."""
    data = {"5a-Androstane": 1.0, "Pristane": 2.0, "nC17": 3.0}
    out = normalize_data_payload("alkanes", data)
    assert out == data


# ── Other analyses: passthrough ───────────────────────────────────────────

def test_other_analysis_passthrough():
    data = {"col_a": 1.0, "col_b": 2.0}
    out = normalize_data_payload("hopanes", data)
    assert out == data


def test_none_analysis_passthrough():
    data = {"x": 1}
    out = normalize_data_payload(None, data)
    assert out == data
