"""Tests for db_code.parsing.normalize — sample/analysis normalisation helpers."""
from __future__ import annotations

import pytest

from db_code.parsing.normalize import (
    _clean_cell,
    _parse_date_like,
    check_fraction_consistency,
    explicit_fraction_from_sample,
    extract_base_fraction_notes,
    inferred_fraction_from_analysis,
    normalize_analysis,
    normalize_sample_number,
    normalize_type_label,
)


# ---------------------------------------------------------------------------
# normalize_sample_number
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("G000392-1a", "G000392"),
        ("g001953 (dup)", "G001953"),
        ("G003200-2", "G003200"),
        ("some text G012345 extra", "G012345"),
        ("no sample here", None),
        (None, None),
        ("", None),
    ],
)
def test_normalize_sample_number(raw, expected):
    assert normalize_sample_number(raw) == expected


# ---------------------------------------------------------------------------
# normalize_analysis
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("hopanes", "hopanes"),
        ("Hopanes_combined", "hopanes"),
        ("Flourenes", "fluorenes"),
        ("bipheniles", "biphenyls"),
        ("naphtalenes", "naphthalenes"),
        ("n-alkanes", "alkanes"),
        ("alkanes", "alkanes"),
        ("WO", "whole_oil"),
        ("whole oil", "whole_oil"),
        ("fatty acids", "fatty_acids"),
        ("archaelipids", "archaeolipids"),
    ],
)
def test_normalize_analysis(raw, expected):
    assert normalize_analysis(raw) == expected


# ---------------------------------------------------------------------------
# _clean_cell
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "val, expected",
    [
        (None, None),
        ("", None),
        ("  ", None),
        ("na", None),
        ("N.A.", None),
        ("null", None),
        ("1.23", 1.23),
        ("412 219", 412219.0),
        ("1,234", 1.234),    # single comma → European decimal
        ("hello", "hello"),
        (42, 42.0),            # numeric → parsed as float
    ],
)
def test_clean_cell(val, expected):
    result = _clean_cell(val)
    if isinstance(expected, float):
        assert result == pytest.approx(expected)
    else:
        assert result == expected


# ---------------------------------------------------------------------------
# _parse_date_like
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "val, expected",
    [
        ("2024-06-01", "2024-06-01"),
        ("01.06.2024", "2024-06-01"),
        ("01/07/2024", "2024-07-01"),
        ("2024/03/15", "2024-03-15"),
        ("not-a-date", None),
        (None, None),
        ("", None),
    ],
)
def test_parse_date_like(val, expected):
    assert _parse_date_like(val) == expected


# ---------------------------------------------------------------------------
# explicit_fraction_from_sample
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, label, code",
    [
        ("G003200-0", "whole extract from rock", "0"),
        ("G003200-1", "aliphatic", "1"),
        ("G003200-2", "aromatic", "2"),
        ("G003200-3", "NSO", "3"),
        ("G003200", None, None),
        ("G003200-5", None, None),  # code 5 not in 0-3
        (None, None, None),
    ],
)
def test_explicit_fraction_from_sample(raw, label, code):
    got_label, got_code = explicit_fraction_from_sample(raw)
    assert got_label == label
    assert got_code == code


# ---------------------------------------------------------------------------
# inferred_fraction_from_analysis
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "analysis, expected",
    [
        ("hopanes", "aliphatic"),
        ("naphthalenes", "aromatic"),
        ("carbazoles", "NSO"),
        ("whole_oil", "whole crude oil"),
        ("unknown_method", None),
        (None, None),
    ],
)
def test_inferred_fraction_from_analysis(analysis, expected):
    assert inferred_fraction_from_analysis(analysis) == expected


# ---------------------------------------------------------------------------
# extract_base_fraction_notes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, analysis, base, fraction, notes",
    [
        ("G003200-1", None, "G003200", "aliphatic", None),
        ("G003200-2", None, "G003200", "aromatic", None),
        ("G003200-0", None, "G003200", "whole extract from rock", None),
        ("G003200-3", None, "G003200", "NSO", None),
        ("G003200-1a", None, "G003200", "aliphatic", "a"),
        ("G003200 wdh", None, "G003200", "whole crude oil", "wdh"),
        # No explicit code → infer from analysis
        ("G003200", "hopanes", "G003200", "aliphatic", None),
        ("G003200", "naphthalenes", "G003200", "aromatic", None),
        (None, None, None, None, None),
    ],
)
def test_extract_base_fraction_notes(raw, analysis, base, fraction, notes):
    b, f, n = extract_base_fraction_notes(raw, analysis)
    assert b == base
    assert f == fraction
    assert n == notes


# ---------------------------------------------------------------------------
# check_fraction_consistency
# ---------------------------------------------------------------------------

def test_check_fraction_consistency_match():
    match, explicit, inferred, code = check_fraction_consistency("G003200-1", "hopanes")
    assert match is True
    assert explicit == "aliphatic"
    assert inferred == "aliphatic"
    assert code == "1"


def test_check_fraction_consistency_mismatch():
    match, explicit, inferred, code = check_fraction_consistency("G003200-2", "hopanes")
    assert match is False
    assert explicit == "aromatic"
    assert inferred == "aliphatic"
    assert code == "2"


def test_check_fraction_consistency_no_explicit():
    match, explicit, inferred, code = check_fraction_consistency("G003200", "phenanthrenes")
    assert match is None
    assert explicit is None
    assert inferred == "aromatic"
    assert code is None
