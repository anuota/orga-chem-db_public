"""Unit tests for GC combined CSV filename parser."""

import pytest
from db_code.parsing.filename_parser import parse_gc_filename


@pytest.mark.parametrize(
    "filename, expected",
    [
        # Simple: no instrument, no fraction
        (
            "Alcohols_combined (Area).csv",
            {"instrument": None, "fraction": None, "method": "alcohols", "data_type": "Area"},
        ),
        (
            "EBFAs_combined (Area).csv",
            {"instrument": None, "fraction": None, "method": "ebfas", "data_type": "Area"},
        ),
        (
            "FAMEs_combined (Area).csv",
            {"instrument": None, "fraction": None, "method": "fames", "data_type": "Area"},
        ),
        (
            "Etherlipids_combined (Area).csv",
            {"instrument": None, "fraction": None, "method": "etherlipids", "data_type": "Area"},
        ),
        (
            "Archaelipids_combined (Area).csv",
            {"instrument": None, "fraction": None, "method": "archaeolipids", "data_type": "Area"},
        ),
        (
            "Norcholestanes_combined (Area).csv",
            {"instrument": None, "fraction": None, "method": "norcholestanes", "data_type": "Area"},
        ),
        # Instrument only (no fraction)
        (
            "GCFID-WO_combined (Area).csv",
            {"instrument": "GCFID", "fraction": None, "method": "whole_oil", "data_type": "Area"},
        ),
        (
            "GCFID-WO_combined (Concentration).csv",
            {"instrument": "GCFID", "fraction": None, "method": "whole_oil", "data_type": "Concentration"},
        ),
        # Instrument + fraction
        (
            "GCFID-aliphatic-alkanes_combined (Concentration).csv",
            {"instrument": "GCFID", "fraction": "aliphatic", "data_type": "Concentration"},
        ),
        (
            "GCFID-aliphatic_alkanes_combined (Area).csv",
            {"instrument": "GCFID", "fraction": "aliphatic", "data_type": "Area"},
        ),
        (
            "GCMRMMS-aliphatic_hopanes_combined (Area).csv",
            {"instrument": "GCMRMMS", "fraction": "aliphatic", "method": "hopanes", "data_type": "Area"},
        ),
        (
            "GCMRMMS-aliphatic_steranes_combined (Area).csv",
            {"instrument": "GCMRMMS", "fraction": "aliphatic", "method": "steranes", "data_type": "Area"},
        ),
        (
            "GCMRMMS-aliphatic_steranes_combined (concentration).csv",
            {"instrument": "GCMRMMS", "fraction": "aliphatic", "method": "steranes", "data_type": "Concentration"},
        ),
        # GCMS with NSO fraction
        (
            "GCMS-NSO_carbazoles_combined (Area).csv",
            {"instrument": "GCMS", "fraction": "NSO", "method": "carbazoles", "data_type": "Area"},
        ),
        (
            "GCMS-NSOsilyl_fattyAcids_combined (Area).csv",
            {"instrument": "GCMS", "fraction": "NSOsilyl", "method": "fatty_acids", "data_type": "Area"},
        ),
        # GCMS with aliphatic fraction
        (
            "GCMS-aliphatic_Diamondoids_combined (Area).csv",
            {"instrument": "GCMS", "fraction": "aliphatic", "method": "diamondoids", "data_type": "Area"},
        ),
        (
            "GCMS-aliphatic_terpanes_combined (Area).csv",
            {"instrument": "GCMS", "fraction": "aliphatic", "method": "terpanes", "data_type": "Area"},
        ),
        # GCMS with aromatic fraction
        (
            "GCMS-aromatic_biphenyls_combined (Area).csv",
            {"instrument": "GCMS", "fraction": "aromatic", "method": "biphenyls", "data_type": "Area"},
        ),
        (
            "GCMS-aromatic_naphthalenes_combined (Area).csv",
            {"instrument": "GCMS", "fraction": "aromatic", "method": "naphthalenes", "data_type": "Area"},
        ),
        (
            "GCMS-aromatic_phenanthrenes_combined (Area).csv",
            {"instrument": "GCMS", "fraction": "aromatic", "method": "phenanthrenes", "data_type": "Area"},
        ),
        (
            "GCMS-aromatic_thiophenes_combined (Area).csv",
            {"instrument": "GCMS", "fraction": "aromatic", "method": "thiophenes", "data_type": "Area"},
        ),
        # No '_combined' in filename
        (
            "GCMS-aromatic_aromaticSteroids (Area).csv",
            {"instrument": "GCMS", "fraction": "aromatic", "data_type": "Area"},
        ),
        # GCMS with NSO (without silyl)
        (
            "GCMS-NSO_fluorenes_combined (Area).csv",
            {"instrument": "GCMS", "fraction": "NSO", "method": "fluorenes", "data_type": "Area"},
        ),
    ],
)
def test_parse_gc_filename(filename, expected):
    result = parse_gc_filename(filename)
    for key, value in expected.items():
        assert result[key] == value, (
            f"Mismatch for '{filename}' field '{key}': "
            f"expected {value!r}, got {result[key]!r}"
        )


def test_parse_gc_filename_no_data_type():
    """Filename without parenthesized data type."""
    result = parse_gc_filename("Hopanes_combined.csv")
    assert result["data_type"] is None
    assert result["method"] == "hopanes"
    assert result["instrument"] is None


def test_parse_gc_filename_full_path():
    """Should work with a full file path, not just basename."""
    result = parse_gc_filename("/data/GC-DataForDatabase/GCFID-WO_combined (Area).csv")
    assert result["instrument"] == "GCFID"
    assert result["method"] == "whole_oil"
    assert result["data_type"] == "Area"
