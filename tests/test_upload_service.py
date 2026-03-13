"""Tests for db_code.services.upload_service – CSV parsing helpers."""
from __future__ import annotations

import pytest

from db_code.services.upload_service import (
    _parse_csv_text,
    parse_gc_upload,
    parse_ft_upload,
    parse_isotope_upload,
)


# ---------------------------------------------------------------------------
# _parse_csv_text
# ---------------------------------------------------------------------------

class TestParseCsvText:
    def test_basic(self):
        csv = "SampleNumber,Name,Type,nC17\nG000392,AA,Oil,1234\n"
        header, rows = _parse_csv_text(csv)
        assert header == ["SampleNumber", "Name", "Type", "nC17"]
        assert len(rows) == 1
        assert rows[0][0] == "G000392"

    def test_bom(self):
        csv = "\ufeffSampleNumber,Value\nG000001,42\n"
        header, rows = _parse_csv_text(csv)
        assert header[0] == "SampleNumber"

    def test_no_header_raises(self):
        with pytest.raises(ValueError, match="no header"):
            _parse_csv_text("")

    def test_blank_rows_skipped(self):
        csv = "SampleNumber,Val\n\n  \nG000001,10\n\n"
        _, rows = _parse_csv_text(csv)
        assert len(rows) == 1

    def test_multi_header_blocks(self):
        csv = (
            "SampleNumber,A\nG000001,1\n"
            "SampleNumber,B\nG000002,2\n"
        )
        header, rows = _parse_csv_text(csv)
        # Last header wins for rows coming after it
        assert len(rows) == 2


# ---------------------------------------------------------------------------
# parse_gc_upload
# ---------------------------------------------------------------------------

class TestParseGcUpload:
    def test_single_sample(self):
        csv = "SampleNumber,Name,Type,nC17,nC18\nG000392,AA,Oil,100,200\n"
        rows = parse_gc_upload(csv, "hopanes", data_type="Area")
        assert len(rows) == 1
        assert rows[0]["samplenumber"] == "G000392"
        entries = rows[0]["hopanes"]["entries"]
        assert len(entries) == 1
        assert entries[0]["data"]["nC17"] == 100.0
        assert entries[0]["data_type"] == "Area"

    def test_multiple_samples(self):
        csv = (
            "SampleNumber,A\n"
            "G000001,10\n"
            "G000002,20\n"
        )
        rows = parse_gc_upload(csv, "hopanes")
        assert len(rows) == 2
        assert {r["samplenumber"] for r in rows} == {"G000001", "G000002"}

    def test_groups_fractions(self):
        csv = (
            "SampleNumber,A\n"
            "G000001-1,10\n"
            "G000001-2,20\n"
        )
        rows = parse_gc_upload(csv, "hopanes")
        assert len(rows) == 1
        assert rows[0]["samplenumber"] == "G000001"
        assert len(rows[0]["hopanes"]["entries"]) == 2

    def test_skips_missing_sample(self):
        csv = "SampleNumber,A\n,10\nBadRow,20\n"
        rows = parse_gc_upload(csv, "hopanes")
        # "BadRow" has no G-number so is skipped, blank is skipped
        assert len(rows) == 0

    def test_metadata_extraction(self):
        csv = "SampleNumber,Operator,Date,Type,Val\nG000001,Smith,2024-01-15,oil,99\n"
        rows = parse_gc_upload(csv, "hopanes")
        entry = rows[0]["hopanes"]["entries"][0]
        assert entry["measured_by"] == "Smith"
        assert entry["type"] is not None

    def test_instrument_and_data_type(self):
        csv = "SampleNumber,X\nG000001,5\n"
        rows = parse_gc_upload(csv, "hopanes", instrument="GCFID", data_type="Concentration")
        entry = rows[0]["hopanes"]["entries"][0]
        assert entry["instrument"] == "GCFID"
        assert entry["data_type"] == "Concentration"


# ---------------------------------------------------------------------------
# parse_ft_upload
# ---------------------------------------------------------------------------

class TestParseFtUpload:
    def test_basic_ft(self):
        csv = (
            "signalNoise_ratio,observedExactMass_ion,some_col\n"
            "5.0,200.123,abc\n"
            "10.0,400.567,xyz\n"
        )
        rows = parse_ft_upload(csv, "esineg", "Smith", "Signallist_G000001_ESIneg.csv")
        assert len(rows) == 1
        assert rows[0]["samplenumber"] == "G000001"
        entry = rows[0]["ft_icr_ms"]["entries"][0]
        assert entry["data"]["peak_count"] == 2
        assert entry["data"]["min_signal_to_noise"] == 5.0
        assert entry["data"]["max_mass"] == 400.567
        assert entry["data_type"] == "ESIneg"
        assert entry["measured_by"] == "Smith"

    def test_bad_mode_raises(self):
        with pytest.raises(ValueError, match="Unknown FT mode"):
            parse_ft_upload("a,b\n1,2\n", "badmode", "op", "G000001.csv")

    def test_no_sample_in_filename_raises(self):
        with pytest.raises(ValueError, match="Cannot extract sample number"):
            parse_ft_upload("a,b\n1,2\n", "esipos", "op", "noSample.csv")

    def test_empty_csv_raises(self):
        with pytest.raises(ValueError, match="no data"):
            parse_ft_upload("col_a,col_b\n", "esipos", "op", "G000001.csv")


# ---------------------------------------------------------------------------
# parse_isotope_upload
# ---------------------------------------------------------------------------

class TestParseIsotopeUpload:
    def test_basic_isotope(self):
        csv = "SampleNumber,d13C,d18O\nG000100,−28.5,−5.2\nG000101,-30.1,-6.0\n"
        table, rows = parse_isotope_upload(csv, "co2_werte", "Lab")
        assert table == "isotope_co2_werte"
        assert len(rows) == 2
        entry = rows[0]["isotope_co2_werte"]["entries"][0]
        assert entry["instrument"] == "CO2_WERTE"
        assert entry["measured_by"] == "Lab"

    def test_bad_kind_raises(self):
        with pytest.raises(ValueError, match="Unknown isotope kind"):
            parse_isotope_upload("SampleNumber,x\nG000001,1\n", "badkind", "op")

    def test_groups_by_sample(self):
        csv = (
            "SampleNumber,Param\n"
            "G000001,10\n"
            "G000001,20\n"
            "G000002,30\n"
        )
        table, rows = parse_isotope_upload(csv, "hd_werte", "op")
        assert table == "isotope_hd_werte"
        assert len(rows) == 2
        # G000001 should have 2 entries
        r1 = next(r for r in rows if r["samplenumber"] == "G000001")
        assert len(r1["isotope_hd_werte"]["entries"]) == 2
