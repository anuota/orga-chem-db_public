"""Tests for DDL generators (tables & views)."""
from __future__ import annotations

import pytest

from db_code.ddl.tables import make_family_table_ddl
from db_code.ddl.views import make_entries_view_ddl, make_presence_view_ddl


# ---------------------------------------------------------------------------
# make_family_table_ddl
# ---------------------------------------------------------------------------

def test_family_table_ddl_basic():
    ddl = make_family_table_ddl("hopanes", "hopanes")
    assert "CREATE TABLE IF NOT EXISTS public.hopanes" in ddl
    assert "samplenumber TEXT NOT NULL" in ddl
    assert "hopanes   JSONB NOT NULL" in ddl
    assert "PRIMARY KEY (samplenumber)" in ddl


def test_family_table_ddl_different_json_col():
    ddl = make_family_table_ddl("steranes", "steranes")
    assert "public.steranes" in ddl
    assert "steranes   JSONB" in ddl


# ---------------------------------------------------------------------------
# make_entries_view_ddl
# ---------------------------------------------------------------------------

def test_entries_view_ddl_default_name():
    ddl = make_entries_view_ddl("hopanes", "hopanes")
    assert "DROP VIEW IF EXISTS public.hopanes_entries" in ddl
    assert "CREATE VIEW public.hopanes_entries" in ddl
    assert "t.samplenumber" in ddl
    assert "jsonb_array_elements" in ddl
    assert "t.hopanes" in ddl


def test_entries_view_ddl_custom_name():
    ddl = make_entries_view_ddl("steranes", "steranes", view_name="my_view")
    assert "public.my_view" in ddl
    assert "public.steranes" in ddl


def test_entries_view_ddl_extracts_fields():
    ddl = make_entries_view_ddl("hopanes", "hopanes")
    for field in ("measured_by", "type", "date", "fraction", "instrument", "data_type", "notes"):
        assert f"e->>'{field}'" in ddl


# ---------------------------------------------------------------------------
# make_presence_view_ddl
# ---------------------------------------------------------------------------

def test_presence_view_ddl_basic():
    ddl = make_presence_view_ddl("analysis_presence_simple", ["hopanes", "steranes"])
    assert "CREATE OR REPLACE VIEW public.analysis_presence_simple" in ddl
    assert "has_hopanes" in ddl
    assert "has_steranes" in ddl
    assert "SELECT samplenumber FROM public.hopanes" in ddl
    assert "SELECT samplenumber FROM public.steranes" in ddl


def test_presence_view_ddl_deduplicates():
    ddl = make_presence_view_ddl("pv", ["hopanes", "hopanes", "steranes"])
    assert ddl.count("has_hopanes") == 1  # deduplicated


def test_presence_view_ddl_empty_raises():
    with pytest.raises(ValueError, match="empty"):
        make_presence_view_ddl("pv", [])
