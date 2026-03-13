"""Tests for compound info loader and web routes."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import api.main as main_mod


@pytest.fixture()
def client():
    return TestClient(main_mod.app, raise_server_exceptions=False)


# ---- Compound loader tests ----

def test_load_all_compounds():
    from api.compound_info import load_all_compounds
    compounds = load_all_compounds()
    assert len(compounds) > 50
    # All entries have at least a name or abbreviation
    for c in compounds:
        assert c["compound_name"] or c["abbrev1"]


def test_compound_index_lookup():
    from api.compound_info import compound_index
    idx = compound_index()
    # nC10 should be present (from alkanes CSV)
    assert "nc10" in idx
    entry = idx["nc10"]
    assert entry["compound_name"] == "n-decane"
    assert entry["cas"] == "124-18-5"
    assert entry["structure_graphic"]  # has a graphic


def test_graphics_index():
    from api.compound_info import _graphics_index
    gfx = _graphics_index()
    assert len(gfx) > 50
    # Check a known graphic
    assert "nc10" in gfx
    assert gfx["nc10"].endswith(".png")


# ---- Web route tests ----

def test_compounds_index_page(client):
    resp = client.get("/web/compounds")
    assert resp.status_code == 200
    assert "Compound Information" in resp.text
    assert "n-decane" in resp.text


def test_compound_detail_page(client):
    resp = client.get("/web/compounds/nC10")
    assert resp.status_code == 200
    assert "n-decane" in resp.text
    assert "124-18-5" in resp.text  # CAS number


def test_compound_detail_404(client):
    resp = client.get("/web/compounds/NONEXISTENT_COMPOUND")
    assert resp.status_code == 404
    assert "not found" in resp.text.lower()


def test_compound_graphic_serves_png(client):
    resp = client.get("/api/compounds/graphics/n-alkanes pictures/nC10.png")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "image/png"


def test_compound_graphic_404(client):
    resp = client.get("/api/compounds/graphics/nonexistent/file.png")
    assert resp.status_code == 404


# ---- Explorer meta includes method_groups ----

def test_explorer_meta_has_groups(client):
    from unittest.mock import patch
    import api.routes.explorer as explorer_mod

    def fake_rls(sql_text, _request):
        return ([], [])

    with patch.object(explorer_mod, "run_query_with_rls", side_effect=fake_rls):
        resp = client.get("/api/explorer/meta")
    assert resp.status_code == 200
    data = resp.json()
    assert "method_groups" in data


# ---- Column order preserves CSV ordering ----

def test_csv_column_order_uses_json():
    from api.shared import csv_column_order
    # steranes should come back in CSV header order, not alphabetical
    keys = {"27dbaS", "27dbaR", "27aaaS", "27aaaR"}
    ordered = csv_column_order("steranes", keys)
    assert set(ordered) == keys
    # 27dbaS should come before 27aaaS (CSV order), whereas
    # alphabetical would put 27aaaR first.
    assert ordered.index("27dbaS") < ordered.index("27aaaS")


def test_csv_column_order_fallback_alpha():
    from api.shared import csv_column_order
    # Unknown table should fall back to alphabetical
    keys = {"z_col", "a_col", "m_col"}
    ordered = csv_column_order("nonexistent_table_xyz", keys)
    assert ordered == ["a_col", "m_col", "z_col"]
