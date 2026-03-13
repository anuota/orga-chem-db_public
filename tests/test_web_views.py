"""Web-view integration tests (TestClient against FastAPI app)."""
from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

import api.main as main_mod
import api.routes.presence as presence_mod


@pytest.fixture()
def client():
    return TestClient(main_mod.app)


def test_root_redirects_to_presence(client):
    resp = client.get("/", follow_redirects=False)
    assert resp.status_code == 307
    assert resp.headers.get("location") == "/web/presence"


def test_sample_filter_page_renders(client):
    def fake_query(sql_text, _request):
        if "analysis_presence_simple" in sql_text:
            return (
                ["samplenumber", "project", "rock_type", "analysis_date", "operator_name"],
                [
                    {
                        "samplenumber": "G000059",
                        "project": "open",
                        "rock_type": "",
                        "analysis_date": "",
                        "operator_name": "",
                    }
                ],
            )
        raise AssertionError(f"Unexpected SQL: {sql_text}")

    with patch.object(presence_mod, "run_query_with_rls", side_effect=fake_query):
        resp = client.get("/web/samples/filter")
        assert resp.status_code == 200
        assert "Open mixed combined view" in resp.text
        assert "G000059" in resp.text


def test_selected_view_csv_download(client):
    """Download from the selected-samples view now returns a ZIP
    containing one CSV per method."""
    def fake_query(sql_text, _request):
        if "public.hopanes_entries" in sql_text:
            return (
                ["samplenumber", "data"],
                [
                    {
                        "samplenumber": "G000059",
                        "data": {"C27Tm": 1.23, "C27Ts": 2.34},
                    },
                    {
                        "samplenumber": "G000060",
                        "data": {"C27Tm": 3.21},
                    },
                ],
            )
        raise AssertionError(f"Unexpected SQL: {sql_text}")

    with patch.object(presence_mod, "run_query_with_rls", side_effect=fake_query):
        resp = client.get(
            "/web/presence/selected?s=G000059,G000060&m=hopanes&format=csv"
        )
        assert resp.status_code == 200
        assert resp.headers.get("content-type") == "application/zip"
        assert "selected_samples.zip" in resp.headers.get("content-disposition", "")
        import zipfile, io
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        names = zf.namelist()
        assert len(names) == 1
        csv_content = zf.read(names[0]).decode()
        assert "samplenumber" in csv_content
        assert "C27Tm" in csv_content
        assert "G000059" in csv_content


def test_selected_view_ft_split_by_mode(client):
    """FT-ICR-MS entries should be split into virtual methods by mode."""
    def fake_query(sql_text, _request):
        if "public.ft_icr_ms_entries" in sql_text:
            return (
                ["samplenumber", "method", "notes", "data"],
                [
                    {
                        "samplenumber": "G000001",
                        "method": "APPIpos",
                        "notes": "",
                        "data": {"peak_count": 100, "source_file": "a/b.csv"},
                    },
                    {
                        "samplenumber": "G000001",
                        "method": "ESIneg",
                        "notes": "",
                        "data": {"peak_count": 50, "source_file": "c/d.csv"},
                    },
                ],
            )
        return ([], [])

    with patch.object(presence_mod, "run_query_with_rls", side_effect=fake_query):
        # HTML view should show separate FT-ICR-MS columns per mode
        resp = client.get("/web/presence/selected?s=G000001&m=ft_icr_ms")
        assert resp.status_code == 200
        assert "FT-ICR-MS APPIpos" in resp.text
        assert "FT-ICR-MS ESIneg" in resp.text

        # ZIP download should contain one CSV per virtual method
        resp = client.get(
            "/web/presence/selected?s=G000001&m=ft_icr_ms&format=csv"
        )
        assert resp.status_code == 200
        import zipfile, io
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        names = sorted(zf.namelist())
        assert len(names) == 2
        assert any("APPIpos" in n for n in names)
        assert any("ESIneg" in n for n in names)
