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
        assert resp.headers.get("content-type") == "text/csv; charset=utf-8"
        assert "selected_samples_matrix.csv" in resp.headers.get("content-disposition", "")
        assert "samplenumber" in resp.text
        assert "Hopanes: C27Tm" in resp.text
        assert "G000059" in resp.text


if __name__ == "__main__":
    unittest.main()
