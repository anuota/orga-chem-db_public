"""Tests migrated from db_code/db_tests.py — db_utils upsert/CSV parsing and ACL/RLS."""
from __future__ import annotations

import os
import tempfile
import textwrap

import pytest

import db_code.db_users as acl
import db_code.db_utils as du
from db_code.parsing.normalize import normalize_type_label
from tests.conftest import FakeConn, fake_execute_values


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _patch_execute_values(monkeypatch):
    """Replace psycopg2 execute_values with our fake so no DB is needed."""
    monkeypatch.setattr(du, "execute_values", fake_execute_values)


# ---------------------------------------------------------------------------
# db_utils – upsert / CSV parsing
# ---------------------------------------------------------------------------

class TestUpsertRows:
    def test_dedup_and_distinct_merge(self):
        conn = FakeConn()
        r1 = {
            "samplenumber": "G000123",
            "hopanes": {
                "entries": [
                    {
                        "measured_by": "Alice",
                        "type": "Aliphatics",
                        "date": "2024-06-01",
                        "fraction": "aliphatic",
                        "notes": "_1",
                        "data": {"Ts": 1.23},
                    }
                ]
            },
        }
        r2 = {
            "samplenumber": "G000123",
            "hopanes": {
                "entries": [
                    {
                        "measured_by": "Alice",
                        "type": "Aliphatics",
                        "date": "2024-06-01",
                        "fraction": "aliphatic",
                        "notes": "_1",
                        "data": {"Ts": 1.23},
                    },
                    {
                        "measured_by": "Bob",
                        "type": "Aromatics",
                        "date": "2024-06-02",
                        "fraction": "aromatic",
                        "notes": None,
                        "data": {"Tm": 9.87},
                    },
                ]
            },
        }
        inserted = du.upsert_rows(
            conn,
            table="public.hopanes",
            rows=[r1, r2],
            conflict_cols=["samplenumber"],
            update_cols=["hopanes"],
            json_cols=["hopanes"],
            commit=True,
        )
        assert inserted == 1

        sql_texts = "\n".join(
            item[1] for item in conn.log if item and item[0] == "EXECUTE"
        )
        assert "ON CONFLICT" in sql_texts
        has_distinct_on = "SELECT DISTINCT ON" in sql_texts
        has_distinct_agg = "jsonb_agg(DISTINCT elem)" in sql_texts
        assert has_distinct_on or has_distinct_agg, (
            "Expected SQL dedupe pattern not found in UPSERT statement"
        )
        assert "jsonb_agg(d.elem)" in sql_texts or has_distinct_agg, (
            "Expected jsonb aggregation pattern not found in UPSERT statement"
        )

        payloads = [item[2] for item in conn.log if item and item[0] == "EXECUTE"][0]
        assert any(isinstance(v, (list, tuple)) for v in payloads)


class TestMultiHeaderCsv:
    def test_grouped_parses(self):
        content = textwrap.dedent("""\
            SampleNumber,Name,Type,Ts,Tm,Date
            G000111-1,Alice,Aliphatics,1.0,,2024-06-01
            G000111-2,Alice,Aromatics,,2.0,2024-06-02
            SampleNumber,Name,Type,Ts,Tm,Date
            G000222,Alice,Aliphatics,3.0,4.0,01/07/2024
        """)
        with tempfile.NamedTemporaryFile("w+", suffix=".csv", delete=False) as f:
            f.write(content)
            f.flush()
            path = f.name
        try:
            rows = du.rows_from_multiheader_csv_grouped(path, json_col="hopanes")
            assert len(rows) == 2
            by_sn = {r["samplenumber"]: r for r in rows}
            assert "G000111" in by_sn
            assert "G000222" in by_sn

            e1 = by_sn["G000111"]["hopanes"]["entries"]
            assert len(e1) == 2
            fractions = sorted({e.get("fraction") for e in e1})
            assert "aliphatic" in fractions
            assert "aromatic" in fractions

            e2 = by_sn["G000222"]["hopanes"]["entries"][0]
            assert e2["date"] == "2024-07-01"
        finally:
            os.unlink(path)


class TestFractionConsistency:
    @pytest.mark.parametrize(
        "sample, analysis, match, label",
        [
            ("G003200-1", "hopanes", True, "aliphatic"),
            ("G003200-2", "hopanes", False, "aromatic"),
            ("G003200", "phenanthrenes", None, "aromatic"),
        ],
    )
    def test_fraction_cases(self, sample, analysis, match, label):
        m = du.check_fraction_consistency(sample, analysis)
        assert m[0] == match
        if m[1] is not None:
            assert m[1] == label
        else:
            assert m[2] == label

    def test_fuzzy_analysis_name(self):
        m = du.check_fraction_consistency("G003200", "Flourenes")
        assert m[0] is None
        assert m[2] == "aromatic"


# ---------------------------------------------------------------------------
# db_users – ACL / RLS
# ---------------------------------------------------------------------------

class TestRls:
    def test_identity_schema_and_rls_policy_sql(self):
        conn = FakeConn()
        acl.ensure_identity_schema(conn)
        acl.ensure_rls_for_tables(conn, ["hopanes", "steranes"])
        sql_texts = "\n".join(
            item[1] for item in conn.log if item and item[0] == "EXECUTE"
        )
        assert "CREATE TABLE IF NOT EXISTS public.subjects" in sql_texts
        assert "CREATE TABLE IF NOT EXISTS public.group_members" in sql_texts
        assert "CREATE TABLE IF NOT EXISTS public.sample_acl" in sql_texts
        assert "CREATE OR REPLACE FUNCTION public.current_user_id()" in sql_texts
        assert "CREATE OR REPLACE VIEW public.current_subjects" in sql_texts
        assert "ALTER TABLE public.hopanes ENABLE ROW LEVEL SECURITY" in sql_texts
        assert "CREATE POLICY hopanes_read" in sql_texts
        assert "CREATE POLICY steranes_read" in sql_texts

    def test_set_session_user(self):
        conn = FakeConn()
        with conn.cursor() as cur:
            acl.set_session_user(cur, "anna.mueller")
        sql_texts = "\n".join(s for t, s, *_ in conn.log if t == "EXECUTE")
        assert "app" in sql_texts.lower() and "user" in sql_texts.lower()


# ---------------------------------------------------------------------------
# normalize_type_label
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("StandardĂ¶l", "standard oil"),
        ("Öl", "oil"),
        ("Ă–l", "oil"),
        ("Gestein", "rock"),
        ("Muttergestein", "source rock"),
        (" standard  Öl  ", "standard oil"),
    ],
)
def test_normalize_type_label(raw, expected):
    assert normalize_type_label(raw) == expected
