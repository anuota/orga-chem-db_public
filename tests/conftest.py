"""Shared test fixtures for the org-chem-db test suite."""
from __future__ import annotations


class FakeCursor:
    """Lightweight cursor mock that records SQL calls."""

    def __init__(self, log: list):
        self.log = log

    def execute(self, sql, params=None):
        self.log.append(("EXECUTE", str(sql), params))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    """Lightweight connection mock that records SQL calls and commits."""

    def __init__(self):
        self.log: list = []
        self.commits = 0

    def cursor(self):
        return FakeCursor(self.log)

    def commit(self):
        self.commits += 1
        self.log.append(("COMMIT",))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def fake_execute_values(cur, sql, values):
    """Stand-in for psycopg2.extras.execute_values."""
    cur.execute(sql, values)
