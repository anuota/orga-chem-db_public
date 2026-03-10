# migrunner.py
"""
Apply migrations/*.sql in lexical order and record them in public.schema_migrations.
Env: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
"""
from __future__ import annotations
import os, sys, glob
import psycopg2

import logging

logging.basicConfig(level=logging.INFO)

MIGRATIONS_DIR = os.environ.get(
    "MIGRATIONS_DIR", os.path.join(os.getcwd(), "migrations")
)

DDL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    filename   TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def _connect():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "postgres"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD"),
    )


def _applied(cur, filename: str) -> bool:
    cur.execute("SELECT 1 FROM public.schema_migrations WHERE filename=%s", (filename,))
    return cur.fetchone() is not None


def _mark(cur, filename: str) -> None:
    cur.execute(
        "INSERT INTO public.schema_migrations(filename) VALUES (%s)", (filename,)
    )


def _apply_one(conn, path: str) -> None:
    fn = os.path.basename(path)
    with conn.cursor() as cur:
        if _applied(cur, fn):
            print(f"[migrunner] already applied: {fn}")
            return
        print(f"[migrunner] applying: {fn}")
        with open(path, "r", encoding="utf-8") as f:
            cur.execute(f.read())
        _mark(cur, fn)
    conn.commit()


def run() -> int:
    try:
        conn = _connect()
    except Exception as e:
        print(f"[migrunner] connection failed: {e}")
        return 2
    try:
        with conn.cursor() as cur:
            cur.execute(DDL_CREATE_TABLE)
        conn.commit()
        files = sorted(glob.glob(os.path.join(MIGRATIONS_DIR, "*.sql")))
        if not files:
            print(f"[migrunner] no migrations found in {MIGRATIONS_DIR}")
            return 0
        for p in files:
            _apply_one(conn, p)
        print("[migrunner] all migrations up-to-date")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(run())
