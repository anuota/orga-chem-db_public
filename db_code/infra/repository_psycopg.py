import os
import psycopg2
from contextlib import contextmanager
from typing import Any, Dict, List

from db_code.services.ports import Repository

class PsyRepo(Repository):
    def __init__(self, host=None, port=None, dbname=None, user=None, password=None):
        self.host = host or os.getenv("PGHOST", "localhost")
        self.port = port or os.getenv("PGPORT", "5432")
        self.dbname = dbname or os.getenv("PGDATABASE", "postgres")
        self.user = user or os.getenv("PGUSER", "postgres")
        self.password = password or os.getenv("PGPASSWORD")
        self._bound_user: str | None = None

    @contextmanager
    def _conn_cur(self):
        with psycopg2.connect(
            host=self.host, port=self.port, dbname=self.dbname,
            user=self.user, password=self.password
        ) as conn:
            with conn.cursor() as cur:
                if self._bound_user:
                    # IMPORTANT: quote the custom GUC key
                    cur.execute('SET LOCAL "app.user" = %s', (self._bound_user,))
                yield cur

    def with_rls_user(self, user: str) -> "PsyRepo":
        r = PsyRepo(self.host, self.port, self.dbname, self.user, self.password)
        r._bound_user = user
        return r

    def server_version(self) -> str:
        with self._conn_cur() as cur:
            cur.execute("SELECT version()")
            return cur.fetchone()[0]

    def get_presence(self, samplenumber: str) -> Dict[str, Any] | None:
        with self._conn_cur() as cur:
            cur.execute(
                "SELECT * FROM public.analysis_presence WHERE samplenumber = %s",
                (samplenumber,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [d.name for d in cur.description]
            return dict(zip(cols, row))

    def list_entries(self, table: str, samplenumber: str) -> List[Dict[str, Any]]:
        with self._conn_cur() as cur:
            cur.execute(
                f"SELECT measured_by, type, date, fraction, notes, data "
                f"FROM public.{table}_entries "
                f"WHERE samplenumber = %s",
                (samplenumber,),
            )
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    def search_samples(self, q: str, limit: int) -> List[str]:
        patt = f"%{q.strip()}%" if q else "%"
        with self._conn_cur() as cur:
            cur.execute(
                """
                SELECT samplenumber
                FROM public.analysis_presence
                WHERE samplenumber ILIKE %s
                ORDER BY samplenumber
                LIMIT %s
                """,
                (patt, limit),
            )
            return [r[0] for r in cur.fetchall()]

    def filter_samples(
        self, table: str, measured_by: str | None, type: str | None,
        fraction: str | None, limit: int
    ) -> List[str]:
        clauses, params = ["1=1", "samplenumber IS NOT NULL"], []
        if measured_by:
            clauses.append("measured_by ILIKE %s"); params.append(f"%{measured_by}%")
        if type:
            clauses.append("type = %s"); params.append(type)
        if fraction:
            clauses.append("fraction = %s"); params.append(fraction)
        params.append(limit)
        where = " AND ".join(clauses)
        sql = (
            f"SELECT DISTINCT samplenumber FROM public.{table}_entries "
            f"WHERE {where} ORDER BY samplenumber LIMIT %s"
        )
        with self._conn_cur() as cur:
            cur.execute(sql, params)
            return [r[0] for r in cur.fetchall()]