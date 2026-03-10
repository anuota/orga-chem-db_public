# infra/upsert.py
from typing import Protocol, Sequence

# Reuse the canonical implementation from db_utils to avoid duplication
from db_code.db_utils import upsert_rows as _upsert_rows_impl
from db_code.infra.db_conn import ConnectionProvider


class Upserter(Protocol):
    def upsert(
        self,
        conn,
        *,
        table: str,
        rows: Sequence[dict],
        conflict_cols: Sequence[str],
        update_cols: Sequence[str] | None,
        json_cols: Sequence[str] | None,
        commit: bool
    ) -> int: ...


class DefaultUpserter:
    def __init__(self, conn_provider: ConnectionProvider):
        self.conn_provider = conn_provider

    def upsert(
        self, conn, *, table, rows, conflict_cols, update_cols, json_cols, commit
    ) -> int:
        # If a connection is provided (tests), use it; else own it.
        owned = False
        if conn is None:
            conn = self.conn_provider.get_connection()
            owned = True
        try:
            return _upsert_rows_impl(
                conn,
                table,
                list(rows),
                list(conflict_cols),
                list(update_cols or []),
                list(json_cols or []),
                commit,
            )
        finally:
            if owned:
                conn.close()
