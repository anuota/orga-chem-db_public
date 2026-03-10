# infra/db_conn.py
import os
from typing import Protocol

import psycopg2


class ConnectionProvider(Protocol):
    def get_connection(self): ...


class PsycopgEnvConnectionProvider:
    def __init__(self, *, dbname=None, user=None, password=None, host=None, port=None):
        self.dbname = dbname or os.getenv("PGDATABASE", "postgres")
        self.user = user or os.getenv("PGUSER", "postgres")
        self.password = password or os.getenv("PGPASSWORD")
        self.host = host or os.getenv("PGHOST", "localhost")
        self.port = port or os.getenv("PGPORT", "5432")

    def get_connection(self):
        return psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
