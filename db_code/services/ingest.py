# services/ingest.py
from typing import Sequence
import re
import os
import psycopg2
from db_code.infra.db_conn import PsycopgEnvConnectionProvider
from db_code.infra.upsert import DefaultUpserter, Upserter
from db_code.parsing.csv_multiheader import (CsvParseStrategy,
                                             MultiHeaderEntriesStrategy)
from db_code.parsing.normalize import normalize_analysis
from db_code.parsing.filename_parser import parse_gc_filename


# --- Synonym/canonical helpers for n-alkanes ---
def _norm_synonym_key(s: str) -> str:
    """
    Normalize header/synonym for dictionary lookup: lower, strip, unify dashes/spaces.
    Examples: 'nC13' -> 'nc13'; 'n-C13' -> 'n-c13'; ' C31  ' -> 'c31'.
    """
    if s is None:
        return ""
    t = str(s).strip().lower()
    # unify various dashes to hyphen and collapse spaces
    t = t.replace("–", "-").replace("—", "-")
    t = " ".join(t.split())
    # common patterns for n-alkanes
    t = t.replace("_", "-")
    # drop leading 'n' when written without dash (e.g., 'nc13' -> 'n-c13'-like key)
    if t.startswith("nc") and len(t) > 2 and t[2].isdigit():
        t = "n-" + t[1:]
    return t


def _load_synonyms_map(conn) -> dict[str, str]:
    """
    {synonym(normalized) -> canonical_name} for n-alkanes.
    Sources:
      - ref_n_alkanes.gfz_short (e.g., 'nC18'),
      - ref_n_alkanes.canonical_name (self-alias),
      - ref_n_alkane_synonyms (if present).
    """
    q = """
        SELECT LOWER(TRIM(syn)) AS syn, canon FROM (
            -- явные синонимы (может быть пусто)
            SELECT s.synonym AS syn, r.canonical_name AS canon
            FROM public.ref_n_alkane_synonyms s
            JOIN public.ref_n_alkanes r USING (compound_id)
            UNION ALL
            -- GFZ_short как синоним (главный источник: 'nC18', 'n-C13' и т.д.)
            SELECT r.gfz_short AS syn, r.canonical_name AS canon
            FROM public.ref_n_alkanes r
            WHERE r.gfz_short IS NOT NULL AND TRIM(r.gfz_short) <> ''
            UNION ALL
            -- canonical_name сам на себя (если в данных уже канон)
            SELECT r.canonical_name AS syn, r.canonical_name AS canon
            FROM public.ref_n_alkanes r
            WHERE r.canonical_name IS NOT NULL AND TRIM(r.canonical_name) <> ''
        ) u
        WHERE syn IS NOT NULL AND TRIM(syn) <> ''
    """
    out: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute(q)
        for syn, canon in cur.fetchall():
            out[_norm_synonym_key(syn)] = canon
    return out


def _canonize_header_with_suffix(header: str, synmap: dict[str, str]) -> str:
    """
    If the header contains a '+', canonicalize only the left token and keep the right intact.
      "nC18+Phytane"           -> "<canon(nC18)>+Phytane"
      "n-C13 + Pristane"       -> "<canon(n-C13)> + Pristane"
    If there is no '+', behave like single-token canonicalization.
    """
    if not isinstance(header, str):
        return header

    # 0) попробуем целиком — вдруг уже канон или явный алиас
    full = synmap.get(_norm_synonym_key(header))
    if full:
        return full

    # 1) разбор на левый/правый по '+', позволяя пробелы вокруг
    if "+" in header:
        parts = re.split(r"\s*\+\s*", header, maxsplit=1)
        left, right = parts[0], parts[1] if len(parts) > 1 else ""
        canon_left = synmap.get(_norm_synonym_key(left))
        if canon_left:
            # Сохраняем исходный формат «плюса»: ставим как "<canon>+<right>" без трогания right
            return f"{canon_left}+{right}"
        # если левый не распознали — оставляем как было
        return header

    # 2) обычный одиночный ключ
    single = synmap.get(_norm_synonym_key(header))
    return single or header

def _canonize_entries(entries: list[dict], synmap: dict[str, str]) -> list[dict]:
    """
    For each entry in entries, rewrite data keys to canonical names when a synonym is known.
    Store values as plain scalars under canonical keys.
    Shape preserved: {"entries":[{"data":{canon: value, ...}, ... }]}
    """
    out: list[dict] = []
    for e in entries:
        data = e.get("data") or {}
        new_data: dict = {}
        for header, payload in data.items():
            # payload may be raw value or legacy {"orig":..., "value":...}; normalize robustly
            if isinstance(payload, dict) and "value" in payload:
                val = payload.get("value")
            else:
                val = payload
            key_out = _canonize_header_with_suffix(header, synmap)
            new_data[key_out] = val
        new_e = dict(e)
        new_e["data"] = new_data
        out.append(new_e)
    return out



class TableIngestor:
    def __init__(
        self,
        *,
        upserter: Upserter | None = None,
        parser: CsvParseStrategy | None = None
    ):
        self.upserter = upserter or DefaultUpserter(PsycopgEnvConnectionProvider())
        self.parser = parser or MultiHeaderEntriesStrategy()
        self._synmap_cache: dict[str, dict[str, str]] = {}
        self._conn_provider = PsycopgEnvConnectionProvider()

    def _maybe_canonize_rows(self, rows: list[dict], analysis_name: str, json_col: str) -> list[dict]:
        """
        If this is the n-alkanes analysis, rewrite per-entry 'data' keys using the synonyms map.
        """
        if analysis_name != "alkanes":
            return rows
        # load or cache synonyms map
        synmap = self._synmap_cache.get(analysis_name)
        if synmap is None:
            with self._conn_provider.get_connection() as conn:
                try:
                    synmap = _load_synonyms_map(conn)
                except psycopg2.Error:
                    synmap = {}
            self._synmap_cache[analysis_name] = synmap
        if not synmap:
            return rows

        out_rows: list[dict] = []
        for r in rows:
            r2 = dict(r)
            col = r2.get(json_col) or {}
            if isinstance(col, dict) and isinstance(col.get("entries"), list):
                col2 = dict(col)
                col2["entries"] = _canonize_entries(col["entries"], synmap)
                r2[json_col] = col2
            out_rows.append(r2)
        return out_rows

    def ingest(
        self,
        *,
        table: str,
        csv_path: str,
        json_col: str,
        conflict_cols: Sequence[str] = ("samplenumber",),
        commit: bool = True
    ) -> int:
        analysis_name = table.split(".", 1)[1] if "." in table else table
        analysis_name = normalize_analysis(analysis_name)
        # Parse filename metadata
        file_meta = parse_gc_filename(csv_path)
        rows = self.parser.build_rows(
            csv_path, json_col, analysis=analysis_name,
            instrument=file_meta.get("instrument"),
            data_type=file_meta.get("data_type"),
        )
        rows = self._maybe_canonize_rows(rows, analysis_name, json_col)
        return self.upserter.upsert(
            conn=None,  # let Upserter own the connection
            table=table,
            rows=rows,
            conflict_cols=conflict_cols,
            update_cols=[json_col],
            json_cols=[json_col],
            commit=commit,
        )

    def ingest_many(
        self,
        *,
        table: str,
        csv_paths: list[str],
        json_col: str,
        conflict_cols: Sequence[str] = ("samplenumber",),
        commit: bool = True
    ) -> int:
        analysis_name = table.split(".", 1)[1] if "." in table else table
        analysis_name = normalize_analysis(analysis_name)
        all_rows: list[dict] = []
        for p in csv_paths:
            # Parse filename metadata per CSV file
            file_meta = parse_gc_filename(p)
            rows = self.parser.build_rows(
                p, json_col, analysis=analysis_name,
                instrument=file_meta.get("instrument"),
                data_type=file_meta.get("data_type"),
            )
            rows = self._maybe_canonize_rows(rows, analysis_name, json_col)
            all_rows.extend(rows)
        return self.upserter.upsert(
            conn=None,
            table=table,
            rows=all_rows,
            conflict_cols=conflict_cols,
            update_cols=[json_col],
            json_cols=[json_col],
            commit=commit,
        )
