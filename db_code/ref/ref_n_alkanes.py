#  /dev/null
#  a/db_code/ref_n_alkanes.py

from __future__ import annotations
import csv
import json
import logging
import os
from dataclasses import dataclass
from typing import Iterable, Tuple

from db_code.db_utils import ensure_table, get_connection

logger = logging.getLogger(__name__)

# ---------- DDL ----------

REF_N_ALKANES_DDL = """
CREATE TABLE IF NOT EXISTS public.ref_n_alkanes (
    compound_id      SERIAL PRIMARY KEY,
    canonical_name   TEXT NOT NULL,
    gfz_short        TEXT UNIQUE NOT NULL,
    trivial_name     TEXT,
    inchi            TEXT,
    cas              TEXT,
    fraction         TEXT,
    compound_type    TEXT,
    analysis_methods JSONB
);
"""

REF_N_ALKANE_SYNONYMS_DDL = """
CREATE TABLE IF NOT EXISTS public.ref_n_alkane_synonyms (
    synonym     TEXT PRIMARY KEY,
    compound_id INT NOT NULL REFERENCES public.ref_n_alkanes(compound_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_ref_n_alkane_syn_compound
    ON public.ref_n_alkane_synonyms(compound_id);
"""

def ensure_ref_tables(conn, *, commit: bool = True) -> None:
    """Create dictionary tables if missing (idempotent)."""
    ensure_table(conn, REF_N_ALKANES_DDL)
    ensure_table(conn, REF_N_ALKANE_SYNONYMS_DDL)
    if commit:
        conn.commit()


# ---------- CSV seeding ----------

@dataclass
class _RefRow:
    canonical_name: str
    gfz_short: str
    trivial_name: str | None
    inchi: str | None
    cas: str | None
    fraction: str | None
    compound_type: str | None
    analysis_methods: list[str] | None

def _split_methods(s: str | None) -> list[str] | None:
    if s is None:
        return None
    raw = str(s).strip()
    if not raw:
        return None
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]

def _read_sp_csv(path: str) -> list[_RefRow]:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    rows: list[_RefRow] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        # Headers expected: '#', IUPAC_name_eng, Trivial_name, GFZ_short, InChi, CAS, Fraction, Compound_Type, Analysis
        for r in reader:
            canon = (r.get("IUPAC_name_eng") or "").strip()
            gfz = (r.get("GFZ_short") or "").strip()
            if not canon or not gfz:
                continue
            rows.append(
                _RefRow(
                    canonical_name=canon,
                    gfz_short=gfz,
                    trivial_name=(r.get("Trivial_name") or "").strip() or None,
                    inchi=(r.get("InChi") or "").strip() or None,
                    cas=(r.get("CAS") or "").strip() or None,
                    fraction=(r.get("Fraction") or "").strip() or None,
                    compound_type=(r.get("Compound_Type") or "").strip() or None,
                    analysis_methods=_split_methods(r.get("Analysis")),
                )
            )
    return rows

def _norm_synonym(s: str | None) -> str | None:
    if s is None:
        return None
    t = s.strip()
    if not t:
        return None
    t = t.replace("–", "-").replace("—", "-")
    t = " ".join(t.split())
    return t.lower()

def _read_diff_names_csv(path: str) -> list[Tuple[str, str]]:
    """Return (column_header, synonym_value) pairs for each non-empty cell."""
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    pairs: list[Tuple[str, str]] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.reader(f)
        rows = [[(c or "").strip() for c in row] for row in rdr]
    if not rows:
        return pairs
    headers = rows[0]
    for col_idx, head in enumerate(headers):
        if not head:
            continue
        for r in rows[1:]:
            if col_idx >= len(r):
                continue
            cell = (r[col_idx] or "").strip()
            if not cell:
                continue
            pairs.append((head, cell))
    return pairs

def _upsert_ref(conn, items: Iterable[_RefRow]) -> dict[str, int]:
    """Upsert into ref_n_alkanes and return {gfz_short -> compound_id}."""
    mapping: dict[str, int] = {}
    with conn.cursor() as cur:
        for it in items:
            cur.execute(
                """
                INSERT INTO public.ref_n_alkanes
                  (canonical_name, gfz_short, trivial_name, inchi, cas, fraction, compound_type, analysis_methods)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (gfz_short)
                DO UPDATE SET
                  canonical_name = EXCLUDED.canonical_name,
                  trivial_name   = COALESCE(EXCLUDED.trivial_name, public.ref_n_alkanes.trivial_name),
                  inchi          = COALESCE(EXCLUDED.inchi, public.ref_n_alkanes.inchi),
                  cas            = COALESCE(EXCLUDED.cas, public.ref_n_alkanes.cas),
                  fraction       = COALESCE(EXCLUDED.fraction, public.ref_n_alkanes.fraction),
                  compound_type  = COALESCE(EXCLUDED.compound_type, public.ref_n_alkanes.compound_type),
                  analysis_methods = COALESCE(EXCLUDED.analysis_methods, public.ref_n_alkanes.analysis_methods)
                RETURNING compound_id, gfz_short
                """,
                (
                    it.canonical_name,
                    it.gfz_short,
                    it.trivial_name,
                    it.inchi,
                    it.cas,
                    it.fraction,
                    it.compound_type,
                    json.dumps(it.analysis_methods) if it.analysis_methods is not None else None,
                ),
            )
            cid, short = cur.fetchone()
            mapping[short] = cid
    conn.commit()
    return mapping

def _upsert_synonyms(conn, gfz_to_id: dict[str, int], pairs: Iterable[Tuple[str, str]], sp_rows: list[_RefRow]) -> int:
    """
    Insert synonyms. Registers:
            - Every value from `n_alkanes_different names_new.csv` under its column header
      - The header itself
      - The canonical_name and trivial_name as synonyms too
    """
    gfz_to_canon: dict[str, set[str]] = {r.gfz_short: {r.canonical_name} for r in sp_rows}
    for r in sp_rows:
        if r.trivial_name:
            gfz_to_canon.setdefault(r.gfz_short, set()).add(r.trivial_name)

    inserted = 0
    with conn.cursor() as cur:
        # Pairs from the “different names” sheet
        for head, val in pairs:
            gfz = head.strip()
            syn = _norm_synonym(val)
            if not syn:
                continue
            cid = gfz_to_id.get(gfz)
            if cid is None:
                for k, v in gfz_to_id.items():
                    if k.lower() == gfz.lower():
                        cid = v
                        break
            if cid is None:
                logger.warning("No matching GFZ_short in ref for synonyms column '%s'", gfz)
                continue
            cur.execute(
                """
                INSERT INTO public.ref_n_alkane_synonyms(synonym, compound_id)
                VALUES (%s, %s)
                ON CONFLICT (synonym) DO UPDATE SET compound_id = EXCLUDED.compound_id
                """,
                (syn, cid),
            )
            inserted = int(cur.rowcount > 0)

        # Also store the headers themselves and canonical/trivial forms as synonyms
        for gfz, cid in gfz_to_id.items():
            to_add = {gfz, *gfz_to_canon.get(gfz, set())}
            for s in to_add:
                syn = _norm_synonym(s)
                if not syn:
                    continue
                cur.execute(
                    """
                    INSERT INTO public.ref_n_alkane_synonyms(synonym, compound_id)
                    VALUES (%s, %s)
                    ON CONFLICT (synonym) DO UPDATE SET compound_id = EXCLUDED.compound_id
                    """,
                    (syn, cid),
                )
    conn.commit()
    return inserted

# Public API ---------------------------------------------------------------

def seed_from_csvs(*, sp_csv: str, diff_names_csv: str) -> tuple[int, int]:
    """Seed ref tables from the two CSVs. Returns (ref_rows_upserted, synonym_rows_upserted)."""
    rows = _read_sp_csv(sp_csv)
    with get_connection() as conn:
        ensure_ref_tables(conn, commit=False)
        gfz_to_id = _upsert_ref(conn, rows)
        pairs = _read_diff_names_csv(diff_names_csv)
        syn_count = _upsert_synonyms(conn, gfz_to_id, pairs, rows)
    return (len(gfz_to_id), syn_count)