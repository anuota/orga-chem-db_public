
from __future__ import annotations
import logging
from functools import lru_cache

import psycopg2
from db_code.infra.db_conn import PsycopgEnvConnectionProvider
from .normalize import normalize_analysis

logger = logging.getLogger(__name__)

_ANA_N_ALKANES = {"n_alkanes_isoprenoids"}

@lru_cache(maxsize=1)
def _load_synonym_map() -> dict[str, tuple[int, str]]:
    """
    Returns map: normalized synonym -> (compound_id, canonical_name).
    """
    provider = PsycopgEnvConnectionProvider()
    conn = provider.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.synonym, a.canonical_name, s.compound_id
                FROM public.ref_n_alkane_synonyms s
                JOIN public.ref_n_alkanes a ON a.compound_id = s.compound_id
                """
            )
            out: dict[str, tuple[int, str]] = {}
            for syn, canon, cid in cur.fetchall():
                if syn:
                    out[str(syn).lower()] = (int(cid), canon)
            return out
    finally:
        conn.close()

def _norm_key(k: str) -> str:
    return (k or "").strip().replace("–", "-").replace("—", "-").lower()

def normalize_data_payload(analysis_key: str | None, data: dict[str, object]) -> dict:
    """
    If analysis is n_alkanes_isoprenoids, map headers via synonyms dictionary
    and store values as plain scalars under canonical keys. Otherwise, passthrough.
    """
    key = normalize_analysis(analysis_key or "")
    if key not in _ANA_N_ALKANES:
        return data
    synmap = _load_synonym_map()
    out: dict[str, object] = {}
    for k, v in data.items():
        nk = _norm_key(k)
        canon = synmap.get(nk, (None, None))[1] if nk in synmap else None
        if not canon:
            canon = k  # unknown header -> keep as-is
        target = canon
        i = 1
        while target in out:
            i = 1
            target = f"{canon}__dup{i}"
        out[target] = v
    return out