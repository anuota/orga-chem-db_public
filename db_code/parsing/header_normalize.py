
from __future__ import annotations
import logging
import re
from functools import lru_cache

from .normalize import normalize_analysis

logger = logging.getLogger(__name__)

_ANA_N_ALKANES = {"alkanes"}

# ---------------------------------------------------------------------------
# Steranes – concentration CSV prefixes every column with "Ster-" (or "Ster")
# ---------------------------------------------------------------------------
_STERANE_PREFIX_RE = re.compile(r"^Ster-?", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Whole-oil – concentration CSV uses a different naming convention.
# Map concentration names → Area (canonical) names.
# None values mean "drop this column" (e.g. operator name in header).
# ---------------------------------------------------------------------------
_WO_CONC_TO_CANON: dict[str, str | None] = {
    # operator name leaked into header
    "Ahmad": None,
    # separator convention: comma-hyphen → period
    "2,2-DMB": "2.2DMB",
    "2,3-DMB": "2.3DMB",
    "2,2-DMP": "2.2DMP",
    "2,4-DMP": "2.4DMP",
    "2,2,3-TMB": "2.2.3TMB",
    "3,3-DMP": "3.3DMP",
    "2,3-DMP": "2.3DMP",
    "1,1-DMCP": "1.1DMCP",
    "2,5-DMHex": "2.5DMHex",
    "1,2,3-TMCP": "1.2.3TMCP",
    "2,3,4-TMCP": "2.3.4TMP",
    "1tans,2cis,4-TMCP": "1.2.4TMCP",
    # German ↔ English compound names
    "Benzene": "Benzol",
    "Toloene": "Tol",
    "EBenzene": "EBenz",
    "m+p-Xylene": "m/p Xylol",
    "1,2-Xylene": "o-Xylol",
    # abbreviation differences
    "1,cis,3-DMCP": "1C3DMCP",
    "1,trans,3-DMCP": "1T3DMCP",
    "1,trans,2-DMCP": "1T2DMCP",
    "ISD": "IS 2.2.4TMP",
    "2MHep": "2MHept",
    "3MHep": "3MHept",
    "3MOct": "3MO",
    "Pr.": "Pri",
    "Ph.": "Phy",
    "iC9": "C9so",
}

# ---------------------------------------------------------------------------
# N-alkanes – static fallback for concentration-specific column names
# that may not be in the DB synonym tables. Keys are _norm_key()-ed.
# ---------------------------------------------------------------------------
_ALKANE_CONC_FALLBACK: dict[str, str] = {
    "5aandrostane": "5a-Androstane",
    "nc17+pristan": "nC17+Pristane",
    "nc18*": "nC18",
    "phytan*": "Phytane",
}


@lru_cache(maxsize=1)
def _load_synonym_map() -> dict[str, tuple[int, str]]:
    """
    Returns map: normalized synonym -> (compound_id, canonical_name).
    DB connection is imported lazily to keep the parsing layer decoupled.
    """
    from db_code.infra.db_conn import PsycopgEnvConnectionProvider

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


def normalize_data_payload(
    analysis_key: str | None,
    data: dict[str, object],
    synonym_map: dict[str, tuple[int, str]] | None = None,
) -> dict:
    """
    Normalize CSV column headers to canonical compound parameter names.

    - steranes: strip ``Ster-`` prefix added by concentration CSVs.
    - whole_oil: map concentration naming convention to Area canonical names.
    - alkanes: DB synonym lookup + static fallback for
      concentration-specific variants.
    - all others: passthrough.

    Pass *synonym_map* from the service layer to avoid a DB call inside
    the parsing module. When omitted the map is loaded lazily from the DB.
    """
    key = normalize_analysis(analysis_key or "")

    if key == "steranes":
        return _normalize_steranes(data)

    if key == "whole_oil":
        return _normalize_whole_oil(data)

    if key in _ANA_N_ALKANES:
        return _normalize_n_alkanes(data, synonym_map=synonym_map)

    return data


# ---- per-analysis helpers --------------------------------------------------

def _normalize_steranes(data: dict[str, object]) -> dict[str, object]:
    out: dict[str, object] = {}
    for k, v in data.items():
        canonical = _STERANE_PREFIX_RE.sub("", k)
        if not canonical:
            canonical = k
        out.setdefault(canonical, v)
    return out


def _normalize_whole_oil(data: dict[str, object]) -> dict[str, object]:
    out: dict[str, object] = {}
    for k, v in data.items():
        if k in _WO_CONC_TO_CANON:
            canonical = _WO_CONC_TO_CANON[k]
            if canonical is None:
                continue  # drop (e.g. operator name)
            out.setdefault(canonical, v)
        else:
            out.setdefault(k, v)
    return out


def _normalize_n_alkanes(
    data: dict[str, object],
    synonym_map: dict[str, tuple[int, str]] | None = None,
) -> dict[str, object]:
    synmap = synonym_map if synonym_map is not None else _load_synonym_map()
    out: dict[str, object] = {}
    for k, v in data.items():
        nk = _norm_key(k)
        canon = synmap.get(nk, (None, None))[1] if nk in synmap else None
        if not canon:
            canon = _ALKANE_CONC_FALLBACK.get(nk)
        if not canon:
            canon = k  # unknown header → keep as-is
        out.setdefault(canon, v)
    return out