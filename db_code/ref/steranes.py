from db_code.db_utils import ensure_table

DDL = """
CREATE TABLE IF NOT EXISTS public.ref_steranes (
    compound_id      SERIAL PRIMARY KEY,
    canonical_name   TEXT UNIQUE,
    trivial_name     TEXT,
    cas              TEXT,
    analysis_methods JSONB
);
CREATE TABLE IF NOT EXISTS public.ref_steranes_synonyms (
    synonym     TEXT PRIMARY KEY,
    compound_id INT NOT NULL REFERENCES public.ref_steranes(compound_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_ref_steranes_syn_compound
    ON public.ref_steranes_synonyms(compound_id);
"""
def ensure_ref_tables(conn, *, commit: bool = True) -> None:
    ensure_table(conn, DDL)
    if commit: conn.commit()