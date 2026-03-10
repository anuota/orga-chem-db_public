-- canonical dictionary + synonyms for n-alkanes (and isoprenoids)
CREATE TABLE IF NOT EXISTS public.ref_n_alkanes (
    compound_id SERIAL PRIMARY KEY,
    canonical_name   TEXT NOT NULL,       -- e.g., IUPAC_name_eng
    gfz_short        TEXT UNIQUE NOT NULL,-- e.g., nC23, pristane
    trivial_name     TEXT,
    inchi            TEXT,
    cas              TEXT,
    fraction         TEXT,
    compound_type    TEXT,
    analysis_methods JSONB                -- << JSON array we can query
);

CREATE TABLE IF NOT EXISTS public.ref_n_alkane_synonyms (
    synonym    TEXT PRIMARY KEY,          -- normalized (lowercased, dash-normalized)
    compound_id INT NOT NULL REFERENCES public.ref_n_alkanes(compound_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ref_n_alkane_syn_compound ON public.ref_n_alkane_synonyms(compound_id);