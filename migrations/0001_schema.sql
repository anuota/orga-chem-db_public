-- 0001_schema.sql — core tables for families

CREATE TABLE IF NOT EXISTS public.hopanes (
    samplenumber TEXT PRIMARY KEY,
    hopanes      JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS public.steranes (
    samplenumber TEXT PRIMARY KEY,
    steranes     JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS public.alkanes (
    samplenumber TEXT PRIMARY KEY,
    alkanes      JSONB NOT NULL
);

-- Helpful indexes (idempotent)
CREATE INDEX IF NOT EXISTS idx_hopanes_sn  ON public.hopanes(samplenumber);
CREATE INDEX IF NOT EXISTS idx_steranes_sn ON public.steranes(samplenumber);
CREATE INDEX IF NOT EXISTS idx_alkanes_sn  ON public.alkanes(samplenumber);