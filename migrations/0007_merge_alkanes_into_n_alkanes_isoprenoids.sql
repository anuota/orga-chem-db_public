-- 0007_merge_alkanes_into_n_alkanes_isoprenoids.sql
-- Canonicalize alkanes data into n_alkanes_isoprenoids.
-- Legacy table/view names are kept readable via alkanes_entries alias view.

CREATE TABLE IF NOT EXISTS public.n_alkanes_isoprenoids (
    samplenumber           TEXT PRIMARY KEY,
    n_alkanes_isoprenoids  JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_n_alkanes_iso_sn
    ON public.n_alkanes_isoprenoids(samplenumber);

DO $$
BEGIN
    IF to_regclass('public.alkanes') IS NOT NULL THEN
        INSERT INTO public.n_alkanes_isoprenoids (samplenumber, n_alkanes_isoprenoids)
        SELECT
            a.samplenumber,
            jsonb_build_object('entries', COALESCE(a.alkanes->'entries', '[]'::jsonb))
        FROM public.alkanes a
        ON CONFLICT (samplenumber)
        DO UPDATE SET
            n_alkanes_isoprenoids = jsonb_set(
                COALESCE(public.n_alkanes_isoprenoids.n_alkanes_isoprenoids, '{}'::jsonb),
                '{entries}',
                COALESCE(public.n_alkanes_isoprenoids.n_alkanes_isoprenoids->'entries', '[]'::jsonb)
                || COALESCE(EXCLUDED.n_alkanes_isoprenoids->'entries', '[]'::jsonb),
                true
            );
    END IF;
END
$$;

-- Rebuild canonical entries view.
DROP VIEW IF EXISTS public.n_alkanes_isoprenoids_entries;
CREATE VIEW public.n_alkanes_isoprenoids_entries AS
SELECT
    t.samplenumber AS samplenumber,
    e->>'measured_by' AS measured_by,
    e->>'type'        AS type,
    e->>'date'        AS date,
    e->>'fraction'    AS fraction,
    e->>'instrument'  AS instrument,
    e->>'data_type'   AS data_type,
    e->>'notes'       AS notes,
    e->'data'         AS data
FROM public.n_alkanes_isoprenoids t
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.n_alkanes_isoprenoids->'entries', '[]'::jsonb)) AS e;

-- Keep legacy URL /web/matrix/alkanes and old consumers readable via alias view.
DROP VIEW IF EXISTS public.alkanes_entries;
CREATE VIEW public.alkanes_entries AS
SELECT *
FROM public.n_alkanes_isoprenoids_entries;
