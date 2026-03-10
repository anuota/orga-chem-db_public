-- 0008_merge_whole_oil_and_add_name_metadata.sql
-- 1) Canonicalize whole-oil data into public.whole_oil
-- 2) Rebuild *_entries views with `name` metadata column
-- 3) Keep legacy alias views readable (alkanes_entries, wo_entries, whole_oil_gc_entries)

CREATE TABLE IF NOT EXISTS public.whole_oil (
    samplenumber TEXT PRIMARY KEY,
    whole_oil    JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_whole_oil_sn
    ON public.whole_oil(samplenumber);

DO $$
BEGIN
    IF to_regclass('public.whole_oil_gc') IS NOT NULL THEN
        INSERT INTO public.whole_oil (samplenumber, whole_oil)
        SELECT
            t.samplenumber,
            jsonb_build_object('entries', COALESCE(t.whole_oil_gc->'entries', '[]'::jsonb))
        FROM public.whole_oil_gc t
        ON CONFLICT (samplenumber)
        DO UPDATE SET
            whole_oil = jsonb_set(
                COALESCE(public.whole_oil.whole_oil, '{}'::jsonb),
                '{entries}',
                COALESCE(public.whole_oil.whole_oil->'entries', '[]'::jsonb)
                || COALESCE(EXCLUDED.whole_oil->'entries', '[]'::jsonb),
                true
            );
    END IF;

    IF to_regclass('public.wo') IS NOT NULL THEN
        INSERT INTO public.whole_oil (samplenumber, whole_oil)
        SELECT
            t.samplenumber,
            jsonb_build_object('entries', COALESCE(t.wo->'entries', '[]'::jsonb))
        FROM public.wo t
        ON CONFLICT (samplenumber)
        DO UPDATE SET
            whole_oil = jsonb_set(
                COALESCE(public.whole_oil.whole_oil, '{}'::jsonb),
                '{entries}',
                COALESCE(public.whole_oil.whole_oil->'entries', '[]'::jsonb)
                || COALESCE(EXCLUDED.whole_oil->'entries', '[]'::jsonb),
                true
            );
    END IF;
END
$$;

DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'hopanes',
        'steranes',
        'n_alkanes_isoprenoids',
        'naphthalenes',
        'phenanthrenes',
        'diamondoids',
        'terpanes',
        'thiophenes',
        'carbazoles',
        'alcohols',
        'fatty_acids',
        'ebfas',
        'etherlipids',
        'archaeolipids',
        'whole_oil'
    ]
    LOOP
        IF to_regclass('public.' || t) IS NOT NULL THEN
            EXECUTE format(
                $sql$
                DROP VIEW IF EXISTS public.%I_entries CASCADE;
                CREATE VIEW public.%I_entries AS
                SELECT
                    b.samplenumber AS samplenumber,
                    e->>'name'        AS name,
                    e->>'measured_by' AS measured_by,
                    e->>'type'        AS type,
                    e->>'date'        AS date,
                    e->>'fraction'    AS fraction,
                    e->>'instrument'  AS instrument,
                    e->>'data_type'   AS data_type,
                    e->>'notes'       AS notes,
                    e->'data'         AS data
                FROM public.%I b
                CROSS JOIN LATERAL jsonb_array_elements(COALESCE(b.%I->'entries', '[]'::jsonb)) AS e;
                $sql$,
                t, t, t, t
            );
        END IF;
    END LOOP;
END
$$;

-- Backward-compatible aliases
DROP VIEW IF EXISTS public.alkanes_entries;
CREATE VIEW public.alkanes_entries AS
SELECT *
FROM public.n_alkanes_isoprenoids_entries;

DROP VIEW IF EXISTS public.wo_entries;
CREATE VIEW public.wo_entries AS
SELECT *
FROM public.whole_oil_entries;

DROP VIEW IF EXISTS public.whole_oil_gc_entries;
CREATE VIEW public.whole_oil_gc_entries AS
SELECT *
FROM public.whole_oil_entries;
