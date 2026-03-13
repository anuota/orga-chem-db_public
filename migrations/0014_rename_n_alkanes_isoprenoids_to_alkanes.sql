-- 0014_rename_n_alkanes_isoprenoids_to_alkanes.sql
-- Rename the n_alkanes_isoprenoids table and JSONB column back to the shorter
-- canonical name "alkanes".  Drop the leftover original alkanes table (data was
-- merged by 0007) and rebuild affected views.

BEGIN;

-- 1. Drop views that depend on the table / column being renamed.
DROP VIEW IF EXISTS public.alkanes_entries          CASCADE;
DROP VIEW IF EXISTS public.n_alkanes_isoprenoids_entries CASCADE;

-- 2. Drop the legacy alkanes table left behind by 0007 (all data already
--    merged into n_alkanes_isoprenoids).
DROP TABLE IF EXISTS public.alkanes CASCADE;

-- 3. Rename the table.
ALTER TABLE public.n_alkanes_isoprenoids RENAME TO alkanes;

-- 4. Rename the JSONB column to match.
ALTER TABLE public.alkanes RENAME COLUMN n_alkanes_isoprenoids TO alkanes;

-- 5. Rebuild the canonical entries view.
CREATE VIEW public.alkanes_entries AS
SELECT
    b.samplenumber  AS samplenumber,
    e->>'name'        AS name,
    e->>'measured_by' AS measured_by,
    e->>'type'        AS type,
    e->>'date'        AS date,
    e->>'fraction'    AS fraction,
    e->>'instrument'  AS instrument,
    e->>'data_type'   AS data_type,
    e->>'notes'       AS notes,
    e->'data'         AS data
FROM public.alkanes b
CROSS JOIN LATERAL jsonb_array_elements(
    COALESCE(b.alkanes->'entries', '[]'::jsonb)
) AS e;

-- 6. Backward-compatible alias so old code referencing the long name still works.
CREATE VIEW public.n_alkanes_isoprenoids_entries AS
SELECT * FROM public.alkanes_entries;

COMMIT;
