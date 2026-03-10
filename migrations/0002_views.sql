-- 0002_views.sql — entries flattening + presence views
-- Ensure we can change view column sets by dropping old versions first
DROP VIEW IF EXISTS public.analysis_presence_simple CASCADE;
DROP VIEW IF EXISTS public.analysis_presence CASCADE;
DROP VIEW IF EXISTS public.hopanes_entries CASCADE;
DROP VIEW IF EXISTS public.steranes_entries CASCADE;
DROP VIEW IF EXISTS public.alkanes_entries CASCADE;

-- Per-family entries views (match make_entries_view_ddl in db_utils.py)
CREATE OR REPLACE VIEW public.hopanes_entries AS
SELECT
  t.samplenumber AS samplenumber,
  e->>'measured_by' AS measured_by,
  e->>'type'        AS type,
  e->>'date'        AS date,
  e->>'fraction'    AS fraction,
  e->>'notes'       AS notes,
  e->'data'         AS data
FROM public.hopanes t
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.hopanes->'entries', '[]'::jsonb)) AS e;

CREATE OR REPLACE VIEW public.steranes_entries AS
SELECT
  t.samplenumber AS samplenumber,
  e->>'measured_by' AS measured_by,
  e->>'type'        AS type,
  e->>'date'        AS date,
  e->>'fraction'    AS fraction,
  e->>'notes'       AS notes,
  e->'data'         AS data
FROM public.steranes t
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.steranes->'entries', '[]'::jsonb)) AS e;

CREATE OR REPLACE VIEW public.alkanes_entries AS
SELECT
  t.samplenumber AS samplenumber,
  e->>'measured_by' AS measured_by,
  e->>'type'        AS type,
  e->>'date'        AS date,
  e->>'fraction'    AS fraction,
  e->>'notes'       AS notes,
  e->'data'         AS data
FROM public.alkanes t
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.alkanes->'entries', '[]'::jsonb)) AS e;

-- Presence view (booleans)
CREATE OR REPLACE VIEW public.analysis_presence_simple AS
WITH sample_keys AS (
    SELECT samplenumber FROM public.hopanes
    UNION
    SELECT samplenumber FROM public.steranes
    UNION
    SELECT samplenumber FROM public.alkanes
)
SELECT sk.samplenumber,
       EXISTS (SELECT 1 FROM public.hopanes  t WHERE t.samplenumber = sk.samplenumber) AS has_hopanes,
       EXISTS (SELECT 1 FROM public.steranes t WHERE t.samplenumber = sk.samplenumber) AS has_steranes,
       EXISTS (SELECT 1 FROM public.alkanes  t WHERE t.samplenumber = sk.samplenumber) AS has_alkanes
FROM sample_keys sk
GROUP BY sk.samplenumber
ORDER BY sk.samplenumber;

-- Presence view with counts and entries
CREATE OR REPLACE VIEW public.analysis_presence AS
WITH sample_keys AS (
    SELECT samplenumber FROM public.hopanes
    UNION
    SELECT samplenumber FROM public.steranes
    UNION
    SELECT samplenumber FROM public.alkanes
)
SELECT sk.samplenumber,
       -- hopanes
       EXISTS (SELECT 1 FROM public.hopanes t WHERE t.samplenumber = sk.samplenumber) AS has_hopanes,
       (SELECT COALESCE(COUNT(*), 0) FROM public.hopanes t CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.hopanes->'entries','[]'::jsonb)) AS e WHERE t.samplenumber = sk.samplenumber) AS hopanes_count,
       (SELECT COALESCE(jsonb_agg(e), '[]'::jsonb) FROM public.hopanes t CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.hopanes->'entries','[]'::jsonb)) AS e WHERE t.samplenumber = sk.samplenumber) AS hopanes_entries,
       -- steranes
       EXISTS (SELECT 1 FROM public.steranes t WHERE t.samplenumber = sk.samplenumber) AS has_steranes,
       (SELECT COALESCE(COUNT(*), 0) FROM public.steranes t CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.steranes->'entries','[]'::jsonb)) AS e WHERE t.samplenumber = sk.samplenumber) AS steranes_count,
       (SELECT COALESCE(jsonb_agg(e), '[]'::jsonb) FROM public.steranes t CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.steranes->'entries','[]'::jsonb)) AS e WHERE t.samplenumber = sk.samplenumber) AS steranes_entries,
       -- alkanes
       EXISTS (SELECT 1 FROM public.alkanes t WHERE t.samplenumber = sk.samplenumber) AS has_alkanes,
       (SELECT COALESCE(COUNT(*), 0) FROM public.alkanes t CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.alkanes->'entries','[]'::jsonb)) AS e WHERE t.samplenumber = sk.samplenumber) AS alkanes_count,
       (SELECT COALESCE(jsonb_agg(e), '[]'::jsonb) FROM public.alkanes t CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.alkanes->'entries','[]'::jsonb)) AS e WHERE t.samplenumber = sk.samplenumber) AS alkanes_entries
FROM sample_keys sk
GROUP BY sk.samplenumber
ORDER BY sk.samplenumber;