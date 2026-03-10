-- 0005_views_add_instrument_datatype.sql
-- Add instrument and data_type columns to all *_entries views.
-- These fields are stored inside each JSONB entry and may be NULL for
-- data ingested before this migration.

-- CREATE OR REPLACE VIEW cannot add columns in the middle of an existing
-- view (PG treats it as a column rename and rejects).  We must DROP first.

DROP VIEW IF EXISTS public.hopanes_entries;
CREATE VIEW public.hopanes_entries AS
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
FROM public.hopanes t
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.hopanes->'entries', '[]'::jsonb)) AS e;

DROP VIEW IF EXISTS public.steranes_entries;
CREATE VIEW public.steranes_entries AS
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
FROM public.steranes t
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.steranes->'entries', '[]'::jsonb)) AS e;

DROP VIEW IF EXISTS public.alkanes_entries;
CREATE VIEW public.alkanes_entries AS
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
FROM public.alkanes t
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.alkanes->'entries', '[]'::jsonb)) AS e;

-- Note: Additional tables' views are rebuilt dynamically by create_db_milti_table.py
-- during ingest (via make_entries_view_ddl). This migration covers the three tables
-- defined in 0002_views.sql. The ingest script will handle any others.
