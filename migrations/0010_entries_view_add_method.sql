-- 0010_entries_view_add_method.sql
-- Add explicit method column to lab entries views.

DROP VIEW IF EXISTS public.ft_icr_ms_entries;
CREATE VIEW public.ft_icr_ms_entries AS
SELECT
    t.samplenumber AS samplenumber,
    e->>'name'        AS name,
    e->>'measured_by' AS measured_by,
    e->>'type'        AS type,
    e->>'date'        AS date,
    e->>'fraction'    AS fraction,
    e->>'instrument'  AS instrument,
    e->>'data_type'   AS data_type,
    COALESCE(e->>'method', e->>'data_type') AS method,
    e->>'notes'       AS notes,
    e->'data'         AS data
FROM public.ft_icr_ms t
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.ft_icr_ms->'entries', '[]'::jsonb)) AS e;

DROP VIEW IF EXISTS public.isotope_co2_werte_entries;
CREATE VIEW public.isotope_co2_werte_entries AS
SELECT
    t.samplenumber AS samplenumber,
    e->>'name'        AS name,
    e->>'measured_by' AS measured_by,
    e->>'type'        AS type,
    e->>'date'        AS date,
    e->>'fraction'    AS fraction,
    e->>'instrument'  AS instrument,
    e->>'data_type'   AS data_type,
    COALESCE(e->>'method', e->>'data_type') AS method,
    e->>'notes'       AS notes,
    e->'data'         AS data
FROM public.isotope_co2_werte t
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.isotope_co2_werte->'entries', '[]'::jsonb)) AS e;

DROP VIEW IF EXISTS public.isotope_hd_werte_entries;
CREATE VIEW public.isotope_hd_werte_entries AS
SELECT
    t.samplenumber AS samplenumber,
    e->>'name'        AS name,
    e->>'measured_by' AS measured_by,
    e->>'type'        AS type,
    e->>'date'        AS date,
    e->>'fraction'    AS fraction,
    e->>'instrument'  AS instrument,
    e->>'data_type'   AS data_type,
    COALESCE(e->>'method', e->>'data_type') AS method,
    e->>'notes'       AS notes,
    e->'data'         AS data
FROM public.isotope_hd_werte t
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.isotope_hd_werte->'entries', '[]'::jsonb)) AS e;
