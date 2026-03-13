-- 0012_ft_icr_ms_summary_only.sql
-- Truncate FT-ICR-MS raw signal data.
-- After this migration, re-run the FT ingest to populate summary-only entries.
--
-- The old data stored one JSON entry per signal (~800K entries total, ~35MB JSONB).
-- The new ingest stores one summary entry per CSV file (~435 entries, <1MB JSONB)
-- with peak_count, mass range, S/N range, and source_file reference.
-- Actual CSV files are served from the filesystem via /api/lab/ft-icr-ms/download.

TRUNCATE public.ft_icr_ms;
