-- 0013_drop_redundant_pk_indexes.sql
-- Remove indexes on PRIMARY KEY columns — the PK constraint already creates
-- a unique B-tree index, so these are pure overhead.

DROP INDEX IF EXISTS public.idx_hopanes_sn;
DROP INDEX IF EXISTS public.idx_steranes_sn;
DROP INDEX IF EXISTS public.idx_alkanes_sn;
DROP INDEX IF EXISTS public.idx_n_alkanes_iso_sn;
DROP INDEX IF EXISTS public.idx_whole_oil_sn;
