-- 0003_rls.sql — identity, ACL, and RLS policies

-- Subjects and groups
CREATE TABLE IF NOT EXISTS public.subjects (
    subject_id   TEXT PRIMARY KEY,
    subject_type TEXT NOT NULL CHECK (subject_type IN ('user','group'))
);

CREATE TABLE IF NOT EXISTS public.group_members (
    group_id TEXT NOT NULL REFERENCES public.subjects(subject_id),
    user_id  TEXT NOT NULL REFERENCES public.subjects(subject_id),
    PRIMARY KEY (group_id, user_id)
);

-- Per-sample ACL
CREATE TABLE IF NOT EXISTS public.sample_acl (
    samplenumber TEXT NOT NULL,
    subject_id   TEXT NOT NULL REFERENCES public.subjects(subject_id),
    can_read     BOOLEAN NOT NULL DEFAULT TRUE,
    can_write    BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (samplenumber, subject_id)
);
CREATE INDEX IF NOT EXISTS idx_sample_acl_subject ON public.sample_acl(subject_id);

-- Session identity helpers
CREATE OR REPLACE FUNCTION public.current_user_id() RETURNS TEXT
LANGUAGE sql STABLE AS $$ SELECT current_setting('app.user', true) $$;

CREATE OR REPLACE VIEW public.current_subjects AS
SELECT public.current_user_id() AS subject_id
UNION
SELECT gm.group_id
FROM public.group_members gm
WHERE gm.user_id = public.current_user_id();

-- Helper predicates used by app-side policy creation
CREATE OR REPLACE FUNCTION public.acl_can_read(sample TEXT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.sample_acl a
    JOIN public.current_subjects s ON s.subject_id = a.subject_id
    WHERE a.samplenumber = sample AND a.can_read
  );
$$;

CREATE OR REPLACE FUNCTION public.acl_can_write(sample TEXT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.sample_acl a
    JOIN public.current_subjects s ON s.subject_id = a.subject_id
    WHERE a.samplenumber = sample AND a.can_write
  );
$$;