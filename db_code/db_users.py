"""
Access control & Row-Level Security (RLS) helpers for per-sample authorization.

This module creates:
  - subjects(user/group), group_members, sample_acl
  - a stable function current_user_id() and a view current_subjects
  - RLS policies for your data tables (e.g., hopanes, steranes, alkanes)

Usage (one-time setup):
  from db_users import ensure_identity_schema, ensure_rls_for_tables
  from db_utils import get_connection

  with get_connection() as conn:
      ensure_identity_schema(conn)
      ensure_rls_for_tables(conn, ["hopanes", "steranes", "alkanes"])  # add any other tables

Grant access:
  from db_users import ensure_subject_user, ensure_subject_group, add_user_to_group, grant_sample_access
  with get_connection() as conn:
      ensure_subject_user(conn, "anna.mueller")
      ensure_subject_group(conn, "geochem")
      add_user_to_group(conn, user_id="anna.mueller", group_id="geochem")
      grant_sample_access(conn, samplenumber="G003200", subject_id="geochem", as_group=True, can_read=True, can_write=False)

At runtime (per request/transaction), set the acting user so RLS policies apply:
  from db_users import set_session_user
  with get_connection() as conn:
      with conn.cursor() as cur:
          set_session_user(cur, "anna.mueller")
          # now any SELECT will only see rows allowed by sample_acl

"""

from __future__ import annotations

from typing import Iterable

from psycopg2.extensions import cursor as PsyCursor

from .db_utils import ensure_table

# ---------- DDL builders ----------

SUBJECTS_DDL = """
CREATE TABLE IF NOT EXISTS public.subjects (
    subject_id   TEXT PRIMARY KEY,
    subject_type TEXT NOT NULL CHECK (subject_type IN ('user','group'))
);
"""

GROUP_MEMBERS_DDL = """
CREATE TABLE IF NOT EXISTS public.group_members (
    group_id TEXT NOT NULL REFERENCES public.subjects(subject_id),
    user_id  TEXT NOT NULL REFERENCES public.subjects(subject_id),
    PRIMARY KEY (group_id, user_id)
);
"""

SAMPLE_ACL_DDL = """
CREATE TABLE IF NOT EXISTS public.sample_acl (
    samplenumber TEXT NOT NULL,
    subject_id   TEXT NOT NULL REFERENCES public.subjects(subject_id),
    can_read     BOOLEAN NOT NULL DEFAULT TRUE,
    can_write    BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (samplenumber, subject_id)
);
CREATE INDEX IF NOT EXISTS idx_sample_acl_subject ON public.sample_acl(subject_id);
"""

CURRENT_USER_ID_FN = """
CREATE OR REPLACE FUNCTION public.current_user_id() RETURNS TEXT
LANGUAGE sql STABLE AS $$ SELECT current_setting('app.user', true) $$;
"""

CURRENT_SUBJECTS_VIEW = """
CREATE OR REPLACE VIEW public.current_subjects AS
SELECT public.current_user_id() AS subject_id
UNION
SELECT gm.group_id
FROM public.group_members gm
WHERE gm.user_id = public.current_user_id();
"""

# ---------- Projects DDL ----------
PROJECTS_DDL = """
CREATE TABLE IF NOT EXISTS public.projects (
    project_id TEXT PRIMARY KEY,
    title      TEXT
);
"""

SAMPLE_PROJECTS_DDL = """
CREATE TABLE IF NOT EXISTS public.sample_projects (
    samplenumber TEXT NOT NULL,
    project_id   TEXT NOT NULL REFERENCES public.projects(project_id),
    PRIMARY KEY (samplenumber, project_id)
);
CREATE INDEX IF NOT EXISTS idx_sample_projects_project ON public.sample_projects(project_id);
"""

PROJECT_ACL_DDL = """
CREATE TABLE IF NOT EXISTS public.project_acl (
    project_id TEXT NOT NULL REFERENCES public.projects(project_id),
    subject_id TEXT NOT NULL REFERENCES public.subjects(subject_id),
    can_read   BOOLEAN NOT NULL DEFAULT TRUE,
    can_write  BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (project_id, subject_id)
);
CREATE INDEX IF NOT EXISTS idx_project_acl_subject ON public.project_acl(subject_id);
"""

# ---------- Ensure core identity/ACL schema ----------


def ensure_identity_schema(conn, *, commit: bool = True) -> None:
    ensure_table(conn, SUBJECTS_DDL)
    ensure_table(conn, GROUP_MEMBERS_DDL)
    ensure_table(conn, SAMPLE_ACL_DDL)
    ensure_table(conn, CURRENT_USER_ID_FN)
    ensure_table(conn, CURRENT_SUBJECTS_VIEW)
    ensure_table(conn, PROJECTS_DDL)
    ensure_table(conn, SAMPLE_PROJECTS_DDL)
    ensure_table(conn, PROJECT_ACL_DDL)
    if commit:
        conn.commit()


# ---------- Helpers to manage users/groups/ACL ----------


def ensure_subject_user(conn, user_id: str) -> None:
    """Insert a subject of type 'user' if missing."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.subjects(subject_id, subject_type)
            VALUES (%s, 'user')
            ON CONFLICT (subject_id) DO UPDATE SET subject_type = EXCLUDED.subject_type
            """,
            (user_id,),
        )
    conn.commit()


def ensure_subject_group(conn, group_id: str) -> None:
    """Insert a subject of type 'group' if missing. Pass group_id without 'group:' prefix."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.subjects(subject_id, subject_type)
            VALUES (%s, 'group')
            ON CONFLICT (subject_id) DO NOTHING
            """,
            (group_id,),
        )
    conn.commit()


def add_user_to_group(conn, *, user_id: str, group_id: str) -> None:
    """Add membership (user -> group)."""
    ensure_subject_user(conn, user_id)
    ensure_subject_group(conn, group_id)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.group_members(group_id, user_id)
            VALUES (%s, %s)
            ON CONFLICT (group_id, user_id) DO NOTHING
            """,
            (group_id, user_id),
        )
    conn.commit()


def grant_sample_access(
    conn,
    *,
    samplenumber: str,
    subject_id: str,
    as_group: bool = False,
    can_read: bool = True,
    can_write: bool = False,
) -> None:
    """Grant access for a user/group to a specific sample."""
    if as_group:
        ensure_subject_group(conn, subject_id)
    else:
        ensure_subject_user(conn, subject_id)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.sample_acl(samplenumber, subject_id, can_read, can_write)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (samplenumber, subject_id)
            DO UPDATE SET can_read = EXCLUDED.can_read, can_write = EXCLUDED.can_write
            """,
            (samplenumber, subject_id, can_read, can_write),
        )
    conn.commit()


def revoke_sample_access(conn, *, samplenumber: str, subject_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM public.sample_acl WHERE samplenumber = %s AND subject_id = %s",
            (samplenumber, subject_id),
        )
    conn.commit()


# ---------- RLS policies ----------

RLS_POLICY_TEMPLATE = """
ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_policies WHERE schemaname = 'public' AND tablename = '{table}' AND policyname = '{table}_read'
    ) THEN
        EXECUTE 'DROP POLICY {table}_read ON public.{table}';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_policies WHERE schemaname = 'public' AND tablename = '{table}' AND policyname = '{table}_write'
    ) THEN
        EXECUTE 'DROP POLICY {table}_write ON public.{table}';
    END IF;
END$$;

CREATE POLICY {table}_read ON public.{table}
FOR SELECT USING (
  EXISTS (
    SELECT 1
    FROM public.sample_acl a
    JOIN public.current_subjects s ON s.subject_id = a.subject_id
    WHERE a.samplenumber = {table}.samplenumber
      AND a.can_read
  )
  OR EXISTS (
    SELECT 1
    FROM public.sample_projects sp
    JOIN public.project_acl pa ON pa.project_id = sp.project_id AND pa.can_read
    JOIN public.current_subjects s ON s.subject_id = pa.subject_id
    WHERE sp.samplenumber = {table}.samplenumber
  )
);

CREATE POLICY {table}_write ON public.{table}
FOR UPDATE USING (
  EXISTS (
    SELECT 1
    FROM public.sample_acl a
    JOIN public.current_subjects s ON s.subject_id = a.subject_id
    WHERE a.samplenumber = {table}.samplenumber
      AND a.can_write
  )
  OR EXISTS (
    SELECT 1
    FROM public.sample_projects sp
    JOIN public.project_acl pa ON pa.project_id = sp.project_id AND pa.can_write
    JOIN public.current_subjects s ON s.subject_id = pa.subject_id
    WHERE sp.samplenumber = {table}.samplenumber
  )
);
"""


def ensure_rls_for_table(conn, table: str) -> None:
    """Enable RLS and create read/write policies for a single table (idempotent-ish)."""
    ddl = RLS_POLICY_TEMPLATE.format(table=table)
    ensure_table(conn, ddl)


def ensure_rls_for_tables(conn, tables: Iterable[str]) -> None:
    for t in tables:
        ensure_rls_for_table(conn, t)


# ---------- Session helper ----------


def set_session_user(cur: PsyCursor, user_id: str) -> None:
    """Set acting user for the current transaction/connection so RLS policies apply."""
    cur.execute('SET LOCAL "app.user" = %s', (user_id,))


# ---------- Project helpers ----------


def ensure_project(conn, project_id: str, title: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.projects(project_id, title)
            VALUES (%s, %s)
            ON CONFLICT (project_id) DO UPDATE SET title = COALESCE(EXCLUDED.title, public.projects.title)
            """,
            (project_id, title),
        )
    conn.commit()


def grant_project_access(
    conn,
    *,
    project_id: str,
    subject_id: str,
    as_group: bool = False,
    can_read: bool = True,
    can_write: bool = False,
) -> None:
    if as_group:
        ensure_subject_group(conn, subject_id)
    else:
        ensure_subject_user(conn, subject_id)
    ensure_project(conn, project_id)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.project_acl(project_id, subject_id, can_read, can_write)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (project_id, subject_id)
            DO UPDATE SET can_read = EXCLUDED.can_read, can_write = EXCLUDED.can_write
            """,
            (project_id, subject_id, can_read, can_write),
        )
    conn.commit()


def link_samples_to_project(conn, *, project_id: str, tables: Iterable[str]) -> int:
    """Link all distinct samplenumbers present in the given tables to the project.
    Returns number of (samplenumber, project_id) rows inserted.
    """
    ensure_project(conn, project_id)
    union_sql = "\n    UNION\n    ".join(
        [f"SELECT samplenumber FROM public.{t}" for t in tables]
    )
    inserted = 0
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH u AS (
                {union_sql}
            )
            INSERT INTO public.sample_projects(samplenumber, project_id)
            SELECT DISTINCT samplenumber, %s FROM u
            ON CONFLICT (samplenumber, project_id) DO NOTHING
            RETURNING 1
            """,
            (project_id,),
        )
        rows = cur.fetchall()
        inserted = len(rows)
    conn.commit()
    return inserted
