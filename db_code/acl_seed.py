from __future__ import annotations

import logging
import os
import sys
from typing import Any

from db_code import db_users as acl
from db_code.db_utils import load_env
from db_code.infra.db_conn import PsycopgEnvConnectionProvider

if __name__ == "__main__" and __package__ is None:
    print(
        "[acl_seed] Tip: run as 'python -m db_code.acl_seed [config]' from the repo root."
    )

try:
    import tomllib  # Python 3.11+
except Exception:  # pragma: no cover
    tomllib = None

logger = logging.getLogger("acl_seed")
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(levelname)s %(name)s: %(message)s",
)

USAGE = """
Usage: python -m db_code.acl_seed [path/to/acl_seed.toml]

TOML schema:
[users]\nitems=["anya","brian"]\n
[groups]\nitems=["open","geochem"]\n
[memberships]\nopen=["anya"]\n
[projects]\nopen="Open data"\n
[project_acl.open]\nopen={read=true, write=false}\n
[sample_projects]\n# G003200=["open"]\n
[sample_acl.G003200]\n# anya={read=true, write=false}
"""


def _load_toml(path: str) -> dict[str, Any]:
    if tomllib is None:
        raise RuntimeError("Python 3.11+ required for tomllib (or vendor a parser)")
    with open(path, "rb") as f:
        return tomllib.load(f)


def main(argv: list[str]) -> int:
    # Resolve paths relative to repo root (parent of this file's directory)
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    default_env = os.environ.get("ENV_FILE", os.path.join(repo_root, ".env"))
    load_env(default_env)

    default_cfg = os.path.join(repo_root, "config", "acl_seed.toml")
    cfg_path = (
        argv[1] if len(argv) > 1 else os.environ.get("ACL_SEED_FILE", default_cfg)
    )
    if not os.path.exists(cfg_path):
        print("[acl_seed] config not found:\n" + USAGE)
        return 2

    cfg = _load_toml(cfg_path)

    provider = PsycopgEnvConnectionProvider()
    with provider.get_connection() as conn: 
        # Ensure identity + projects schema exists
        acl.ensure_identity_schema(conn)

        # Users
        for u in cfg.get("users", {}).get("items", []):
            acl.ensure_subject_user(conn, u)
            logger.info("user ensured: %s", u)

        # Groups
        for g in cfg.get("groups", {}).get("items", []):
            acl.ensure_subject_group(conn, g)
            logger.info("group ensured: %s", g)

        # Memberships
        for group, users in cfg.get("memberships", {}).items():
            if not isinstance(users, list):
                continue
            for u in users:
                acl.add_user_to_group(conn, user_id=u, group_id=group)
                logger.info("member added: %s -> %s", u, group)

        # Projects
        for pid, title in cfg.get("projects", {}).items():
            acl.ensure_project(
                conn, pid, title=str(title) if title is not None else None
            )
            logger.info("project ensured: %s (%s)", pid, title)

        # Project ACL
        for pid, subjects in cfg.get("project_acl", {}).items():
            if not isinstance(subjects, dict):
                continue
            for sid, rights in subjects.items():
                can_read = bool(rights.get("read", True))
                can_write = bool(rights.get("write", False))
                # If id listed in groups, treat as group; else assume user
                as_group = sid in set(cfg.get("groups", {}).get("items", []))
                acl.grant_project_access(
                    conn,
                    project_id=pid,
                    subject_id=sid,
                    as_group=as_group,
                    can_read=can_read,
                    can_write=can_write,
                )
                logger.info(
                    "project ACL: %s -> %s (r=%s w=%s)", sid, pid, can_read, can_write
                )

        # Sample projects (explicit). Usually you rely on auto-linker in create_db_milti_table.py
        for sn, projects in cfg.get("sample_projects", {}).items():
            if not isinstance(projects, list):
                continue
            for pid in projects:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO public.sample_projects(samplenumber, project_id)
                        VALUES (%s, %s)
                        ON CONFLICT (samplenumber, project_id) DO NOTHING
                        """,
                        (sn, pid),
                    )
                conn.commit()
                logger.info("sample_project: %s -> %s", sn, pid)

        # Sample ACL (explicit per-sample)
        for sn, subjects in cfg.get("sample_acl", {}).items():
            if not isinstance(subjects, dict):
                continue
            for sid, rights in subjects.items():
                can_read = bool(rights.get("read", True))
                can_write = bool(rights.get("write", False))
                as_group = sid in set(cfg.get("groups", {}).get("items", []))
                acl.grant_sample_access(
                    conn,
                    samplenumber=sn,
                    subject_id=sid,
                    as_group=as_group,
                    can_read=can_read,
                    can_write=can_write,
                )
                logger.info(
                    "sample ACL: %s -> %s (r=%s w=%s)", sid, sn, can_read, can_write
                )

    print("[acl_seed] done:", cfg_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
