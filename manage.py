# ⸻ One file to rule them all: manage.py ⸻
# ##Examples:
# 	•	Local dev: python manage.py serve --reload
# 	•	Migrate: python manage.py migrate
# 	•	Fresh load: python manage.py ingest --reset
# 	•	Seed ACL: python manage.py seed
# 	•	Compose up: python manage.py compose-up --build
# 	•	Compose down: python manage.py compose-down --volumes
	# •	python manage.py migrate
	# •	python manage.py ingest --reset
	# •	python manage.py seed
	# •	python manage.py serve --reload
	# •	Compose
	# •	python manage.py compose-up --build
	# •	python manage.py compose-down

#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path
from typing import Optional

import typer

app = typer.Typer(help="Org-Chem DB management CLI")

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_ENV = REPO_ROOT / ".env"
DEFAULT_COMPOSE = REPO_ROOT / "compose" / "docker-compose.prod.yml"

def _env_from_file(env_file: Optional[str]) -> dict:
    """
    Load key=value lines from .env into a dict without importing any extra libs.
    (Docker/CI already exports these; this is for local convenience.)
    """
    env = dict(os.environ)
    if not env_file:
        return env
    p = Path(env_file)
    if not p.exists():
        raise SystemExit(f"[manage] .env not found: {p}")
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip()
        v = v.strip().strip("\"'")
        env[k] = v
    return env

# ---------- DB: migrations ----------
@app.command("migrate")
def migrate(
    env_file: Optional[str] = typer.Option(None, help="Path to .env (defaults to repo .env)"),
    migrations_dir: Optional[str] = typer.Option("migrations", help="Folder with *.sql"),
):
    """
    Apply migrations/*.sql in order using migrunner.py (idempotent).
    """
    env = _env_from_file(env_file or str(DEFAULT_ENV))
    env["MIGRATIONS_DIR"] = str(REPO_ROOT / migrations_dir)
    code = subprocess.call([sys.executable, str(REPO_ROOT / "migrunner.py")], env=env)
    raise SystemExit(code)

# ---------- DB: ingest ----------
@app.command("ingest")
def ingest(
    env_file: Optional[str] = typer.Option(None, help="Path to .env (defaults to repo .env)"),
    data_dir: Optional[str] = typer.Option(None, help="Override ORG_CHEM_DATA_DIR"),
    reset: bool = typer.Option(False, "--reset", help="Drop schema public and re-create before ingest"),
):
    """
    Run the full ingestion pipeline (tables -> views -> ACL -> link -> upsert).
    Uses db_code/create_db_milti_table.py.
    """
    env = _env_from_file(env_file or str(DEFAULT_ENV))
    if data_dir:
        env["ORG_CHEM_DATA_DIR"] = data_dir
    if reset:
        env["ORG_CHEM_RESET"] = "1"
    code = subprocess.call([sys.executable, str(REPO_ROOT / "db_code" / "create_db_milti_table.py")], env=env)
    raise SystemExit(code)


@app.command("ingest-fticrms")
def ingest_fticrms(
    env_file: Optional[str] = typer.Option(None, help="Path to .env (defaults to repo .env)"),
    root_dir: str = typer.Option(
        "/Users/anya/Coding/Database/FT-DataForDatabase",
        help="Root folder with FT-ICR-MS data",
    ),
    file: Optional[str] = typer.Option(None, help="Single FT Signallist CSV to ingest"),
    dry_run: bool = typer.Option(False, help="Discover files but do not write to DB"),
):
    """
    Ingest FT-ICR-MS Signallist CSVs (APPIpos/ESIneg/ESIpos).
    """
    env = _env_from_file(env_file or str(DEFAULT_ENV))
    cmd = [sys.executable, "-m", "db_code.services.special_ingest", "ft", "--root-dir", root_dir]
    if file:
        cmd += ["--file", file]
    if dry_run:
        cmd.append("--dry-run")
    code = subprocess.call(cmd, env=env, cwd=str(REPO_ROOT))
    raise SystemExit(code)


@app.command("ingest-isotope")
def ingest_isotope(
    env_file: Optional[str] = typer.Option(None, help="Path to .env (defaults to repo .env)"),
    root_dir: str = typer.Option(
        "/Users/anya/Coding/Database/FT-DataForDatabase",
        help="Root folder with Isotope Data",
    ),
    file: Optional[str] = typer.Option(None, help="Single isotope combined CSV to ingest"),
    kind: Optional[str] = typer.Option(
        None,
        help="Optional kind filter: co2_werte or hd_werte",
    ),
    dry_run: bool = typer.Option(False, help="Discover files but do not write to DB"),
):
    """
    Ingest isotope CSVs (CO2_Werte and HD_Werte).
    """
    env = _env_from_file(env_file or str(DEFAULT_ENV))
    cmd = [
        sys.executable,
        "-m",
        "db_code.services.special_ingest",
        "isotope",
        "--root-dir",
        root_dir,
    ]
    if file:
        cmd += ["--file", file]
    if kind:
        cmd += ["--kind", kind]
    if dry_run:
        cmd.append("--dry-run")
    code = subprocess.call(cmd, env=env, cwd=str(REPO_ROOT))
    raise SystemExit(code)


@app.command("ingest-labdata")
def ingest_labdata(
    env_file: Optional[str] = typer.Option(None, help="Path to .env (defaults to repo .env)"),
    root_dir: str = typer.Option(
        "/Users/anya/Coding/Database/FT-DataForDatabase",
        help="Root folder with FT-ICR-MS and Isotope Data",
    ),
    dry_run: bool = typer.Option(False, help="Discover files but do not write to DB"),
):
    """
    Ingest both FT-ICR-MS and Isotope datasets in one run.
    """
    env = _env_from_file(env_file or str(DEFAULT_ENV))
    cmd = [sys.executable, "-m", "db_code.services.special_ingest", "all", "--root-dir", root_dir]
    if dry_run:
        cmd.append("--dry-run")
    code = subprocess.call(cmd, env=env, cwd=str(REPO_ROOT))
    raise SystemExit(code)

# ---------- DB: seed ACL ----------
@app.command("seed")
def seed(
    env_file: Optional[str] = typer.Option(None, help="Path to .env (defaults to repo .env)"),
    config: Optional[str] = typer.Option(None, help="TOML config (defaults to config/acl_seed.toml)"),
):
    """
    Seed users, groups, projects and ACLs from TOML (db_code/acl_seed.py).
    """
    env = _env_from_file(env_file or str(DEFAULT_ENV))
    args = [sys.executable, "-m", "db_code.acl_seed"]
    if config:
        args.append(config)
    code = subprocess.call(args, env=env, cwd=str(REPO_ROOT))
    raise SystemExit(code)

# ---------- API: local serve (dev) ----------
@app.command("serve")
def serve(
    env_file: Optional[str] = typer.Option(None, help="Path to .env (defaults to repo .env)"),
    host: str = typer.Option("0.0.0.0", help="Bind host"),
    port: int = typer.Option(8000, help="Port"),
    reload: bool = typer.Option(False, help="Dev auto-reload"),
):
    """
    Start FastAPI locally (uvicorn) — useful for dev.
    """
    env = _env_from_file(env_file or str(DEFAULT_ENV))
    cmd = ["uvicorn", "api.main:app", "--host", host, "--port", str(port)]
    if reload:
        cmd.append("--reload")
    code = subprocess.call(cmd, env=env, cwd=str(REPO_ROOT))
    raise SystemExit(code)

# ---------- Compose helpers (ops) ----------
@app.command("compose-up")
def compose_up(
    env_file: Optional[str] = typer.Option(None, help="Path to .env (defaults to repo .env)"),
    compose_file: Optional[str] = typer.Option(None, help="Compose file (defaults to compose/docker-compose.prod.yml)"),
    build: bool = typer.Option(False, "--build", help="Build images before start"),
    detach: bool = typer.Option(True, "--detach/--no-detach", help="Run in background"),
):
    """
    Bring up the stack via docker compose.
    """
    env = _env_from_file(env_file or str(DEFAULT_ENV))
    compose = compose_file or str(DEFAULT_COMPOSE)
    cmd = ["docker", "compose", "--env-file", str(DEFAULT_ENV), "-f", compose]
    cmd += (["up", "-d"] if detach else ["up"])
    if build:
        cmd.insert(-1 if detach else -0, "--build")
    raise SystemExit(subprocess.call(cmd, env=env, cwd=str(REPO_ROOT)))

@app.command("compose-down")
def compose_down(
    env_file: Optional[str] = typer.Option(None, help="Path to .env"),
    compose_file: Optional[str] = typer.Option(None, help="Compose file"),
    volumes: bool = typer.Option(False, "--volumes", help="Also remove volumes"),
):
    """
    Stop and remove the stack via docker compose.
    """
    env = _env_from_file(env_file or str(DEFAULT_ENV))
    compose = compose_file or str(DEFAULT_COMPOSE)
    cmd = ["docker", "compose", "--env-file", str(DEFAULT_ENV), "-f", compose, "down"]
    if volumes:
        cmd.append("-v")
    raise SystemExit(subprocess.call(cmd, env=env, cwd=str(REPO_ROOT)))

if __name__ == "__main__":
    app()