from __future__ import annotations

import logging
import os

from db_code.infra.repository_psycopg import PsyRepo

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from api.shared import (
    ALLOWED_TABLES,
    canonical_table_name,
)
from api.routes.presence import router as presence_router
from api.routes.explorer import router as explorer_router
from api.routes.lab import router as lab_router

# -----------------
# App & DB helpers
# -----------------
app = FastAPI(title="OrgChem API", version="0.1.0")
app.include_router(presence_router)
app.include_router(explorer_router)
app.include_router(lab_router)


@app.get("/", include_in_schema=False)
def root_redirect():
    return RedirectResponse(url="/web/presence", status_code=307)

# CORS setup (optional, controlled by env)
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN")  # e.g., http://localhost:3000 for Next.js dev
if FRONTEND_ORIGIN:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[FRONTEND_ORIGIN],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"]
    )

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(levelname)s %(name)s: %(message)s",
)

repo = PsyRepo(
    host=os.getenv("PGHOST", "db"),
    port=os.getenv("PGPORT", "5432"),
    dbname=os.getenv("PGDATABASE", "postgres"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD"),
)

@app.get("/config/tables")
def list_allowed_tables():
    return sorted(list(ALLOWED_TABLES))



# -----------------
# Middleware: who is the user? (RLS)
# -----------------
@app.middleware("http")
async def capture_user(request: Request, call_next):
    # Prefer SSO headers from oauth2-proxy/ingress; fall back to DEV_USER
    request.state.user = request.headers.get("X-Email") or request.headers.get("X-User") or os.getenv("DEV_USER", "open")
    return await call_next(request)




# -----------------
# Health & DB info
# -----------------
@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/dbz")
def dbz(request: Request):
    ver = repo.with_rls_user(getattr(request.state, "user", "open")).server_version()
    return {"ok": True, "db_version": ver, "as_user": getattr(request.state, "user", "open")}


# -----------------
# Presence & search
# -----------------


@app.get("/samples/{samplenumber}/presence")
def sample_presence(samplenumber: str, request: Request):
    r = repo.with_rls_user(getattr(request.state, "user", "open")).get_presence(samplenumber)
    if not r:
        raise HTTPException(404, "Sample not found")
    return r


@app.get("/search/samples")
def search_samples(request: Request, q: str | None = None, limit: int = 50):
    return repo.with_rls_user(getattr(request.state, "user", "open")).search_samples(q or "", limit)

# -----------------
# Entries APIs
# -----------------
@app.get("/entries/{table}")
def entries_by_table(table: str, samplenumber: str, request: Request):
    table = canonical_table_name(table)
    if table not in ALLOWED_TABLES:
        raise HTTPException(400, f"Unknown table: {table}")
    return repo.with_rls_user(getattr(request.state, "user", "open")).list_entries(table, samplenumber)

