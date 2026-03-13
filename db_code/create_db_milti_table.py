import os
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    # Script entry: extend sys.path to import db_code.* when run as a script
    sys.path.append(str(Path(__file__).resolve().parents[1]))
import logging
from glob import glob

from db_code import db_users as acl
from db_code.db_utils import ensure_table, get_connection, load_env
from db_code.ddl.tables import make_family_table_ddl
from db_code.ddl.views import (make_entries_view_ddl, make_presence_view_ddl,
                               make_presence_view_with_links)
from db_code.parsing.csv_multiheader import collect_data_keys_from_csv
from db_code.parsing.normalize import normalize_analysis, derive_table_from_filename
from db_code.services.ingest import TableIngestor

from db_code.ref.ref_n_alkanes import ensure_ref_tables as ensure_ref_n_alkanes, seed_from_csvs
from db_code.ref import (
    ensure_ref_steranes,
    ensure_ref_hopanes,
    ensure_ref_fatty_acids,
    ensure_ref_terpanes,
    ensure_ref_phenanthrenes,
)

level_name = os.getenv("LOG_LEVEL", "INFO").upper()
level = getattr(logging, level_name, logging.INFO)
logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")

logger = logging.getLogger(__name__)


def reset_public_schema(conn) -> None:
    """Drop and recreate public schema (DANGEROUS: wipes all tables/views).
    Use when you want a clean re-ingest with current normalization rules.
    """
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    conn.commit()


def drop_view(conn, view_name: str) -> None:
    """Drop public.<view_name> if it exists so we can recreate with new columns."""
    with conn.cursor() as cur:
        cur.execute(f"DROP VIEW IF EXISTS public.{view_name} CASCADE;")
    conn.commit()


# --- 0) Environment selection ---
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = os.environ.get("ENV_FILE", str(PROJECT_ROOT / ".env"))
load_env(ENV_PATH)

# Optional full reset flag: export ORG_CHEM_RESET=1 or run with --reset
RESET = (
    os.environ.get("ORG_CHEM_RESET", "0") in {"1", "true", "True"}
    or "--reset" in sys.argv
)

# --- 1) Family registry (single source of truth) ---
# GC compound data lives under the shared data root (FT-DataForDatabase).
# ORG_CHEM_DATA_DIR overrides if set (backward compat / CI).
_FT_ROOT = os.environ.get(
    "ORG_CHEM_FT_ROOT",
    "/Users/anya/Coding/Database/FT-DataForDatabase",
)
DATA_DIR = os.environ.get(
    "ORG_CHEM_DATA_DIR",
    os.path.join(_FT_ROOT, "GC-DataForDatabase"),
)

# Keep these families available in schema/view layer even when ingesting only GC files.
REQUIRED_EMPTY_FAMILIES = [
    {"table": "ft_icr_ms", "json_col": "ft_icr_ms", "csvs": []},
    {"table": "isotope_co2_werte", "json_col": "isotope_co2_werte", "csvs": []},
    {"table": "isotope_hd_werte", "json_col": "isotope_hd_werte", "csvs": []},
]


def discover_families(data_dir: str) -> list[dict]:
    patterns = [
        os.path.join(data_dir, "*combined (Area).csv"),
        os.path.join(data_dir, "*combined.csv"),
        os.path.join(data_dir, "* (Area).csv"),
        os.path.join(data_dir, "* (Concentration).csv"),
        os.path.join(data_dir, "* (concentration).csv"),
    ]
    files: list[str] = []
    for p in patterns:
        files.extend(glob(p, recursive=True))

    groups: dict[str, dict] = {}
    for csv_path in sorted(set(files)):
        table = derive_table_from_filename(csv_path)
        if not table:
            continue
        g = groups.setdefault(table, {"table": table, "json_col": table, "csvs": []})
        g["csvs"].append(csv_path)

    return list(groups.values())


FAMILIES = discover_families(DATA_DIR)

# Fallback if nothing discovered (hand-maintained list)
if not FAMILIES:
    FAMILIES = [
        {
            "table": "hopanes",
            "json_col": "hopanes",
            "csvs": [os.path.join(DATA_DIR, "Hopanes_combined (Area).csv")],
        },
        {
            "table": "steranes",
            "json_col": "steranes",
            "csvs": [os.path.join(DATA_DIR, "Steranes_combined (Area).csv")],
        },
        {
            "table": "alcohols",
            "json_col": "alcohols",
            "csvs": [os.path.join(DATA_DIR, "Alcohols_combined (Area).csv")],
        },
        # {"table": "alkanes",   "json_col": "alkanes",   "csv": os.path.join(DATA_DIR, "N-Alkanes_combined (Area).csv")},
    ]


def _ensure_required_families(families: list[dict]) -> list[dict]:
    by_table: dict[str, dict] = {}
    order: list[str] = []

    for f in families:
        table = f.get("table")
        if not table:
            continue
        if table not in by_table:
            order.append(table)
            by_table[table] = {
                "table": table,
                "json_col": f.get("json_col", table),
                "csvs": list(f.get("csvs") or []),
            }
        else:
            by_table[table]["csvs"].extend(list(f.get("csvs") or []))

    for req in REQUIRED_EMPTY_FAMILIES:
        table = req["table"]
        if table not in by_table:
            by_table[table] = dict(req)
            order.append(table)

    return [by_table[t] for t in order]


FAMILIES = _ensure_required_families(FAMILIES)

logger.info("Discovered %d families", len(FAMILIES))
for f in FAMILIES:
    logger.info("  - %s <- %d files", f["table"], len(f.get("csvs", [])))
    for p in f.get("csvs", []):
        logger.info("      • %s", p)


# --- 2) Ensure schema: tables first, then views ---
with get_connection() as conn:
    # Optionally wipe the schema so no legacy, pre-normalized entries remain
    if RESET:
        logger.warning("RESET requested: dropping and recreating public schema…")
        reset_public_schema(conn)

    # 1) Ensure ALL analysis tables exist first (so dependent views won't fail)
    for f in FAMILIES:
        ddl = make_family_table_ddl(
            f["table"], f["json_col"]
        )  # PK(samplenumber) + JSONB
        ensure_table(conn, ddl)

    # 2) Presence view (booleans only) — rebuild from discovered tables
    # discover existing public tables having a samplenumber column
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.table_name
            FROM information_schema.tables t
            WHERE t.table_schema='public' AND t.table_type='BASE TABLE'
              AND NOT t.table_name LIKE 'pg_%' AND NOT t.table_name LIKE 'sql_%'
              AND t.table_name NOT IN (
                  'schema_migrations','subjects','group_members','sample_acl',
                  'projects','sample_projects','project_acl','alkanes','wo','whole_oil_gc'
              )
              AND EXISTS (
                  SELECT 1 FROM information_schema.columns c
                  WHERE c.table_schema='public' AND c.table_name=t.table_name AND c.column_name='samplenumber'
              )
            ORDER BY t.table_name
            """
        )
        existing_tables = [r[0] for r in cur.fetchall()]

    # union discovered families with existing tables to drive presence view
    table_names = sorted({f["table"] for f in FAMILIES} | set(existing_tables))

    # (re)create ONLY the simple presence view with booleans; no counts, no entries
    drop_view(conn, "analysis_presence_simple")
    presence = make_presence_view_ddl(
        view_name="analysis_presence_simple",
        tables=table_names,
    )
    ensure_table(conn, presence)

    # 3) Drop & recreate per-family entry views to keep them in sync
    for f in FAMILIES:
        view_name = f"{f['table']}_entries"
        drop_view(conn, view_name)
        v = make_entries_view_ddl(
            table=f["table"], json_col=f["json_col"], view_name=view_name
        )
        ensure_table(conn, v)



# ensure the ref tables exist
with get_connection() as conn:
    ensure_ref_n_alkanes(conn)
    # placeholders — safe and idempotent
    ensure_ref_steranes(conn)
    ensure_ref_hopanes(conn)
    ensure_ref_fatty_acids(conn)
    ensure_ref_terpanes(conn)
    ensure_ref_phenanthrenes(conn)

# seed from CSVs (idempotent) — try container-mounted /app/data first, then repo /data
try:
    candidates = [
        ("/app/data/columns/n_alkanes_SP.csv",
         "/app/data/column_names/n_alkanes_different names_new.csv"),
        (os.path.join(PROJECT_ROOT, "app", "data", "columns", "n_alkanes_SP.csv"),
         os.path.join(PROJECT_ROOT, "app", "data", "column_names", "n_alkanes_different names_new.csv")),
        (os.path.join(PROJECT_ROOT, "data", "columns", "n_alkanes_SP.csv"),
         os.path.join(PROJECT_ROOT, "data", "column_names", "n_alkanes_different names_new.csv")),
    ]
    sp_csv = diff_csv = None
    for sp_cand, diff_cand in candidates:
        if os.path.exists(sp_cand) and os.path.exists(diff_cand):
            sp_csv, diff_csv = sp_cand, diff_cand
            break
    if not sp_csv or not diff_csv:
        raise FileNotFoundError("n-alkane seed CSVs not found in any known location.")

    logger.info("Seeding ref_n_alkanes from: %s ; %s", sp_csv, diff_csv)
    n_ref, n_syn = seed_from_csvs(sp_csv=sp_csv, diff_names_csv=diff_csv)
    logger.info("Seeded ref_n_alkanes (%d) & ref_n_alkane_synonyms (%d)", n_ref, n_syn)
except Exception as e:
    logger.warning("Seeding ref_n_alkanes failed (continuing): %s", e)

# --- 3) Identity / ACL / Projects ---
with get_connection() as conn:
    # Ensure identity + ACL + projects schema exists
    acl.ensure_identity_schema(conn)
    # Enable/refresh RLS on all current data tables
    acl.ensure_rls_for_tables(conn, table_names)
    # Ensure default project exists (linking happens AFTER ingest)
    acl.ensure_project(conn, "open", title="Open data")
    # Ensure a default group and grant read to the project
    acl.ensure_subject_group(conn, "open")
    acl.grant_project_access(
        conn,
        project_id="open",
        subject_id="open",
        as_group=True,
        can_read=True,
        can_write=False,
    )
    logger.info("Project 'open' ensured and granted read access to group 'open'")


ingestor = TableIngestor()
for f in FAMILIES:
    table, json_col, paths = f["table"], f["json_col"], f.get("csvs", [])
    if not paths:
        continue

    # Header consistency scan across CSVs for the same analysis
    try:
        base_keys = collect_data_keys_from_csv(paths[0])
    except Exception as e:
        logger.warning("header scan failed for %s: %s", paths[0], e)
        base_keys = set()

    for p in paths[1:]:
        try:
            k = collect_data_keys_from_csv(p)
        except Exception as e:
            logger.warning("header scan failed for %s: %s", p, e)
            continue
        added = sorted(k - base_keys)
        missing = sorted(base_keys - k)
        if added:
            logger.warning(
                "[%s] new columns detected in %s: %s",
                table,
                os.path.basename(p),
                ", ".join(added),
            )
            base_keys |= set(added)
        if missing:
            logger.info(
                "[%s] file %s lacks columns present elsewhere: %s",
                table,
                os.path.basename(p),
                ", ".join(missing),
            )

    count = ingestor.ingest_many(
        table=f"public.{table}",
        csv_paths=paths,
        json_col=json_col,
    )
    print(f"Upserted {count} rows into public.{table} from {len(paths)} file(s).")

# --- 4) Link samples to project now that data is ingested ---
with get_connection() as conn:
    # Discover existing public tables that expose `samplenumber` and merge with discovered families
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.table_name
            FROM information_schema.tables t
            WHERE t.table_schema='public' AND t.table_type='BASE TABLE'
              AND NOT t.table_name LIKE 'pg_%' AND NOT t.table_name LIKE 'sql_%'
              AND t.table_name NOT IN (
                  'schema_migrations','subjects','group_members','sample_acl',
                  'projects','sample_projects','project_acl','alkanes','wo','whole_oil_gc'
              )
              AND EXISTS (
                  SELECT 1 FROM information_schema.columns c
                  WHERE c.table_schema='public' AND c.table_name=t.table_name AND c.column_name='samplenumber'
              )
            ORDER BY t.table_name
            """
        )
        existing_tables = [r[0] for r in cur.fetchall()]

    table_names = sorted({f["table"] for f in FAMILIES} | set(existing_tables))

    # Link all current sample keys from all data tables to the default project 'open'
    linked = acl.link_samples_to_project(conn, project_id="open", tables=table_names)
    logger.info("Linked %d sample keys to project 'open' (post-ingest)", linked)