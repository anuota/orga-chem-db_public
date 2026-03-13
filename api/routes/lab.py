from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse

from api.compound_info import compound_index as _compound_index
from api.shared import (
    ALLOWED_TABLES,
    LAB_METHODS,
    FT_MODE_LABELS,
    FT_MODE_TO_VIRTUAL,
    MATRIX_META_FIELDS,
    MATRIX_META_LABELS,
    canonical_ft_mode,
    canonical_table_name,
    column_display_name,
    csv_column_order,
    method_label,
    run_query_with_rls,
)

router = APIRouter()

def _method_stats(method: str, request: Request) -> dict[str, Any]:
    method = canonical_table_name(method)
    view_name = f"public.{method}_entries"
    matrix_url = f"/web/matrix/{method}"
    entries_api_url = f"/api/lab/{method}/entries"
    if method == "ft_icr_ms":
        matrix_url = "/web/labdata/ft-icr-ms"
        entries_api_url = "/api/lab/ft-icr-ms/measurements"
    stats = {
        "method": method,
        "label": method_label(method),
        "samples": 0,
        "entries": 0,
        "matrix_url": matrix_url,
        "entries_api_url": entries_api_url,
    }
    try:
        _, rows = run_query_with_rls(
            (
                "SELECT COUNT(DISTINCT samplenumber) AS samples, "
                "COUNT(*) AS entries "
                f"FROM {view_name}"
            ),
            request,
        )
        if rows:
            stats["samples"] = int(rows[0].get("samples") or 0)
            stats["entries"] = int(rows[0].get("entries") or 0)
    except Exception:
        pass
    return stats


_canonical_ft_mode = canonical_ft_mode  # backward-compat alias for tests


def _ft_root() -> Path:
    return Path(os.getenv("ORG_CHEM_FT_ROOT", "/ftdata"))


@lru_cache(maxsize=4)
def _ft_file_index(root_dir: str) -> dict[str, Any]:
    root = Path(root_dir)
    signallist_by_name: dict[str, str] = {}
    masslists_by_key: dict[tuple[str, str], list[str]] = {}

    if not root.exists():
        return {
            "root": str(root),
            "signallist_by_name": signallist_by_name,
            "masslists_by_key": masslists_by_key,
        }

    for p in root.rglob("Signallist_*.csv"):
        try:
            rel = str(p.relative_to(root))
            signallist_by_name[p.name.lower()] = rel
        except Exception:
            continue

    masslist_rx = re.compile(
        r"^(G\d{6}[A-Za-z]*)_(APPIpos|ESIneg|ESIpos)_MassList_S-N-.*\.csv$",
        flags=re.IGNORECASE,
    )
    for p in root.rglob("*_MassList_S-N-*.csv"):
        m = masslist_rx.match(p.name)
        if not m:
            continue
        sample_code = m.group(1).lower()
        mode = _canonical_ft_mode(m.group(2)) or m.group(2)
        key = (sample_code, mode)
        rel = str(p.relative_to(root))
        masslists_by_key.setdefault(key, []).append(rel)

    for k in masslists_by_key:
        masslists_by_key[k] = sorted(set(masslists_by_key[k]))

    return {
        "root": str(root),
        "signallist_by_name": signallist_by_name,
        "masslists_by_key": masslists_by_key,
    }


def _ft_measurement_code_from_notes(notes: str | None, fallback_sample: str) -> tuple[str, str | None]:
    if notes:
        m = re.match(r"^Signallist_(.+?)_(APPIpos|ESIneg|ESIpos)\.csv$", notes, flags=re.IGNORECASE)
        if m:
            return m.group(1), _canonical_ft_mode(m.group(2))
    return fallback_sample, None


def _safe_measurement_date(raw_date: Any, signallist_abs: Path | None) -> str | None:
    if raw_date not in (None, ""):
        return str(raw_date)
    if signallist_abs and signallist_abs.exists():
        try:
            ts = signallist_abs.stat().st_mtime
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            return None
    return None


def _ft_measurement_rows(
    request: Request,
    limit: int,
    samplenumber: str | None = None,
    method: str | None = None,
) -> list[dict[str, Any]]:
    lim = max(1, min(int(limit), 20000))
    conditions: list[str] = []
    params: list = []
    if samplenumber:
        if not re.match(r"^[A-Za-z0-9_.:-]+$", samplenumber):
            raise HTTPException(400, "Invalid samplenumber format")
        conditions.append("samplenumber = %s")
        params.append(samplenumber)
    if method:
        canon = _canonical_ft_mode(method)
        if canon not in {"APPIpos", "ESIneg", "ESIpos"}:
            raise HTTPException(400, "Invalid FT-ICR-MS method")
        conditions.append("COALESCE(NULLIF(method, ''), NULLIF(data_type, '')) = %s")
        params.append(canon)
    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    sql_text = f"""
        SELECT
            samplenumber,
            COALESCE(NULLIF(name, ''), NULLIF(measured_by, '')) AS operator,
            date AS measurement_date,
            COALESCE(NULLIF(method, ''), NULLIF(data_type, '')) AS method,
            notes,
            CASE
                WHEN (data->>'peak_count') ~ '^[0-9]+$'
                THEN (data->>'peak_count')::int
                ELSE 0
            END AS peak_count,
            CASE
                WHEN (data->>'min_signal_to_noise') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                THEN (data->>'min_signal_to_noise')::double precision
                ELSE NULL
            END AS min_signal_to_noise
        FROM public.ft_icr_ms_entries
        {where}
        ORDER BY samplenumber, method, notes
        LIMIT %s
    """
    params.append(lim)
    _, rows = run_query_with_rls(sql_text, request, params)

    idx = _ft_file_index(str(_ft_root()))
    root = Path(idx["root"])
    signallist_by_name: dict[str, str] = idx["signallist_by_name"]
    masslists_by_key: dict[tuple[str, str], list[str]] = idx["masslists_by_key"]

    out: list[dict[str, Any]] = []
    for r in rows:
        notes = (r.get("notes") or "").strip()
        sample = str(r.get("samplenumber") or "")
        method = _canonical_ft_mode(r.get("method") or r.get("data_type"))

        signallist_rel = signallist_by_name.get(notes.lower()) if notes else None
        signallist_abs = (root / signallist_rel) if signallist_rel else None

        measurement_code, method_from_note = _ft_measurement_code_from_notes(notes, sample)
        if not method:
            method = method_from_note

        masslist_rels: list[str] = []
        if measurement_code and method:
            masslist_rels = masslists_by_key.get((measurement_code.lower(), method), [])

        signallist_url = (
            f"/api/lab/ft-icr-ms/download?relpath={quote_plus(signallist_rel)}"
            if signallist_rel
            else None
        )
        masslist_urls = [
            f"/api/lab/ft-icr-ms/download?relpath={quote_plus(rel)}"
            for rel in masslist_rels
        ]

        out.append(
            {
                "samplenumber": sample,
                "operator": r.get("operator") or "",
                "measurement_date": _safe_measurement_date(r.get("measurement_date"), signallist_abs),
                "method": method or "",
                "peak_count": int(r.get("peak_count") or 0),
                "min_signal_to_noise": r.get("min_signal_to_noise"),
                "signallist_file": notes or None,
                "signallist_download_url": signallist_url,
                "masslist_files": [Path(rel).name for rel in masslist_rels],
                "masslist_download_urls": masslist_urls,
            }
        )
    return out


@router.get("/api/lab/overview")
def lab_overview(request: Request):
    methods = [_method_stats(m, request) for m in sorted(LAB_METHODS)]
    return {"methods": methods}


@router.get("/api/lab/{method}/entries")
def lab_method_entries(
    method: str,
    request: Request,
    samplenumber: str | None = None,
    limit: int = 200,
):
    table = canonical_table_name(method)
    if table not in LAB_METHODS:
        raise HTTPException(400, f"Unknown lab method: {method}")

    lim = max(1, min(int(limit), 5000))
    if table == "ft_icr_ms":
        # Keep legacy endpoint path but return the FT summary model only.
        return lab_ft_icr_ms_measurements(request=request, samplenumber=samplenumber, limit=lim)

    where = ""
    params: list = []
    if samplenumber:
        if not re.match(r"^[A-Za-z0-9_.:-]+$", samplenumber):
            raise HTTPException(400, "Invalid samplenumber format")
        where = " WHERE samplenumber = %s"
        params.append(samplenumber)

    sql_text = (
        "SELECT samplenumber, name, measured_by, type, date, fraction, instrument, data_type, data "
        f"FROM public.{table}_entries"
        f"{where} "
        "ORDER BY samplenumber "
        "LIMIT %s"
    )
    params.append(lim)
    cols, rows = run_query_with_rls(sql_text, request, params)
    return {"method": table, "label": method_label(table), "columns": cols, "rows": rows}


@router.get("/api/lab/ft-icr-ms")
def lab_ft_icr_ms_entries(request: Request, samplenumber: str | None = None, method: str | None = None, limit: int = 200):
    return lab_ft_icr_ms_measurements(request=request, samplenumber=samplenumber, method=method, limit=limit)


@router.get("/api/lab/ft-icr-ms/measurements")
def lab_ft_icr_ms_measurements(
    request: Request,
    samplenumber: str | None = None,
    method: str | None = None,
    limit: int = 5000,
):
    rows = _ft_measurement_rows(request, limit=limit, samplenumber=samplenumber, method=method)
    label = "FT-ICR-MS"
    if method:
        canon = _canonical_ft_mode(method)
        if canon:
            label = f"FT-ICR-MS {canon}"
    return {
        "method": "ft_icr_ms",
        "label": label,
        "columns": [
            "samplenumber",
            "operator",
            "measurement_date",
            "method",
            "peak_count",
            "min_signal_to_noise",
            "signallist_file",
            "signallist_download_url",
            "masslist_files",
            "masslist_download_urls",
        ],
        "rows": rows,
    }


@router.get("/api/lab/ft_icr_ms/measurements", include_in_schema=False)
def lab_ft_icr_ms_measurements_alias(
    request: Request,
    samplenumber: str | None = None,
    method: str | None = None,
    limit: int = 5000,
):
    return lab_ft_icr_ms_measurements(request=request, samplenumber=samplenumber, method=method, limit=limit)


@router.get("/api/lab/ft-icr-ms/download")
def lab_ft_icr_ms_download(relpath: str):
    root = _ft_root().resolve()
    rel = Path(relpath)
    if rel.is_absolute() or ".." in rel.parts:
        raise HTTPException(400, "Invalid relpath")
    target = (root / rel).resolve()
    if not str(target).startswith(str(root)):
        raise HTTPException(400, "Invalid relpath")
    if not target.exists() or not target.is_file():
        raise HTTPException(404, "File not found")
    return FileResponse(path=str(target), filename=target.name, media_type="text/csv")


@router.get("/api/lab/ft_icr_ms/download", include_in_schema=False)
def lab_ft_icr_ms_download_alias(relpath: str):
    return lab_ft_icr_ms_download(relpath=relpath)


@router.get("/api/lab/isotope/co2")
def lab_isotope_co2_entries(request: Request, samplenumber: str | None = None, limit: int = 200):
    return lab_method_entries("isotope_co2_werte", request, samplenumber=samplenumber, limit=limit)


@router.get("/api/lab/isotope/hd")
def lab_isotope_hd_entries(request: Request, samplenumber: str | None = None, limit: int = 200):
    return lab_method_entries("isotope_hd_werte", request, samplenumber=samplenumber, limit=limit)


def presence(request: Request):
    cols, rows = run_query_with_rls("SELECT * FROM public.analysis_presence_simple", request)
    return {"columns": cols, "rows": rows}


@router.get("/api/presence")
def api_presence(request: Request):
    return presence(request)


def matrix_wide(table: str, request: Request):
    table = canonical_table_name(table)
    if table not in ALLOWED_TABLES:
        raise HTTPException(400, f"Unknown table: {table}")
    if table == "ft_icr_ms":
        return lab_ft_icr_ms_measurements(request=request, limit=5000)

    # Fetch (samplenumber, data JSON) from the flattened entries view if present, else from base table
    view_name = f"public.{table}_entries"
    cols, rows = run_query_with_rls(
        (
            "SELECT samplenumber, instrument, fraction, data_type, name, measured_by, date, data "
            f"FROM {view_name}"
        ),
        request,
    )
    # Build a wide matrix in Python: columns = samplenumber + sorted unique data keys
    # rows is list of dicts with keys from cols
    samples: dict[str, dict] = {}
    keys: set[str] = set()
    for r in rows:
        sn = r.get("samplenumber")
        data = r.get("data") or {}
        if not isinstance(data, dict):
            continue
        drow = samples.setdefault(sn, {})
        for mf in MATRIX_META_FIELDS:
            if mf not in drow or drow[mf] in (None, ""):
                drow[mf] = r.get(mf)
        for k, v in data.items():
            if isinstance(v, dict):
                v = v.get("value", v.get("orig", ""))
            # Prefer first non-null value per sample/parameter
            if k not in drow or drow[k] in (None, ""):
                drow[k] = v
            keys.add(k)
    ordered_keys = csv_column_order(table, keys)
    out_rows = []
    for sn, kv in samples.items():
        row = {"samplenumber": sn}
        for mf in MATRIX_META_FIELDS:
            row[mf] = kv.get(mf)
        for k in ordered_keys:
            row[k] = kv.get(k)
        out_rows.append(row)
    return {"columns": ["samplenumber"] + MATRIX_META_FIELDS + ordered_keys, "rows": out_rows}


@router.get("/api/matrix/{table}")
def api_matrix_wide(table: str, request: Request):
    return matrix_wide(table, request)

# -----------------
# HTML endpoints: matrix index and per-method matrix as HTML table
# -----------------


# --- Matrix index as HTML page listing all methods ---
@router.get("/web/matrix", response_class=HTMLResponse)
def matrix_index_html():
    """Simple index page listing all available methods with links to their wide HTML tables."""
    methods = sorted(list(ALLOWED_TABLES))
    title = "OrgChem – Methods matrix index"

    items = []
    for m in methods:
        label = method_label(m)
        url = f"/web/matrix/{m}"
        if m == "ft_icr_ms":
            url = "/web/labdata/ft-icr-ms"
        items.append(
            f'<li><a href="{escape(url)}">{escape(label)}</a></li>'
        )
    items_html = "\n".join(items) or "<li><em>No methods configured.</em></li>"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <title>{escape(title)}</title>
    <style>
        body {{
            font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
            margin: 0;
            padding: 1.5rem 2rem;
            background: #f5f6fa;
        }}
        .card {{
            background: #ffffff;
            border-radius: 8px;
            padding: 1rem 1.5rem;
            box-shadow: 0 1px 4px rgba(15, 23, 42, 0.06);
            max-width: 720px;
        }}
        h1 {{
            margin-top: 0;
            font-size: 1.4rem;
        }}
        ul {{
            padding-left: 1.2rem;
        }}
        a {{
            color: #2563eb;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
    </style>
</head>
<body>
  <div class="card">
    <h1>{escape(title)}</h1>
    <p>Select a method to view its sample × parameter matrix:</p>
    <ul>
      {items_html}
    </ul>
        <p>
            <a href="/web/presence">← Back to presence matrix</a>
            &nbsp;|&nbsp;
            <a href="/web/labdata">Lab data page</a>
        </p>
  </div>
</body>
</html>"""
    return HTMLResponse(content=html, status_code=200)


@router.get("/web/labdata", response_class=HTMLResponse)
def labdata_html(request: Request):
    """Landing page with dedicated links for FT-ICR-MS and isotope datasets."""
    methods = [_method_stats(m, request) for m in sorted(LAB_METHODS)]
    cards = []
    for m in methods:
        cards.append(
            """
            <div class="lab-card">
                <h3>{label}</h3>
                <p>Samples: <strong>{samples}</strong> · Entries: <strong>{entries}</strong></p>
                <p>
                    <a href="{matrix_url}" target="_blank" rel="noopener">Open Matrix</a>
                    &nbsp;|&nbsp;
                    <a href="{api_url}" target="_blank" rel="noopener">API Entries</a>
                </p>
            </div>
            """.format(
                label=escape(str(m["label"])),
                samples=escape(str(m["samples"])),
                entries=escape(str(m["entries"])),
                matrix_url=escape(str(m["matrix_url"])),
                api_url=escape(str(m["entries_api_url"])),
            )
        )

    cards_html = "\n".join(cards) if cards else "<p>No lab methods found.</p>"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <title>OrgChem – Lab Data</title>
    <style>
        body {{
            font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
            margin: 0;
            padding: 1.5rem 2rem;
            background: #f5f6fa;
        }}
        .card {{
            background: #ffffff;
            border-radius: 8px;
            padding: 1rem 1.5rem;
            box-shadow: 0 1px 4px rgba(15, 23, 42, 0.06);
            max-width: 960px;
        }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
            gap: 0.75rem;
        }}
        .lab-card {{
            border: 1px solid #e5e7eb;
            border-radius: 6px;
            padding: 0.75rem;
            background: #fbfdff;
        }}
        h1 {{ margin-top: 0; }}
        h3 {{ margin: 0 0 0.35rem 0; font-size: 1rem; }}
        p {{ margin: 0.2rem 0; font-size: 0.88rem; color: #334155; }}
        a {{ color: #2563eb; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
  <div class="card">
    <h1>OrgChem – Lab Data</h1>
    <p>
      Dedicated FT-ICR-MS and isotope entry points.
      <a href="/web/explorer">Open Data Explorer</a>
      &nbsp;|&nbsp;
      <a href="/web/matrix">Open Methods Index</a>
    </p>
    <div class="grid">
      {cards_html}
    </div>
  </div>
</body>
</html>"""
    return HTMLResponse(content=html, status_code=200)


@router.get("/web/matrix/ft-icr-ms", include_in_schema=False)
def matrix_ft_alias():
    return RedirectResponse(url="/web/labdata/ft-icr-ms", status_code=307)


@router.get("/web/matrix/isotope-co2", include_in_schema=False)
def matrix_iso_co2_alias():
    return RedirectResponse(url="/web/matrix/isotope_co2_werte", status_code=307)


@router.get("/web/matrix/isotope-hd", include_in_schema=False)
def matrix_iso_hd_alias():
    return RedirectResponse(url="/web/matrix/isotope_hd_werte", status_code=307)


@router.get("/web/labdata/ft-icr-ms", response_class=HTMLResponse)
def web_ft_icr_ms_measurements(request: Request, method: str | None = None):
    rows = _ft_measurement_rows(request, limit=20000, method=method)
    page_title = "FT-ICR-MS Measurement Summary"
    if method:
        canon = _canonical_ft_mode(method)
        if canon:
            page_title = f"FT-ICR-MS {canon} Measurement Summary"

    body_rows: list[str] = []
    for r in rows:
        mass_links = "<br/>".join(
            [
                f'<a href="{escape(url)}" target="_blank" rel="noopener">{escape(name)}</a>'
                for name, url in zip(r.get("masslist_files") or [], r.get("masslist_download_urls") or [])
            ]
        ) or ""
        signallist_link = ""
        if r.get("signallist_download_url") and r.get("signallist_file"):
            signallist_link = (
                f'<a href="{escape(str(r.get("signallist_download_url")))}" '
                f'target="_blank" rel="noopener">{escape(str(r.get("signallist_file")))}</a>'
            )

        body_rows.append(
            "<tr>"
            f"<td>{escape(str(r.get('samplenumber') or ''))}</td>"
            f"<td>{escape(str(r.get('operator') or ''))}</td>"
            f"<td>{escape(str(r.get('measurement_date') or ''))}</td>"
            f"<td>{escape(str(r.get('method') or ''))}</td>"
            f"<td>{escape(str(r.get('peak_count') or ''))}</td>"
            f"<td>{escape(str(r.get('min_signal_to_noise') or ''))}</td>"
            f"<td>{signallist_link}</td>"
            f"<td>{mass_links}</td>"
            "</tr>"
        )

    body_html = "\n".join(body_rows)
    html = f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\" />
    <title>OrgChem - {escape(page_title)}</title>
    <style>
        body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; padding: 1.5rem 2rem; background: #f5f6fa; }}
        .card {{ background: #fff; border-radius: 8px; padding: 1rem 1.25rem; box-shadow: 0 1px 4px rgba(15, 23, 42, 0.06); }}
        table {{ border-collapse: collapse; width: 100%; font-size: 0.86rem; }}
        th, td {{ border: 1px solid #e5e7eb; padding: 0.3rem 0.4rem; vertical-align: top; }}
        th {{ background: #f1f5f9; text-align: left; }}
        tr:nth-child(even) {{ background: #fafafa; }}
        a {{ color: #2563eb; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .links {{ margin-bottom: 0.6rem; font-size: 0.9rem; }}
    </style>
</head>
<body>
    <div class=\"card\">
        <h1>{escape(page_title)}</h1>
        <div class=\"links\">
            <a href=\"/web/labdata\">Lab data page</a>
            &nbsp;|&nbsp;
            <a href="/web/labdata/ft-icr-ms">All FT-ICR-MS</a>
            &nbsp;|&nbsp;
            <a href="/web/labdata/ft-icr-ms?method=APPIpos">APPIpos</a>
            &nbsp;|&nbsp;
            <a href="/web/labdata/ft-icr-ms?method=ESIneg">ESIneg</a>
            &nbsp;|&nbsp;
            <a href="/web/labdata/ft-icr-ms?method=ESIpos">ESIpos</a>
        </div>
        <table>
            <thead>
                <tr>
                    <th>Sample</th>
                    <th>Operator</th>
                    <th>Measurement Date</th>
                    <th>Method</th>
                    <th>Number of Peaks</th>
                    <th>Min Signal/Noise</th>
                    <th>Signallist CSV</th>
                    <th>MassList CSV(s)</th>
                </tr>
            </thead>
            <tbody>
                {body_html}
            </tbody>
        </table>
    </div>
</body>
</html>"""
    return HTMLResponse(content=html, status_code=200)

# --- Per-method matrix as HTML table ---
@router.get("/web/matrix/{table}", response_class=HTMLResponse)
def matrix_method_html(table: str, request: Request):
    """Render a wide sample × parameter matrix for a single method as an HTML table."""
    table = canonical_table_name(table)
    if table == "ft_icr_ms":
        return RedirectResponse(url="/web/labdata/ft-icr-ms", status_code=307)

    title_method = method_label(table)
    title = f"OrgChem – Matrix for {title_method}"

    # Try to restrict to known tables, but be tolerant: if not configured, still try.
    # This allows legacy tables like 'alkanes' to work even if not in ALLOWED_TABLES.
    table_ok = table in ALLOWED_TABLES

    view_name = f"public.{table}_entries"
    try:
        cols, rows = run_query_with_rls(
            (
                "SELECT samplenumber, instrument, fraction, data_type, name, measured_by, date, data "
                f"FROM {view_name} ORDER BY samplenumber"
            ),
            request,
        )
    except Exception as e:
        # If the view is missing, show a friendly error page.
        msg = escape(str(e))
        html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8" /><title>{escape(title)}</title></head>
<body style="font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; padding: 1.5rem 2rem;">
  <h1>{escape(title)}</h1>
  <p>Could not load data from <code>{escape(view_name)}</code>.</p>
  <pre>{msg}</pre>
  <p><a href="/web/matrix">← Back to method index</a></p>
</body>
</html>"""
        return HTMLResponse(content=html, status_code=500)

    # Collect parameter keys from data JSON
    keys = set()
    parsed_rows = []
    for r in rows:
        sn = r.get("samplenumber")
        data = r.get("data") or {}
        if isinstance(data, dict):
            d = dict(data)
        else:
            d = {}
        for k in d.keys():
            keys.add(k)
        meta = {mf: r.get(mf) for mf in MATRIX_META_FIELDS}
        parsed_rows.append({"samplenumber": sn, "meta": meta, "data": d})

    ordered_keys = csv_column_order(table, keys)
    rows_count = len(parsed_rows)
    columns_count = 1 + len(MATRIX_META_FIELDS) + len(ordered_keys)

    # Build table header
    header_cells = []
    # Sample column
    header_cells.append('<th class="sample-col">Sample</th>')
    # Metadata columns
    for mf in MATRIX_META_FIELDS:
        header_cells.append(f'<th class="meta-header">{escape(MATRIX_META_LABELS.get(mf, mf))}</th>')
    # Parameter columns – link to compound info page when a match exists
    _cidx = _compound_index()
    for k in ordered_keys:
        display = column_display_name(table, str(k))
        label_text = escape(display)
        raw_key = escape(str(k))
        # Show raw DB key on hover when display name differs
        title_attr = f' title="{raw_key}"' if display != str(k) else ""
        norm = str(k).strip().lower().replace(" ", "").replace("-", "")
        if norm in _cidx:
            href = f'/web/compounds/{escape(str(k), quote=True)}'
            label_text = f'<a href="{href}">{label_text}</a>'
        header_cells.append(
            f'<th class="param-header"{title_attr}>{label_text}</th>'
        )
    header_html = "".join(header_cells)

    # Build body
    body_rows = []
    for r in parsed_rows:
        sn = r.get("samplenumber", "")
        sn_txt = escape(str(sn))
        cells = []
        # sample id
        cells.append(f'<td class="sample-cell">{sn_txt}</td>')
        meta = r.get("meta") or {}
        for mf in MATRIX_META_FIELDS:
            mv = meta.get(mf)
            mt = "" if mv is None else escape(str(mv))
            cells.append(f'<td class="meta-cell">{mt}</td>')
        data = r.get("data") or {}
        if not isinstance(data, dict):
            data = {}
        for k in ordered_keys:
            v = data.get(k)
            if isinstance(v, dict):
                v = v.get("value", v.get("orig", ""))
            txt = "" if v is None else escape(str(v))
            cells.append(f'<td class="value-cell">{txt}</td>')
        body_rows.append(f'<tr>{"".join(cells)}</tr>')
    body_html = "\n".join(body_rows)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <title>{escape(title)}</title>
    <style>
        :root {{
            color-scheme: light;
        }}
        body {{
            font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
            margin: 0;
            padding: 1.5rem 2rem;
            background: #f5f6fa;
        }}
        .wrap {{
            display: flex;
            flex-direction: column;
            gap: 1rem;
        }}
        .card {{
            background: #ffffff;
            border-radius: 8px;
            padding: 1rem 1.5rem;
            box-shadow: 0 1px 4px rgba(15, 23, 42, 0.06);
        }}
        h1 {{
            margin-top: 0;
            font-size: 1.4rem;
        }}
        .meta {{
            font-size: 0.85rem;
            color: #6b7280;
            margin-bottom: 0.5rem;
            display: flex;
            align-items: center;
            gap: 0.75rem;
            flex-wrap: wrap;
            justify-content: space-between;
        }}
        .meta a {{
            color: #2563eb;
            text-decoration: none;
        }}
        .meta a:hover {{
            text-decoration: underline;
        }}
        .table-wrap {{
            max-height: 75vh;
            overflow-y: auto;
            overflow-x: auto;
            border-radius: 6px;
            box-shadow: inset 0 0 0 1px #e5e7eb;
            background: #ffffff;
        }}
        table {{
            border-collapse: separate;
            border-spacing: 0;
            table-layout: fixed;
            width: max-content;
            min-width: 100%;
            font-size: 0.8rem;
        }}
        thead {{
            position: sticky;
            top: 0;
            z-index: 3;
        }}
        th, td {{
            border: 1px solid #e5e7eb;
            padding: 0.15rem 0.25rem;
            text-align: left;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        th {{
            background: #f1f5f9;
            font-weight: 600;
        }}
        tbody tr:nth-child(even) {{
            background: #f9fafb;
        }}
        tbody tr:hover {{
            background: #e5f3ff;
        }}
        .sample-col,
        .sample-cell {{
            width: 7ch;
            min-width: 7ch;
            max-width: 7ch;
            text-align: left;
            white-space: nowrap;
            position: sticky;
            left: 0;
            z-index: 1;
            background: #f1f5f9;
        }}
        tbody .sample-cell {{
            background: #ffffff;
        }}
        tbody tr:nth-child(even) .sample-cell {{
            background: #f9fafb;
        }}
        tbody tr:hover .sample-cell {{
            background: #e5f3ff;
        }}
        thead .sample-col {{
            z-index: 4;
        }}
        .meta-header {{
            min-width: 8ch;
            white-space: nowrap;
            text-align: left;
        }}
        .meta-cell {{
            text-align: left;
            white-space: nowrap;
        }}
        .param-header {{
            writing-mode: vertical-rl;
            transform: rotate(180deg);
            padding: 0.25rem 0.1rem;
            text-align: center;
            white-space: nowrap;
            width: 32px;
            min-width: 32px;
            max-width: 32px;
        }}
        .param-header a {{
            color: #2563eb;
            text-decoration: none;
        }}
        .param-header a:hover {{
            text-decoration: underline;
        }}
        .value-cell {{
            text-align: right;
        }}
    </style>
</head>
<body>
    <div class="wrap">
        <div class="card">
            <h1>{escape(title)}</h1>
            <div class="meta">
                <div>
                    <span>Columns: {columns_count} • Rows: {rows_count}</span>
                </div>
                <div>
                    <a href="/web/matrix">← Back to method index</a>
                    &nbsp;|&nbsp;
                    <a href="/web/presence">Presence matrix</a>
                    &nbsp;|&nbsp;
                    <a href="/web/labdata">Lab data page</a>
                </div>
            </div>
            <div class="table-wrap">
                <table>
                    <thead>
                        <tr>{header_html}</tr>
                    </thead>
                    <tbody>
                        {body_html}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</body>
</html>"""
    return HTMLResponse(content=html, status_code=200)

