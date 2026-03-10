"""Presence-related web views (extracted from api/main.py)."""
from __future__ import annotations

import csv
import io
import logging
import re

from html import escape
from urllib.parse import quote_plus

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from api.shared import (
    ALLOWED_TABLES,
    TABLE_ALIASES,
    PRESENCE_METHOD_LABELS,
    FT_MODE_LABELS,
    canonical_table_name,
    method_label,
    run_query_with_rls,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Presence-specific helpers
# ---------------------------------------------------------------------------


def presence_method_label(name: str) -> str:
    method = canonical_table_name(name)
    if method in PRESENCE_METHOD_LABELS:
        return PRESENCE_METHOD_LABELS[method]
    return method_label(method)


def presence_method_category(name: str) -> str:
    method = canonical_table_name(name)
    if method.startswith("ft_icr_ms"):
        return "ft"
    if method in {"isotope_co2_werte", "isotope_hd_werte"}:
        return "isotope"
    return "gc"


def presence_method_link(name: str) -> str:
    method = canonical_table_name(name)
    if method.startswith("ft_icr_ms"):
        return "/web/labdata/ft-icr-ms"
    return f"/web/matrix/{method}"


def canonical_presence_col(col: str) -> str:
    if not col.startswith("has_"):
        return col
    method = col[4:]
    return f"has_{canonical_table_name(method)}"


def presence_alias_cols(col: str) -> list[str]:
    """Return source presence columns that feed one canonical `has_<method>` column."""
    if not col.startswith("has_"):
        return [col]
    method = col[4:]
    out = [f"has_{method}"]
    for alias, canon in TABLE_ALIASES.items():
        if canon == method:
            out.append(f"has_{alias}")
    return out


def _canonical_ft_mode(value: str | None) -> str | None:
    if not value:
        return None
    key = re.sub(r"[^a-z0-9]+", "", str(value).lower())
    return FT_MODE_LABELS.get(key, str(value))


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/web/presence", response_class=HTMLResponse)
def presence_html(request: Request):
    """Render the presence (samples x methods) matrix as an HTML table with:
    - vertical method headers that link to per-method pages,
    - a narrow checkbox column per method,
    - a filter row of checkboxes (per method) above the samples,
    - a selection checkbox per sample,
    - a button to open a new window with only the selected samples.
    """
    # Read simple presence view with booleans
    cols, rows = run_query_with_rls(
        "SELECT * FROM public.analysis_presence_simple ORDER BY samplenumber",
        request,
    )

    title = "OrgChem \u2013 Presence matrix"

    # Ensure samplenumber is the first logical column
    cols = list(cols or [])
    if "samplenumber" not in cols:
        cols = ["samplenumber"] + [c for c in cols if c != "samplenumber"]

    # Methods present in the DB view (canonicalized to collapse aliases)
    view_method_cols = sorted({canonical_presence_col(c) for c in cols if c != "samplenumber"})

    # Methods from config (ALLOWED_TABLES) — we expect has_<method> in the presence view
    cfg_method_cols = [f"has_{t}" for t in sorted(ALLOWED_TABLES)]

    # Replace single FT presence with three FT mode-specific presence columns.
    ft_split_cols = [
        "has_ft_icr_ms_appipos",
        "has_ft_icr_ms_esineg",
        "has_ft_icr_ms_esipos",
    ]
    method_cols_raw = sorted(set(view_method_cols) | set(cfg_method_cols))
    method_cols = [c for c in method_cols_raw if c != "has_ft_icr_ms"]
    for c in ft_split_cols:
        if c not in method_cols:
            method_cols.append(c)
    method_cols = sorted(method_cols)

    # Build sample -> FT mode presence map so split FT columns are data-driven.
    ft_presence_map: dict[str, set[str]] = {}
    try:
        _, ft_rows = run_query_with_rls(
            (
                "SELECT samplenumber, "
                "COALESCE(NULLIF(method, ''), NULLIF(data_type, '')) AS ft_mode "
                "FROM public.ft_icr_ms_entries"
            ),
            request,
        )
        for fr in ft_rows:
            sample = str(fr.get("samplenumber") or "")
            mode = _canonical_ft_mode(fr.get("ft_mode"))
            if not sample or mode not in {"APPIpos", "ESIneg", "ESIpos"}:
                continue
            ft_presence_map.setdefault(sample, set()).add(mode)
    except Exception:
        # Presence page should still render even if FT entries view is unavailable.
        ft_presence_map = {}

    # Meta counters (columns = samplenumber + all methods; select column is UI only)
    rows_count = len(rows)
    columns_count = 1 + len(method_cols)

    # --------- build header ----------
    header_cells = []

    # 0) Selection column (for row checkboxes)
    header_cells.append(
        '<th class="select-col">'
        '<input type="checkbox" id="select-all-samples" />'
        "</th>"
    )

    # 1) Sample column (no visible label)
    header_cells.append('<th class="sample-col"></th>')

    # 2) Method columns
    for c in method_cols:
        method = c[4:] if c.startswith("has_") else c
        label = presence_method_label(method)
        cat = presence_method_category(method)
        method_href = presence_method_link(method)
        header_cells.append(
            f'<th class="method-header cat-{escape(cat)}" data-col="{escape(c)}">'
            f'<a href="{escape(method_href)}" '
            f'target="_blank" rel="noopener">{escape(label)}</a>'
            "</th>"
        )
    header_html = "".join(header_cells)

    # --------- filter row ----------
    filter_cells = []
    filter_cells.append('<td class="select-col filter-cell"></td>')
    filter_cells.append('<td class="sample-col filter-cell"></td>')
    for c in method_cols:
        method = c[4:] if c.startswith("has_") else c
        cat = presence_method_category(method)
        filter_cells.append(
            f'<td class="bool-cell filter-cell cat-{escape(cat)}" data-col="{escape(c)}">'
            f'<input type="checkbox" class="method-filter" data-col="{escape(c)}" />'
            "</td>"
        )
    filter_row_html = "".join(filter_cells)

    # --------- body ----------
    body_rows = []
    for r in rows:
        sn = r.get("samplenumber", "")
        sn_txt = escape(str(sn))
        cells = []

        # Selection checkbox
        cells.append(
            f'<td class="select-cell">'
            f'<input type="checkbox" class="row-select" data-sample="{sn_txt}" />'
            "</td>"
        )

        # Sample id
        cells.append(f'<td class="sample-cell">{sn_txt}</td>')

        # Method boolean cells (missing cols are treated as False)
        for c in method_cols:
            method = c[4:] if c.startswith("has_") else c
            cat = presence_method_category(method)

            if c == "has_ft_icr_ms_appipos":
                val = "APPIpos" in ft_presence_map.get(str(sn), set())
            elif c == "has_ft_icr_ms_esineg":
                val = "ESIneg" in ft_presence_map.get(str(sn), set())
            elif c == "has_ft_icr_ms_esipos":
                val = "ESIpos" in ft_presence_map.get(str(sn), set())
            else:
                val = any(bool(r.get(src_c, False)) for src_c in presence_alias_cols(c))

            checked = bool(val)
            data_val = "true" if checked else "false"
            cells.append(
                f'<td class="bool-cell cat-{escape(cat)}" data-col="{escape(c)}" data-value="{data_val}">'
                f'<input type="checkbox" disabled {"checked" if checked else ""} />'
                "</td>"
            )

        body_rows.append(
            f'<tr class="data-row" data-samplenumber="{sn_txt}">{"".join(cells)}</tr>'
        )
    body_html = "\n".join(body_rows)

    json_link = "/api/presence"

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
        .meta-left {{
            display: flex;
            align-items: center;
            gap: 0.75rem;
            flex-wrap: wrap;
        }}
        .meta a {{
            color: #2563eb;
            text-decoration: none;
        }}
        .meta a:hover {{
            text-decoration: underline;
        }}
        .toolbar-right {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}
        .meta input[type="search"] {{
            padding: 0.25rem 0.5rem;
            font-size: 0.85rem;
            border-radius: 4px;
            border: 1px solid #d1d5db;
            min-width: 220px;
        }}
        .toolbar-right button {{
            padding: 0.3rem 0.7rem;
            font-size: 0.8rem;
            border-radius: 4px;
            border: 1px solid #2563eb;
            background: #2563eb;
            color: #ffffff;
            cursor: pointer;
            white-space: nowrap;
        }}
        .toolbar-right button:hover {{
            background: #1d4ed8;
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
            border-collapse: collapse;
            table-layout: fixed;
            width: max-content;
            min-width: 100%;
            font-size: 0.85rem;
        }}
        thead {{
            position: sticky;
            top: 0;
            z-index: 1;
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
            cursor: default;
        }}
        tbody tr:nth-child(even) {{
            background: #f9fafb;
        }}
        tbody tr:hover {{
            background: #e5f3ff;
        }}
        .bool-cell {{
            text-align: center;
            width: 28px;
            min-width: 28px;
            max-width: 28px;
        }}
        tbody .bool-cell input[type="checkbox"] {{
            pointer-events: none;
        }}
        .select-col,
        .select-cell {{
            width: 32px;
            min-width: 32px;
            max-width: 32px;
            text-align: center;
        }}
        .sample-col,
        .sample-cell {{
            width: 7ch;
            min-width: 7ch;
            max-width: 7ch;
            text-align: left;
            white-space: nowrap;
        }}
        .method-header {{
            writing-mode: vertical-rl;
            transform: rotate(180deg);
            padding: 0.25rem 0.1rem;
            text-align: center;
            white-space: nowrap;
            width: 28px;
            min-width: 28px;
            max-width: 28px;
        }}
        .method-header a {{
            color: #1f2937;
            text-decoration: none;
        }}
        .method-header a:hover {{
            text-decoration: underline;
        }}
        .method-header.cat-gc {{
            background: #e2e8f0;
        }}
        .method-header.cat-ft {{
            background: #bfdbfe;
        }}
        .method-header.cat-isotope {{
            background: #bbf7d0;
        }}
        .filter-row td {{
            background: #e5ecf7;
        }}
        .filter-row .bool-cell.cat-gc {{
            background: #e5ecf7;
        }}
        .filter-row .bool-cell.cat-ft {{
            background: #dbeafe;
        }}
        .filter-row .bool-cell.cat-isotope {{
            background: #dcfce7;
        }}
        tbody .bool-cell.cat-gc {{
            background: #f8fafc;
        }}
        tbody .bool-cell.cat-ft {{
            background: #eff6ff;
        }}
        tbody .bool-cell.cat-isotope {{
            background: #f0fdf4;
        }}
        .filter-row .bool-cell {{
            cursor: pointer;
        }}
    </style>
</head>
<body>
    <div class="wrap">
        <div class="card">
            <h1>{escape(title)}</h1>
            <div class="meta">
                <div class="meta-left">
                    <span>Columns: {columns_count} &bull; Rows: {rows_count}</span>
                    <span><a href="/web/matrix">&larr; Back to method index</a></span>
                    <span>|</span>
                    <span><a href="/web/samples/filter">Sample filters</a></span>
                    <span>|</span>
                    <span><a href="/web/explorer">Data Explorer</a></span>
                    <span>|</span>
                    <span><a href="{escape(json_link)}" target="_blank" rel="noopener">View JSON API</a></span>
                    <label>
                        Search:
                        <input type="search" id="search-input" placeholder="Filter rows\u2026" />
                    </label>
                </div>
                <div class="toolbar-right">
                    <button id="open-selected-btn">Open selected samples in new window</button>
                </div>
            </div>
            <div class="table-wrap">
                <table>
                    <thead>
                        <tr>{header_html}</tr>
                        <tr class="filter-row">{filter_row_html}</tr>
                    </thead>
                    <tbody>
                        {body_html}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    <script>
    (function() {{
        const searchInput = document.getElementById("search-input");
        const methodFilters = Array.from(document.querySelectorAll(".method-filter"));
        const rows = Array.from(document.querySelectorAll("tbody tr.data-row"));
        const selectAll = document.getElementById("select-all-samples");
        const rowCheckboxes = () => Array.from(document.querySelectorAll(".row-select"));
        const openSelectedBtn = document.getElementById("open-selected-btn");

        function applyFilters() {{
            const q = (searchInput.value || "").trim().toLowerCase();
            const activeMethods = methodFilters
                .filter(cb => cb.checked)
                .map(cb => cb.getAttribute("data-col"));

            rows.forEach(tr => {{
                const sn = (tr.getAttribute("data-samplenumber") || "").toLowerCase();
                const matchesSearch = !q || sn.includes(q);

                let matchesMethods = true;
                for (const col of activeMethods) {{
                    const cell = tr.querySelector('td.bool-cell[data-col="' + col + '"]');
                    if (!cell || cell.getAttribute("data-value") !== "true") {{
                        matchesMethods = false;
                        break;
                    }}
                }}

                tr.style.display = (matchesSearch && matchesMethods) ? "" : "none";
            }});
        }}

        if (searchInput) {{
            searchInput.addEventListener("input", applyFilters);
        }}
        methodFilters.forEach(cb => {{
            cb.addEventListener("change", applyFilters);
        }});

        if (selectAll) {{
            selectAll.addEventListener("change", function() {{
                const all = this.checked;
                rowCheckboxes().forEach(cb => {{
                    cb.checked = all;
                }});
            }});
        }}

        if (openSelectedBtn) {{
            openSelectedBtn.addEventListener("click", function() {{
                const selected = rowCheckboxes()
                    .filter(cb => cb.checked)
                    .map(cb => cb.getAttribute("data-sample"))
                    .filter(Boolean);

                if (!selected.length) {{
                    alert("No samples selected.");
                    return;
                }}

                const qs = encodeURIComponent(selected.join(","));
                const url = "/web/presence/selected?s=" + qs;
                window.open(url, "_blank", "noopener");
            }});
        }}
    }})();
    </script>
</body>
    </html>
    """
    return html


@router.get("/web/samples/filter", response_class=HTMLResponse)
def sample_filter_html(request: Request):
    """Sample picker with non-method filters + method selection for mixed view."""
    title = "OrgChem \u2013 Sample filters"

    try:
        cols, rows = run_query_with_rls(
            """
            SELECT p.samplenumber,
                   COALESCE(string_agg(DISTINCT sp.project_id, ', '), '') AS project,
                   ''::text AS rock_type,
                   ''::text AS analysis_date,
                   ''::text AS operator_name
            FROM public.analysis_presence_simple p
            LEFT JOIN public.sample_projects sp ON sp.samplenumber = p.samplenumber
            GROUP BY p.samplenumber
            ORDER BY p.samplenumber
            """,
            request,
        )
    except Exception:
        cols, rows = run_query_with_rls(
            """
            SELECT p.samplenumber,
                   ''::text AS project,
                   ''::text AS rock_type,
                   ''::text AS analysis_date,
                   ''::text AS operator_name
            FROM public.analysis_presence_simple p
            ORDER BY p.samplenumber
            """,
            request,
        )

    methods = sorted(ALLOWED_TABLES)
    method_checks = []
    for m in methods:
        label = method_label(m)
        method_checks.append(
            f'<label class="method-chip"><input type="checkbox" class="method-choice" value="{escape(m)}" checked /> {escape(label)}</label>'
        )
    method_checks_html = "".join(method_checks)

    body_rows = []
    for r in rows:
        sn = escape(str(r.get("samplenumber", "")))
        project = escape(str(r.get("project", "") or ""))
        rock = escape(str(r.get("rock_type", "") or ""))
        adate = escape(str(r.get("analysis_date", "") or ""))
        operator = escape(str(r.get("operator_name", "") or ""))
        body_rows.append(
            f'<tr class="data-row" data-samplenumber="{sn}" data-project="{project.lower()}" data-rock="{rock.lower()}" data-date="{adate.lower()}" data-operator="{operator.lower()}">'
            f'<td class="select-cell"><input type="checkbox" class="row-select" data-sample="{sn}" /></td>'
            f'<td>{sn}</td><td>{project}</td><td>{rock}</td><td>{adate}</td><td>{operator}</td>'
            "</tr>"
        )
    body_html = "\n".join(body_rows)

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
            background: #fff;
            border-radius: 8px;
            padding: 1rem 1.25rem;
            box-shadow: 0 1px 4px rgba(15, 23, 42, 0.06);
        }}
        .meta {{
            display: flex;
            gap: 0.75rem;
            flex-wrap: wrap;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 0.65rem;
            color: #6b7280;
            font-size: 0.9rem;
        }}
        .meta a {{
            color: #2563eb;
            text-decoration: none;
        }}
        .meta a:hover {{
            text-decoration: underline;
        }}
        .filters, .methods {{
            display: flex;
            gap: 0.5rem;
            flex-wrap: wrap;
            margin-bottom: 0.6rem;
        }}
        .filters input {{
            padding: 0.28rem 0.5rem;
            border: 1px solid #d1d5db;
            border-radius: 4px;
            min-width: 130px;
        }}
        .method-chip {{
            display: inline-flex;
            align-items: center;
            gap: 0.3rem;
            border: 1px solid #d1d5db;
            border-radius: 999px;
            padding: 0.2rem 0.5rem;
            background: #f8fafc;
            font-size: 0.82rem;
        }}
        .toolbar button {{
            padding: 0.35rem 0.75rem;
            border: 1px solid #2563eb;
            border-radius: 4px;
            background: #2563eb;
            color: #fff;
            cursor: pointer;
        }}
        .toolbar button:hover {{
            background: #1d4ed8;
        }}
        .table-wrap {{
            max-height: 70vh;
            overflow: auto;
            border: 1px solid #e5e7eb;
            border-radius: 6px;
            background: #fff;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.85rem;
        }}
        th, td {{
            border: 1px solid #e5e7eb;
            padding: 0.25rem 0.4rem;
            text-align: left;
            white-space: nowrap;
        }}
        th {{
            background: #f1f5f9;
        }}
        .select-cell, .select-head {{
            text-align: center;
            width: 34px;
        }}
    </style>
</head>
<body>
    <div class="card">
        <h1>{escape(title)}</h1>
        <div class="meta">
            <span><a href="/web/presence">&larr; Back to presence matrix</a></span>
            <span>|</span>
            <span><a href="/web/explorer">Data Explorer</a></span>
            <span>|</span>
            <span><a href="/web/labdata">Lab data page</a></span>
            <span>|</span>
            <span>Select samples by metadata (extendable)</span>
        </div>
        <div class="filters">
            <input id="f-sample" placeholder="Sample number" />
            <input id="f-project" placeholder="Project" />
            <input id="f-rock" placeholder="Rock type (placeholder)" />
            <input id="f-date" placeholder="Date (placeholder)" />
            <input id="f-operator" placeholder="Operator (placeholder)" />
        </div>
        <div class="methods">{method_checks_html}</div>
        <div class="toolbar" style="margin-bottom:0.6rem;">
            <button id="open-mixed-btn">Open mixed combined view</button>
        </div>
        <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th class="select-head"><input type="checkbox" id="select-all" /></th>
                        <th>Sample</th>
                        <th>Project</th>
                        <th>Rock type</th>
                        <th>Date</th>
                        <th>Operator</th>
                    </tr>
                </thead>
                <tbody>{body_html}</tbody>
            </table>
        </div>
    </div>
    <script>
    (function() {{
        const rows = Array.from(document.querySelectorAll("tr.data-row"));
        const rowChecks = () => Array.from(document.querySelectorAll(".row-select"));
        const selectAll = document.getElementById("select-all");
        const openBtn = document.getElementById("open-mixed-btn");
        const fields = {{
            sample: document.getElementById("f-sample"),
            project: document.getElementById("f-project"),
            rock: document.getElementById("f-rock"),
            date: document.getElementById("f-date"),
            operator: document.getElementById("f-operator"),
        }};
        function applyFilters() {{
            const q = {{
                sample: (fields.sample.value || "").trim().toLowerCase(),
                project: (fields.project.value || "").trim().toLowerCase(),
                rock: (fields.rock.value || "").trim().toLowerCase(),
                date: (fields.date.value || "").trim().toLowerCase(),
                operator: (fields.operator.value || "").trim().toLowerCase(),
            }};
            rows.forEach(tr => {{
                const ok =
                    (!q.sample || (tr.dataset.samplenumber || "").toLowerCase().includes(q.sample)) &&
                    (!q.project || (tr.dataset.project || "").includes(q.project)) &&
                    (!q.rock || (tr.dataset.rock || "").includes(q.rock)) &&
                    (!q.date || (tr.dataset.date || "").includes(q.date)) &&
                    (!q.operator || (tr.dataset.operator || "").includes(q.operator));
                tr.style.display = ok ? "" : "none";
            }});
        }}
        Object.values(fields).forEach(el => el.addEventListener("input", applyFilters));
        if (selectAll) {{
            selectAll.addEventListener("change", function() {{
                const all = this.checked;
                rowChecks().forEach(cb => {{ cb.checked = all; }});
            }});
        }}
        if (openBtn) {{
            openBtn.addEventListener("click", function() {{
                const samples = rowChecks().filter(cb => cb.checked).map(cb => cb.getAttribute("data-sample")).filter(Boolean);
                if (!samples.length) {{
                    alert("No samples selected.");
                    return;
                }}
                const methods = Array.from(document.querySelectorAll(".method-choice"))
                    .filter(cb => cb.checked)
                    .map(cb => cb.value);
                const qs = new URLSearchParams();
                qs.set("s", samples.join(","));
                if (methods.length) qs.set("m", methods.join(","));
                window.open("/web/presence/selected?" + qs.toString(), "_blank", "noopener");
            }});
        }}
    }})();
    </script>
</body>
</html>"""
    return HTMLResponse(content=html)


@router.get("/web/presence/selected", response_class=HTMLResponse)
def presence_selected_html(request: Request, s: str | None = None, m: str | None = None, format: str | None = None):
    """Combined matrix view for several selected samples.

    The list of samples is passed via query parameter `s`, as a comma-separated list:
    /web/presence/selected?s=SAMPLEx,SAMPLEy

    For these samples we build a wide matrix across *all* methods, similar to
    /web/matrix/{method}, but combined in one table:
    - rows = samples
    - columns = per-method parameters
    - an additional presence column per method (checkmark/empty)
    - a top header row with one cell per method and a toggle button that
      collapses/expands all parameter columns for that method, leaving only
      the presence column visible when collapsed.
    """
    title = "OrgChem \u2013 Selected samples (combined matrix)"

    raw = (s or "").strip()
    samples = [x.strip() for x in raw.split(",") if x.strip()]
    samples = sorted(set(samples))
    method_filter_raw = [x.strip() for x in (m or "").split(",") if x.strip()]
    method_filter: list[str] = []
    for m_name in method_filter_raw:
        cm = canonical_table_name(m_name)
        if cm in ALLOWED_TABLES and cm not in method_filter:
            method_filter.append(cm)
    if not samples:
        return HTMLResponse(
            content='<!DOCTYPE html>\n'
            '<html lang="en">\n'
            '<head><meta charset="utf-8" /><title>OrgChem \u2013 Selected samples</title></head>\n'
            '<body style="font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;">\n'
            '  <h1>OrgChem \u2013 Selected samples</h1>\n'
            '  <p>No samples were provided. Use the presence matrix to select samples, or pass <code>?s=G000001,G000002</code> in the URL.</p>\n'
            '</body>\n</html>',
            status_code=200,
        )

    selected_set = set(samples)

    def pretty_method(name: str) -> str:
        return method_label(name)

    # ---- Build combined data matrix in Python ----
    matrix_rows: dict[str, dict] = {sn: {"samplenumber": sn} for sn in selected_set}
    group_cols: dict[str, list[str]] = {}
    col_labels: dict[str, str] = {}

    def presence_col_id(method: str) -> str:
        return f"{method}__PRESENCE"

    methods_to_scan = method_filter if method_filter else sorted(ALLOWED_TABLES)
    for method in methods_to_scan:
        view_name = f"public.{method}_entries"
        try:
            sql = f"SELECT samplenumber, data FROM {view_name}"
            cols, rows = run_query_with_rls(sql, request)
        except Exception as e:
            logger.warning("Skipping method %s in selected view: %s", method, e)
            continue

        if not rows:
            continue

        method_has_selected = False
        gcols = None
        pres_id = None

        for r in rows:
            sn = r.get("samplenumber")
            if sn not in selected_set:
                continue
            data = r.get("data") or {}
            if not isinstance(data, dict):
                continue

            if not method_has_selected:
                method_has_selected = True
                gcols = group_cols.setdefault(method, [])
                pres_id = presence_col_id(method)
                if pres_id not in gcols:
                    gcols.append(pres_id)
                    col_labels[pres_id] = "has"
            else:
                gcols = group_cols[method]
                pres_id = presence_col_id(method)

            row = matrix_rows.setdefault(sn, {"samplenumber": sn})
            any_value = False

            for k, v in data.items():
                col_id = f"{method}__{k}"
                if col_id not in gcols:
                    gcols.append(col_id)
                    col_labels[col_id] = str(k)
                if col_id not in row or row[col_id] in (None, ""):
                    row[col_id] = v
                if v not in (None, ""):
                    any_value = True

            if any_value and pres_id is not None:
                row[pres_id] = True

    ordered_methods = [x for x in methods_to_scan if x in group_cols and group_cols[x]]

    if not ordered_methods:
        return HTMLResponse(
            content=f'<!DOCTYPE html>\n'
            f'<html lang="en">\n'
            f'<head><meta charset="utf-8" /><title>{escape(title)}</title></head>\n'
            f'<body style="font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;">\n'
            f'  <h1>{escape(title)}</h1>\n'
            f"  <p>No entries found for the selected samples: {escape(', '.join(samples))}.</p>\n"
            f'</body>\n</html>',
            status_code=200,
        )

    all_columns: list[str] = ["samplenumber"]
    for meth in ordered_methods:
        gcols = group_cols[meth]
        pres_id = presence_col_id(meth)
        ordered = []
        if pres_id in gcols:
            ordered.append(pres_id)
        ordered.extend([c for c in gcols if c != pres_id])
        group_cols[meth] = ordered
        all_columns.extend(ordered)

    if (format or "").lower() == "csv":
        csv_buf = io.StringIO()
        writer = csv.writer(csv_buf)
        csv_header = ["samplenumber"]
        for meth in ordered_methods:
            for cid in group_cols[meth]:
                if cid == presence_col_id(meth):
                    csv_header.append(f"{pretty_method(meth)}: has")
                else:
                    csv_header.append(f"{pretty_method(meth)}: {col_labels.get(cid, cid)}")
        writer.writerow(csv_header)
        for sn in samples:
            row = matrix_rows.get(sn, {"samplenumber": sn})
            out = [sn]
            for meth in ordered_methods:
                for cid in group_cols[meth]:
                    val = row.get(cid, "")
                    if cid == presence_col_id(meth):
                        out.append("1" if bool(val) else "")
                    else:
                        out.append("" if val in (None, "") else str(val))
            writer.writerow(out)
        return Response(
            content=csv_buf.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="selected_samples_matrix.csv"'},
        )

    # ---- Build HTML ----
    samples_summary = ", ".join(samples)
    csv_qs = f"s={quote_plus(raw)}"
    if ordered_methods:
        csv_qs += f"&m={quote_plus(','.join(ordered_methods))}"
    csv_qs += "&format=csv"

    top_cells: list[str] = []
    top_cells.append('<th class="sample-header" rowspan="2">Sample</th>')
    for meth in ordered_methods:
        col_span = len(group_cols[meth])
        label = pretty_method(meth)
        top_cells.append(
            f'<th class="group-header" data-method="{escape(meth)}" colspan="{col_span}">'
            f'<div class="group-header-inner">'
            f'<button class="method-toggle" data-method="{escape(meth)}" data-collapsed="false">&#8722;</button>'
            f'<span class="group-label">{escape(label)}</span>'
            f'</div>'
            '</th>'
        )
    header_top_html = "".join(top_cells)

    bottom_cells: list[str] = []
    for meth in ordered_methods:
        for cid in group_cols[meth]:
            base_classes = ["col-" + meth]
            if cid == presence_col_id(meth):
                base_classes.append("col-" + meth + "-presence")
                base_classes.append("presence-header")
                label = ""
            else:
                base_classes.append("param-header")
                label = col_labels.get(cid, cid)
            cls = " ".join(base_classes)
            bottom_cells.append(
                f'<th class="{escape(cls)}" data-method="{escape(meth)}" data-col="{escape(cid)}">'
                f'{escape(str(label))}'
                '</th>'
            )
    header_bottom_html = "".join(bottom_cells)
    sidebar_items_html = "".join(
        (
            f'<div class="method-item">'
            f'<button class="method-toggle" data-method="{escape(meth)}" data-collapsed="false">&#8722;</button>'
            f'<span class="method-name">{escape(pretty_method(meth))}</span>'
            f'</div>'
        )
        for meth in ordered_methods
    )

    body_rows_html: list[str] = []
    for sn in samples:
        row = matrix_rows.get(sn, {"samplenumber": sn})
        cells: list[str] = []
        sn_txt = escape(str(sn))
        cells.append(f'<td class="sample-cell">{sn_txt}</td>')
        for meth in ordered_methods:
            for cid in group_cols[meth]:
                val = row.get(cid, "")
                classes = ["cell", "col-" + meth]
                if cid == presence_col_id(meth):
                    classes.append("col-" + meth + "-presence")
                    classes.append("presence-cell")
                    checked = bool(val)
                    display_val = "&#10003;" if checked else ""
                else:
                    classes.append("param-cell")
                    display_val = escape(str(val)) if val not in (None, "") else ""
                cls = " ".join(classes)
                cells.append(f'<td class="{cls}">{display_val}</td>')
        body_rows_html.append("<tr>" + "".join(cells) + "</tr>")

    body_html = "\n".join(body_rows_html)

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
        .meta-left {{
            display: flex;
            align-items: center;
            gap: 0.75rem;
            flex-wrap: wrap;
        }}
        .meta a {{
            color: #2563eb;
            text-decoration: none;
        }}
        .meta a:hover {{
            text-decoration: underline;
        }}
        .matrix-layout {{
            display: grid;
            grid-template-columns: 220px 1fr;
            gap: 0.75rem;
            align-items: start;
        }}
        .method-sidebar {{
            border: 1px solid #e5e7eb;
            border-radius: 6px;
            background: #f8fafc;
            padding: 0.5rem;
            max-height: 75vh;
            overflow-y: auto;
        }}
        .method-sidebar-title {{
            font-size: 0.78rem;
            font-weight: 700;
            color: #475569;
            margin-bottom: 0.35rem;
            text-transform: uppercase;
            letter-spacing: 0.02em;
        }}
        .method-item {{
            display: flex;
            align-items: center;
            gap: 0.4rem;
            padding: 0.2rem 0;
        }}
        .method-item .method-name {{
            font-size: 0.82rem;
            color: #0f172a;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
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
            border-collapse: collapse;
            table-layout: fixed;
            width: max-content;
            min-width: 100%;
            font-size: 0.8rem;
        }}
        thead {{
            position: sticky;
            top: 0;
            z-index: 1;
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
            cursor: default;
        }}
        tbody tr:nth-child(even) {{
            background: #f9fafb;
        }}
        tbody tr:hover {{
            background: #e5f3ff;
        }}
        .sample-header {{
            min-width: 7ch;
            max-width: 7ch;
            vertical-align: bottom;
        }}
        .sample-cell {{
            width: 7ch;
            min-width: 7ch;
            max-width: 7ch;
            white-space: nowrap;
        }}
        .group-header {{
            text-align: center;
            white-space: nowrap;
            position: relative;
            padding: 0.15rem 0.25rem;
            vertical-align: top;
        }}
        .group-header .group-header-inner {{
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.35rem;
        }}
        .group-header .group-label {{
            display: inline-block;
            white-space: nowrap;
        }}
        .group-header button.method-toggle {{
            border: 1px solid #9ca3af;
            border-radius: 3px;
            background: #e5e7eb;
            font-size: 0.7rem;
            width: 1.2rem;
            height: 1.2rem;
            line-height: 1;
            padding: 0;
            cursor: pointer;
        }}
        .group-header button.method-toggle:hover {{
            background: #d1d5db;
        }}
        .presence-header {{
            text-align: center;
            width: 28px;
            min-width: 28px;
            max-width: 28px;
            vertical-align: bottom;
        }}
        .presence-cell {{
            text-align: center;
            width: 28px;
            min-width: 28px;
            max-width: 28px;
        }}
        .param-header {{
            padding: 0.25rem 0.1rem;
            text-align: center;
            white-space: nowrap;
            width: 32px;
            min-width: 32px;
            max-width: 32px;
            vertical-align: bottom;
        }}
        .param-cell {{
            text-align: right;
        }}
        @media (max-width: 980px) {{
            .matrix-layout {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="wrap">
        <div class="card">
            <h1>{escape(title)}</h1>
            <div class="meta">
                <div class="meta-left">
                    <span>Samples: {escape(samples_summary)}</span>
                    <span>|</span>
                    <span>Methods: {escape(', '.join(pretty_method(meth) for meth in ordered_methods))}</span>
                    <span>|</span>
                    <span><a href="/web/presence">&larr; Back to presence matrix</a></span>
                    <span>|</span>
                    <span><a href="/web/samples/filter">Sample filters</a></span>
                    <span>|</span>
                    <span><a href="/web/explorer">Data Explorer</a></span>
                    <span>|</span>
                    <span><a href="/web/labdata">Lab data page</a></span>
                    <span>|</span>
                    <span><a href="/web/presence/selected?{csv_qs}">Download CSV</a></span>
                </div>
            </div>
            <div class="matrix-layout">
                <aside class="method-sidebar">
                    <div class="method-sidebar-title">Methods</div>
                    {sidebar_items_html}
                </aside>
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>{header_top_html}</tr>
                            <tr>{header_bottom_html}</tr>
                        </thead>
                        <tbody>
                            {body_html}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
    <script>
    (function() {{
        const toggles = Array.from(document.querySelectorAll('.method-toggle'));
        function setMethodCollapsed(method, collapsed) {{
            if (!method) return;
            const paramCells = document.querySelectorAll('.col-' + method + ':not(.col-' + method + '-presence)');
            paramCells.forEach(el => {{
                el.style.display = collapsed ? 'none' : '';
            }});
            const methodToggles = document.querySelectorAll('.method-toggle[data-method="' + method + '"]');
            methodToggles.forEach(el => {{
                el.setAttribute('data-collapsed', collapsed ? 'true' : 'false');
                el.textContent = collapsed ? '+' : '\u2212';
            }});
        }}
        toggles.forEach(btn => {{
            btn.addEventListener('click', () => {{
                const method = btn.getAttribute('data-method');
                const isCollapsed = btn.getAttribute('data-collapsed') === 'true';
                setMethodCollapsed(method, !isCollapsed);
            }});
        }});
    }})();
    </script>
</body>
</html>
"""
    return HTMLResponse(content=html)
