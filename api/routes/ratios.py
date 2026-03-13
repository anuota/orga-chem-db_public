"""Calculate Ratios view – user-defined parameter ratios for GC compounds."""
from __future__ import annotations

import os
from html import escape
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from api.shared import (
    ALLOWED_TABLES,
    LAB_METHODS,
    METHOD_GROUPS,
    canonical_table_name,
    column_display_name,
    method_label,
    run_query_with_rls,
)

router = APIRouter()

# GC compound tables only (exclude FT-ICR-MS and isotopes)
_GC_TABLES = sorted(ALLOWED_TABLES - LAB_METHODS)


# ---------------------------------------------------------------------------
# /api/ratios/meta – available GC methods and their parameter columns
# ---------------------------------------------------------------------------

@router.get("/api/ratios/meta")
def ratios_meta(request: Request):
    methods_info: list[dict] = []
    for method in _GC_TABLES:
        view_name = f"public.{method}_entries"
        try:
            _cols, rows = run_query_with_rls(
                f"SELECT data FROM {view_name}",
                request,
            )
        except Exception:
            continue
        param_keys: set[str] = set()
        for r in rows:
            data = r.get("data") or {}
            if isinstance(data, dict):
                param_keys.update(data.keys())
        if not param_keys:
            continue
        # Build display-friendly column list
        columns = []
        for k in sorted(param_keys):
            display = column_display_name(method, k)
            columns.append({"key": k, "label": display or k})
        methods_info.append({
            "method": method,
            "label": method_label(method),
            "columns": columns,
        })

    # Organise into groups (only GC Compounds group)
    gc_group = next((g for g in METHOD_GROUPS if g["label"] == "GC Compounds"), None)
    gc_order = gc_group["methods"] if gc_group else []
    ordered = sorted(methods_info, key=lambda m: (
        gc_order.index(m["method"]) if m["method"] in gc_order else 999,
        m["method"],
    ))

    return {"methods": ordered}


# ---------------------------------------------------------------------------
# /api/ratios/compute – compute user-defined ratios
# ---------------------------------------------------------------------------

_FORMULAS = {
    "a/b":       lambda a, b: a / b if b else None,
    "b/a":       lambda a, b: b / a if a else None,
    "(a+b)/a":   lambda a, b: (a + b) / a if a else None,
    "(a-b)/a":   lambda a, b: (a - b) / a if a else None,
    "a/(a+b)":   lambda a, b: a / (a + b) if (a + b) else None,
    "b/(a+b)":   lambda a, b: b / (a + b) if (a + b) else None,
    "(a-b)/(a+b)": lambda a, b: (a - b) / (a + b) if (a + b) else None,
}


class RatioColumn(BaseModel):
    formula: str          # e.g. "a/b"
    method: str           # e.g. "hopanes"
    param_a: str          # parameter key for A
    param_b: str          # parameter key for B
    label: str | None = None  # optional custom column label


class RatioRequest(BaseModel):
    columns: list[RatioColumn]
    sample_filter: list[str] | None = None  # empty = all


@router.post("/api/ratios/compute")
def ratios_compute(body: RatioRequest, request: Request):
    sample_filter = set(body.sample_filter) if body.sample_filter else None

    # Gather data per method (only fetch methods actually used)
    needed_methods = {canonical_table_name(c.method) for c in body.columns}
    method_data: dict[str, dict[str, list[dict]]] = {}

    for method in needed_methods:
        if method not in ALLOWED_TABLES:
            continue
        view = f"public.{method}_entries"
        try:
            _c, rows = run_query_with_rls(
                f"SELECT samplenumber, data FROM {view}",
                request,
            )
        except Exception:
            continue
        by_sn: dict[str, list[dict]] = {}
        for r in rows:
            sn = str(r.get("samplenumber") or "")
            if not sn:
                continue
            if sample_filter and sn not in sample_filter:
                continue
            data = r.get("data") or {}
            if isinstance(data, dict):
                by_sn.setdefault(sn, []).append(data)
        method_data[method] = by_sn

    # Determine all samples across all methods
    all_samples: set[str] = set()
    for by_sn in method_data.values():
        all_samples.update(by_sn.keys())

    # Build result columns metadata
    out_columns: list[dict] = [{"id": "samplenumber", "label": "Sample"}]
    for i, col in enumerate(body.columns):
        method = canonical_table_name(col.method)
        if col.label:
            lbl = col.label
        else:
            disp_a = column_display_name(method, col.param_a) or col.param_a
            disp_b = column_display_name(method, col.param_b) or col.param_b
            formula_lbl = col.formula.replace("a", disp_a).replace("b", disp_b)
            lbl = f"{method_label(method)}: {formula_lbl}"
        out_columns.append({
            "id": f"ratio_{i}",
            "label": lbl,
            "formula": col.formula,
            "method": method,
            "param_a": col.param_a,
            "param_b": col.param_b,
        })

    # Compute ratios
    out_rows: list[dict] = []
    for sn in sorted(all_samples):
        row: dict[str, Any] = {"samplenumber": sn}
        for i, col in enumerate(body.columns):
            method = canonical_table_name(col.method)
            fn = _FORMULAS.get(col.formula)
            if not fn:
                row[f"ratio_{i}"] = None
                continue
            entries = method_data.get(method, {}).get(sn, [])
            if not entries:
                row[f"ratio_{i}"] = None
                continue
            # Use first entry (primary measurement)
            data = entries[0]
            raw_a = data.get(col.param_a)
            raw_b = data.get(col.param_b)
            val_a = _to_float(raw_a)
            val_b = _to_float(raw_b)
            if val_a is None or val_b is None:
                row[f"ratio_{i}"] = None
            else:
                try:
                    result = fn(val_a, val_b)
                    row[f"ratio_{i}"] = round(result, 6) if result is not None else None
                except (ZeroDivisionError, OverflowError):
                    row[f"ratio_{i}"] = None
        out_rows.append(row)

    return {"columns": out_columns, "rows": out_rows, "total": len(out_rows)}


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, dict):
        v = v.get("value", v.get("orig"))
    if v is None:
        return None
    try:
        s = str(v).strip().replace(",", ".")
        return float(s)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# /web/ratios – HTML page
# ---------------------------------------------------------------------------

@router.get("/web/ratios", response_class=HTMLResponse)
def ratios_html():
    title = "OrgChem \u2013 Calculate Ratios"
    formulas = list(_FORMULAS.keys())

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>{escape(title)}</title>
<style>
:root {{ color-scheme: light; }}
* {{ box-sizing: border-box; }}
body {{
    font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
    margin: 0; padding: 0;
    background: #f5f6fa;
}}
/* --- Top bar --- */
.topbar {{
    background: #fff;
    border-bottom: 1px solid #e5e7eb;
    padding: 0.75rem 1.5rem;
    display: flex; align-items: center; gap: 1rem; flex-wrap: wrap;
    justify-content: space-between;
}}
.topbar h1 {{ margin: 0; font-size: 1.25rem; white-space: nowrap; }}
.topbar-links {{ display: flex; gap: 0.75rem; font-size: 0.85rem; }}
.topbar-links a {{ color: #2563eb; text-decoration: none; }}
.topbar-links a:hover {{ text-decoration: underline; }}
/* --- Layout --- */
.main {{
    display: grid;
    grid-template-columns: 380px 1fr;
    height: calc(100vh - 52px);
}}
@media (max-width: 900px) {{
    .main {{ grid-template-columns: 1fr; }}
    .sidebar {{ max-height: 45vh; }}
}}
/* --- Sidebar --- */
.sidebar {{
    background: #fff;
    border-right: 1px solid #e5e7eb;
    overflow-y: auto; padding: 0.75rem;
    display: flex; flex-direction: column; gap: 0.75rem;
}}
.panel {{
    border: 1px solid #e5e7eb;
    border-radius: 6px;
    padding: 0.6rem 0.75rem;
}}
.panel-title {{
    font-weight: 700; font-size: 0.85rem; color: #334155;
    margin-bottom: 0.4rem;
}}
label.field {{ display: block; font-size: 0.8rem; margin-bottom: 0.45rem; color: #475569; }}
label.field span {{ display: block; margin-bottom: 0.15rem; font-weight: 600; }}
select, input[type="number"] {{
    width: 100%; padding: 0.3rem 0.4rem; font-size: 0.82rem;
    border: 1px solid #d1d5db; border-radius: 4px;
    background: #fff;
}}
select:focus, input:focus {{ outline: 2px solid #2563eb; }}
.btn {{
    padding: 0.35rem 0.75rem; font-size: 0.82rem; border-radius: 4px;
    border: 1px solid #2563eb; cursor: pointer; white-space: nowrap;
}}
.btn-primary {{ background: #2563eb; color: #fff; }}
.btn-primary:hover {{ background: #1d4ed8; }}
.btn-outline {{ background: #fff; color: #2563eb; }}
.btn-outline:hover {{ background: #eff6ff; }}
.btn-danger {{ background: #fff; color: #dc2626; border-color: #dc2626; }}
.btn-danger:hover {{ background: #fef2f2; }}
.btn-sm {{ padding: 0.2rem 0.5rem; font-size: 0.78rem; }}
.actions {{ display: flex; gap: 0.4rem; flex-wrap: wrap; margin-top: 0.5rem; }}
/* --- Ratio columns list --- */
.ratio-col-list {{ display: flex; flex-direction: column; gap: 0.35rem; margin-top: 0.5rem; }}
.ratio-col-item {{
    display: flex; align-items: center; gap: 0.3rem;
    background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 4px;
    padding: 0.25rem 0.4rem; font-size: 0.78rem;
}}
.ratio-col-item .tag {{
    background: #e0e7ff; color: #3730a3; padding: 0.1rem 0.35rem;
    border-radius: 3px; font-size: 0.72rem; font-weight: 600;
}}
.ratio-col-item .formula {{ color: #64748b; flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
.ratio-col-item .remove-col {{
    cursor: pointer; color: #94a3b8; font-size: 1rem; line-height: 1;
}}
.ratio-col-item .remove-col:hover {{ color: #dc2626; }}
/* --- Sample range filter --- */
.range-row {{ display: flex; gap: 0.4rem; align-items: center; font-size: 0.8rem; }}
.range-row input {{ width: 80px; }}
.range-row span {{ color: #64748b; }}
/* --- Table area --- */
.table-area {{
    overflow: auto; padding: 1rem;
}}
.table-area table {{
    border-collapse: collapse; width: max-content; min-width: 100%;
    font-size: 0.82rem;
}}
.table-area th, .table-area td {{
    border: 1px solid #e5e7eb; padding: 0.3rem 0.5rem;
    text-align: left; white-space: nowrap;
}}
.table-area th {{
    background: #f1f5f9; font-weight: 600;
    position: sticky; top: 0; z-index: 1; cursor: pointer;
    user-select: none;
}}
.table-area th:hover {{ background: #e2e8f0; }}
.table-area th .sort-ind {{ font-size: 0.7rem; margin-left: 0.2rem; color: #94a3b8; }}
.table-area tbody tr:nth-child(even) {{ background: #f9fafb; }}
.table-area tbody tr:hover {{ background: #e5f3ff; }}
.table-area td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
.table-area td.null {{ color: #cbd5e1; text-align: center; }}
.status {{
    padding: 0.5rem 0.75rem; font-size: 0.82rem; color: #64748b;
}}
.highlight {{ background: #fef3c7 !important; }}
</style>
</head>
<body>
<div class="topbar">
    <h1>{escape(title)}</h1>
    <div class="topbar-links">
        <a href="/web/presence">Presence matrix</a>
        <a href="/web/matrix">Methods index</a>
        <a href="/web/explorer">Data Explorer</a>
        <a href="/web/samples/filter">Sample filters</a>
        <a href="/web/upload">Upload Data</a>
    </div>
</div>
<div class="main">
<!-- ============ SIDEBAR ============ -->
<aside class="sidebar" id="sidebar">
    <!-- 1. Define ratio column -->
    <div class="panel">
        <div class="panel-title">Define Ratio Column</div>
        <label class="field">
            <span>Formula</span>
            <select id="sel-formula">
                {"".join(f'<option value="{escape(f)}">{escape(f)}</option>' for f in formulas)}
            </select>
        </label>
        <label class="field">
            <span>Compound group</span>
            <select id="sel-method">
                <option value="">(loading\u2026)</option>
            </select>
        </label>
        <label class="field">
            <span>Parameter A</span>
            <select id="sel-param-a" disabled>
                <option value="">Select compound first</option>
            </select>
        </label>
        <label class="field">
            <span>Parameter B</span>
            <select id="sel-param-b" disabled>
                <option value="">Select compound first</option>
            </select>
        </label>
        <div class="actions">
            <button class="btn btn-primary" id="btn-add-col">Add column</button>
        </div>
    </div>
    <!-- 2. Active ratio columns -->
    <div class="panel">
        <div class="panel-title">Active Columns</div>
        <div class="ratio-col-list" id="col-list">
            <span style="color:#94a3b8;font-size:0.8rem;">No columns defined yet.</span>
        </div>
        <div class="actions">
            <button class="btn btn-primary" id="btn-compute">Compute</button>
            <button class="btn btn-danger btn-sm" id="btn-clear-cols">Clear all</button>
            <button class="btn btn-outline btn-sm" id="btn-export-csv">Export CSV</button>
        </div>
    </div>
    <!-- 3. Filter by range -->
    <div class="panel">
        <div class="panel-title">Filter by Range</div>
        <label class="field">
            <span>Column</span>
            <select id="filter-col">
                <option value="">Add columns first</option>
            </select>
        </label>
        <div class="range-row">
            <span>Min</span>
            <input type="number" id="filter-min" step="any" placeholder="\u2212\u221e"/>
            <span>Max</span>
            <input type="number" id="filter-max" step="any" placeholder="+\u221e"/>
            <button class="btn btn-sm btn-outline" id="btn-apply-range">Apply</button>
            <button class="btn btn-sm btn-danger" id="btn-clear-range">Clear</button>
        </div>
    </div>
</aside>

<!-- ============ TABLE AREA ============ -->
<div class="table-area" id="table-area">
    <div class="status" id="status-msg">
        Define one or more ratio columns on the left, then click <b>Compute</b>.
    </div>
</div>
</div><!-- /main -->

<script>
(function() {{
"use strict";

/* ---- State ---- */
let META = null;         // from /api/ratios/meta
let COLUMNS = [];        // user-defined ratio columns
let RESULT = null;       // last compute result {{columns, rows}}
let SORT_COL = null;
let SORT_ASC = true;
let RANGE_FILTER = null; // {{col, min, max}}

/* ---- Init ---- */
fetch("/api/ratios/meta")
  .then(r => r.json())
  .then(data => {{
      META = data;
      populateMethods();
  }})
  .catch(() => {{
      document.getElementById("status-msg").textContent = "Failed to load metadata.";
  }});

/* ---- Populate compound dropdown ---- */
function populateMethods() {{
    const sel = document.getElementById("sel-method");
    sel.innerHTML = '<option value="">-- select --</option>';
    META.methods.forEach(m => {{
        const opt = document.createElement("option");
        opt.value = m.method;
        opt.textContent = m.label;
        sel.appendChild(opt);
    }});
}}

/* ---- Populate param dropdowns when method changes ---- */
document.getElementById("sel-method").addEventListener("change", function() {{
    const method = this.value;
    const selA = document.getElementById("sel-param-a");
    const selB = document.getElementById("sel-param-b");
    selA.innerHTML = "";
    selB.innerHTML = "";
    if (!method) {{
        selA.disabled = true;
        selB.disabled = true;
        selA.innerHTML = '<option value="">Select compound first</option>';
        selB.innerHTML = '<option value="">Select compound first</option>';
        return;
    }}
    const info = META.methods.find(m => m.method === method);
    if (!info) return;
    selA.disabled = false;
    selB.disabled = false;
    info.columns.forEach(c => {{
        const optA = document.createElement("option");
        optA.value = c.key;
        optA.textContent = c.label;
        selA.appendChild(optA);
        const optB = document.createElement("option");
        optB.value = c.key;
        optB.textContent = c.label;
        selB.appendChild(optB);
    }});
    // Default B to second column if available
    if (info.columns.length > 1) selB.selectedIndex = 1;
}});

/* ---- Add column ---- */
document.getElementById("btn-add-col").addEventListener("click", function() {{
    const formula = document.getElementById("sel-formula").value;
    const method = document.getElementById("sel-method").value;
    const paramA = document.getElementById("sel-param-a").value;
    const paramB = document.getElementById("sel-param-b").value;
    if (!method || !paramA || !paramB) {{
        alert("Please select a compound group and both parameters.");
        return;
    }}
    const info = META.methods.find(m => m.method === method);
    const lblA = (info && info.columns.find(c => c.key === paramA) || {{}}).label || paramA;
    const lblB = (info && info.columns.find(c => c.key === paramB) || {{}}).label || paramB;
    const dispFormula = formula.replace(/a/g, lblA).replace(/b/g, lblB);
    const label = (info ? info.label : method) + ": " + dispFormula;
    COLUMNS.push({{ formula, method, param_a: paramA, param_b: paramB, label }});
    renderColList();
    updateFilterColDropdown();
}});

/* ---- Render column list ---- */
function renderColList() {{
    const el = document.getElementById("col-list");
    if (!COLUMNS.length) {{
        el.innerHTML = '<span style="color:#94a3b8;font-size:0.8rem;">No columns defined yet.</span>';
        return;
    }}
    el.innerHTML = COLUMNS.map((c, i) => {{
        const info = META.methods.find(m => m.method === c.method);
        const tag = info ? info.label : c.method;
        return '<div class="ratio-col-item">' +
            '<span class="tag">' + esc(tag) + '</span> ' +
            '<span class="formula">' + esc(c.formula) +
            ' &nbsp; A=' + esc(c.param_a) + ' &nbsp; B=' + esc(c.param_b) + '</span>' +
            '<span class="remove-col" data-idx="' + i + '">&times;</span>' +
            '</div>';
    }}).join("");
    el.querySelectorAll(".remove-col").forEach(btn => {{
        btn.addEventListener("click", function() {{
            COLUMNS.splice(+this.dataset.idx, 1);
            renderColList();
            updateFilterColDropdown();
        }});
    }});
}}

/* ---- Clear all columns ---- */
document.getElementById("btn-clear-cols").addEventListener("click", function() {{
    COLUMNS = [];
    RESULT = null;
    renderColList();
    updateFilterColDropdown();
    document.getElementById("table-area").innerHTML =
        '<div class="status">Define one or more ratio columns on the left, then click <b>Compute</b>.</div>';
}});

/* ---- Compute ---- */
document.getElementById("btn-compute").addEventListener("click", compute);
function compute() {{
    if (!COLUMNS.length) {{
        alert("Add at least one ratio column first.");
        return;
    }}
    const statusEl = document.getElementById("table-area");
    statusEl.innerHTML = '<div class="status">Computing\u2026</div>';
    fetch("/api/ratios/compute", {{
        method: "POST",
        headers: {{ "Content-Type": "application/json" }},
        body: JSON.stringify({{ columns: COLUMNS }}),
    }})
    .then(r => r.json())
    .then(data => {{
        RESULT = data;
        SORT_COL = null;
        SORT_ASC = true;
        RANGE_FILTER = null;
        renderTable();
    }})
    .catch(e => {{
        statusEl.innerHTML = '<div class="status" style="color:#dc2626">Error: ' + esc(String(e)) + '</div>';
    }});
}}

/* ---- Render table ---- */
function renderTable() {{
    if (!RESULT || !RESULT.rows.length) {{
        document.getElementById("table-area").innerHTML =
            '<div class="status">No data returned. Check that the selected compounds have measurements.</div>';
        return;
    }}
    let rows = RESULT.rows.slice();

    // Range filter
    if (RANGE_FILTER && RANGE_FILTER.col) {{
        rows = rows.filter(r => {{
            const v = r[RANGE_FILTER.col];
            if (v == null) return false;
            if (RANGE_FILTER.min != null && v < RANGE_FILTER.min) return false;
            if (RANGE_FILTER.max != null && v > RANGE_FILTER.max) return false;
            return true;
        }});
    }}

    // Sort
    if (SORT_COL) {{
        rows.sort((a, b) => {{
            let va = a[SORT_COL], vb = b[SORT_COL];
            if (va == null && vb == null) return 0;
            if (va == null) return 1;
            if (vb == null) return -1;
            if (typeof va === "number" && typeof vb === "number") return SORT_ASC ? va - vb : vb - va;
            va = String(va); vb = String(vb);
            return SORT_ASC ? va.localeCompare(vb) : vb.localeCompare(va);
        }});
    }}

    const cols = RESULT.columns;
    let html = '<table><thead><tr>';
    cols.forEach(c => {{
        const ind = SORT_COL === c.id ? (SORT_ASC ? " \u25b2" : " \u25bc") : "";
        html += '<th data-col="' + esc(c.id) + '">' + esc(c.label) +
                '<span class="sort-ind">' + ind + '</span></th>';
    }});
    html += '</tr></thead><tbody>';
    rows.forEach(r => {{
        html += '<tr>';
        cols.forEach(c => {{
            const v = r[c.id];
            if (v == null) {{
                html += '<td class="null">\u2014</td>';
            }} else if (typeof v === "number") {{
                html += '<td class="num">' + formatNum(v) + '</td>';
            }} else {{
                html += '<td>' + esc(String(v)) + '</td>';
            }}
        }});
        html += '</tr>';
    }});
    html += '</tbody></table>';
    html += '<div class="status">' + rows.length + ' of ' + RESULT.rows.length + ' samples shown</div>';
    document.getElementById("table-area").innerHTML = html;

    // Sortable headers
    document.querySelectorAll(".table-area th").forEach(th => {{
        th.addEventListener("click", function() {{
            const col = this.dataset.col;
            if (SORT_COL === col) {{ SORT_ASC = !SORT_ASC; }}
            else {{ SORT_COL = col; SORT_ASC = true; }}
            renderTable();
        }});
    }});
}}

/* ---- Filter column dropdown ---- */
function updateFilterColDropdown() {{
    const sel = document.getElementById("filter-col");
    sel.innerHTML = '<option value="">-- select column --</option>';
    COLUMNS.forEach((c, i) => {{
        const opt = document.createElement("option");
        opt.value = "ratio_" + i;
        opt.textContent = c.label;
        sel.appendChild(opt);
    }});
}}

/* ---- Apply range filter ---- */
document.getElementById("btn-apply-range").addEventListener("click", function() {{
    const col = document.getElementById("filter-col").value;
    if (!col) {{ alert("Select a column to filter."); return; }}
    const minVal = document.getElementById("filter-min").value;
    const maxVal = document.getElementById("filter-max").value;
    RANGE_FILTER = {{
        col: col,
        min: minVal !== "" ? parseFloat(minVal) : null,
        max: maxVal !== "" ? parseFloat(maxVal) : null,
    }};
    if (RESULT) renderTable();
}});

document.getElementById("btn-clear-range").addEventListener("click", function() {{
    document.getElementById("filter-col").value = "";
    document.getElementById("filter-min").value = "";
    document.getElementById("filter-max").value = "";
    RANGE_FILTER = null;
    if (RESULT) renderTable();
}});

/* ---- CSV export ---- */
document.getElementById("btn-export-csv").addEventListener("click", function() {{
    if (!RESULT || !RESULT.rows.length) {{ alert("Compute results first."); return; }}
    const cols = RESULT.columns;
    let csv = cols.map(c => '"' + c.label.replace(/"/g, '""') + '"').join(",") + "\\n";
    RESULT.rows.forEach(r => {{
        csv += cols.map(c => {{
            const v = r[c.id];
            if (v == null) return "";
            return '"' + String(v).replace(/"/g, '""') + '"';
        }}).join(",") + "\\n";
    }});
    const blob = new Blob([csv], {{ type: "text/csv" }});
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "ratios.csv";
    a.click();
}});

/* ---- Helpers ---- */
function esc(s) {{ const d = document.createElement("div"); d.textContent = s; return d.innerHTML; }}
function formatNum(v) {{
    if (Number.isInteger(v)) return String(v);
    if (Math.abs(v) < 0.001 || Math.abs(v) >= 1e6) return v.toExponential(4);
    return v.toPrecision(6);
}}

}})();
</script>
</body>
</html>"""
    return HTMLResponse(html)
