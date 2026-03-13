"""Upload Data page – ingest user-uploaded CSV files into the database."""
from __future__ import annotations

import logging
import os
from html import escape
from typing import Any

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse

from api.shared import (
    ALLOWED_TABLES,
    LAB_METHODS,
    METHOD_GROUPS,
    canonical_table_name,
    method_label,
)
from db_code.services.special_ingest import FT_MODES, ISOTOPE_TABLE_BY_KIND
from db_code.services.upload_service import (
    expected_columns,
    ingest_ft_upload,
    ingest_gc_upload,
    ingest_isotope_upload,
    parse_gc_upload,
    parse_ft_upload,
    parse_isotope_upload,
)

router = APIRouter()
logger = logging.getLogger(__name__)

_GC_TABLES = sorted(ALLOWED_TABLES - LAB_METHODS)


# ---------------------------------------------------------------------------
# /api/upload/meta – available types, subtypes, and expected columns
# ---------------------------------------------------------------------------

@router.get("/api/upload/meta")
def upload_meta(request: Request):
    """Return the upload target taxonomy: data families, subtypes, and their
    current DB column names (so the UI can show a mapping preview).
    """
    gc_methods: list[dict] = []
    for table in _GC_TABLES:
        try:
            cols = expected_columns(table)
        except Exception:
            cols = []
        gc_methods.append({
            "table": table,
            "label": method_label(table),
            "columns": cols,
        })

    # Order GC methods same as METHOD_GROUPS
    gc_group = next((g for g in METHOD_GROUPS if g["label"] == "GC Compounds"), None)
    gc_order = gc_group["methods"] if gc_group else []
    gc_methods.sort(key=lambda m: (
        gc_order.index(m["table"]) if m["table"] in gc_order else 999,
        m["table"],
    ))

    ft_modes = [{"key": k, "label": v} for k, v in FT_MODES.items()]

    isotope_kinds = [
        {"key": k, "label": method_label(v), "table": v}
        for k, v in ISOTOPE_TABLE_BY_KIND.items()
    ]

    return {
        "gc": gc_methods,
        "ft": ft_modes,
        "isotope": isotope_kinds,
    }


# ---------------------------------------------------------------------------
# /api/upload/preview – dry-run parse (no DB write)
# ---------------------------------------------------------------------------

@router.post("/api/upload/preview")
async def upload_preview(
    file: UploadFile = File(...),
    data_family: str = Form(...),
    subtype: str = Form(...),
    operator: str = Form("unknown"),
    instrument: str = Form(""),
    data_type_tag: str = Form("Area"),
):
    """Parse the uploaded CSV and return a preview without writing to DB."""
    raw = await file.read()
    csv_text = raw.decode("utf-8-sig", errors="replace")
    filename = file.filename or "upload.csv"

    try:
        if data_family == "gc":
            table = canonical_table_name(subtype)
            if table not in ALLOWED_TABLES or table in LAB_METHODS:
                return JSONResponse({"ok": False, "error": f"Unknown GC table: {subtype}"}, 400)
            rows = parse_gc_upload(
                csv_text, table,
                instrument=instrument or None,
                data_type=data_type_tag or "Area",
            )
            sample_count = len(rows)
            entry_count = sum(len(r[table].get("entries", [])) for r in rows)
            columns: set[str] = set()
            for r in rows:
                for e in r[table].get("entries", []):
                    if isinstance(e.get("data"), dict):
                        columns.update(e["data"].keys())
            return {
                "ok": True,
                "family": "gc",
                "table": table,
                "samples": sample_count,
                "entries": entry_count,
                "columns_found": sorted(columns),
            }

        elif data_family == "ft":
            mode = subtype
            rows = parse_ft_upload(csv_text, mode, operator, filename)
            sample_count = len(rows)
            summary = {}
            if rows:
                entries = rows[0].get(FT_MODES and "ft_icr_ms", {}).get("entries", [])
                if entries:
                    summary = entries[0].get("data", {})
            return {
                "ok": True,
                "family": "ft",
                "table": "ft_icr_ms",
                "samples": sample_count,
                "entries": sample_count,
                "summary": summary,
            }

        elif data_family == "isotope":
            table, rows = parse_isotope_upload(csv_text, subtype, operator)
            sample_count = len(rows)
            entry_count = sum(len(r[table].get("entries", [])) for r in rows)
            columns_found: set[str] = set()
            for r in rows:
                for e in r[table].get("entries", []):
                    if isinstance(e.get("data"), dict):
                        columns_found.update(e["data"].keys())
            return {
                "ok": True,
                "family": "isotope",
                "table": table,
                "samples": sample_count,
                "entries": entry_count,
                "columns_found": sorted(columns_found),
            }
        else:
            return JSONResponse({"ok": False, "error": f"Unknown data family: {data_family}"}, 400)

    except Exception as exc:
        logger.exception("Upload preview failed")
        return JSONResponse({"ok": False, "error": str(exc)}, 400)


# ---------------------------------------------------------------------------
# /api/upload/ingest – parse + write to DB
# ---------------------------------------------------------------------------

@router.post("/api/upload/ingest")
async def upload_ingest(
    file: UploadFile = File(...),
    data_family: str = Form(...),
    subtype: str = Form(...),
    operator: str = Form("unknown"),
    instrument: str = Form(""),
    data_type_tag: str = Form("Area"),
):
    """Parse the uploaded CSV and ingest into the database."""
    raw = await file.read()
    csv_text = raw.decode("utf-8-sig", errors="replace")
    filename = file.filename or "upload.csv"

    try:
        if data_family == "gc":
            table = canonical_table_name(subtype)
            if table not in ALLOWED_TABLES or table in LAB_METHODS:
                return JSONResponse({"ok": False, "error": f"Unknown GC table: {subtype}"}, 400)
            count = ingest_gc_upload(
                csv_text, table,
                instrument=instrument or None,
                data_type=data_type_tag or "Area",
            )
            return {"ok": True, "family": "gc", "table": table, "upserted": count}

        elif data_family == "ft":
            count = ingest_ft_upload(csv_text, subtype, operator, filename)
            return {"ok": True, "family": "ft", "table": "ft_icr_ms", "upserted": count}

        elif data_family == "isotope":
            count = ingest_isotope_upload(csv_text, subtype, operator)
            table = ISOTOPE_TABLE_BY_KIND.get(subtype, subtype)
            return {"ok": True, "family": "isotope", "table": table, "upserted": count}

        else:
            return JSONResponse({"ok": False, "error": f"Unknown data family: {data_family}"}, 400)

    except Exception as exc:
        logger.exception("Upload ingest failed")
        return JSONResponse({"ok": False, "error": str(exc)}, 422)


# ---------------------------------------------------------------------------
# /web/upload – HTML page
# ---------------------------------------------------------------------------

@router.get("/web/upload", response_class=HTMLResponse)
def upload_html():
    title = "OrgChem \u2013 Upload Data"

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
.main {{
    display: grid;
    grid-template-columns: 380px 1fr;
    height: calc(100vh - 52px);
}}
@media (max-width: 900px) {{
    .main {{ grid-template-columns: 1fr; }}
    .sidebar {{ max-height: 45vh; }}
}}
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
select, input[type="text"] {{
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
.drop-zone {{
    border: 2px dashed #cbd5e1;
    border-radius: 6px;
    padding: 1.5rem 0.75rem;
    text-align: center;
    color: #64748b;
    font-size: 0.82rem;
    cursor: pointer;
    transition: border-color 0.15s, background 0.15s;
}}
.drop-zone.dragover {{
    border-color: #2563eb;
    background: #eff6ff;
}}
.drop-zone.has-file {{
    border-color: #16a34a;
    background: #f0fdf4;
    color: #15803d;
}}
.file-info {{
    font-size: 0.78rem; color: #64748b; margin-top: 0.3rem;
}}
.content {{
    overflow: auto; padding: 1rem;
}}
.preview-table {{
    border-collapse: collapse; width: max-content; min-width: 100%;
    font-size: 0.82rem;
}}
.preview-table th, .preview-table td {{
    border: 1px solid #e5e7eb; padding: 0.3rem 0.5rem;
    text-align: left; white-space: nowrap;
}}
.preview-table th {{
    background: #f1f5f9; font-weight: 600;
    position: sticky; top: 0; z-index: 1;
}}
.preview-table tbody tr:nth-child(even) {{ background: #f9fafb; }}
.status {{
    padding: 0.5rem 0.75rem; font-size: 0.82rem; color: #64748b;
}}
.status.ok {{ color: #16a34a; }}
.status.err {{ color: #dc2626; }}
.col-badge {{
    display: inline-block; background: #e0e7ff; color: #3730a3;
    padding: 0.1rem 0.4rem; border-radius: 3px; font-size: 0.72rem;
    font-weight: 600; margin: 0.15rem 0.1rem;
}}
.col-badge.new {{ background: #fef3c7; color: #92400e; }}
.col-badge.match {{ background: #d1fae5; color: #065f46; }}
.hidden {{ display: none; }}
</style>
</head>
<body>
<div class="topbar">
    <h1>{escape(title)}</h1>
    <div class="topbar-links">
        <a href="/web/presence">Presence matrix</a>
        <a href="/web/matrix">Methods index</a>
        <a href="/web/explorer">Data Explorer</a>
        <a href="/web/ratios">Calculate Ratios</a>
        <a href="/web/samples/filter">Sample filters</a>
    </div>
</div>
<div class="main">
<!-- ============ SIDEBAR ============ -->
<aside class="sidebar" id="sidebar">
    <!-- 1. Choose data type -->
    <div class="panel">
        <div class="panel-title">1. Data Family</div>
        <label class="field">
            <span>Type</span>
            <select id="sel-family">
                <option value="">-- select --</option>
                <option value="gc">GC Compounds</option>
                <option value="ft">FT-ICR-MS</option>
                <option value="isotope">Isotopes</option>
            </select>
        </label>
        <label class="field" id="lbl-subtype">
            <span>Subtype</span>
            <select id="sel-subtype" disabled>
                <option value="">Select type first</option>
            </select>
        </label>
    </div>

    <!-- 2. Extra metadata -->
    <div class="panel" id="panel-meta">
        <div class="panel-title">2. Metadata</div>
        <label class="field" id="lbl-instrument">
            <span>Instrument</span>
            <input type="text" id="inp-instrument" placeholder="e.g. GCFID, GCMS"/>
        </label>
        <label class="field" id="lbl-datatype">
            <span>Data type</span>
            <select id="sel-datatype">
                <option value="Area">Area</option>
                <option value="Concentration">Concentration</option>
            </select>
        </label>
        <label class="field" id="lbl-operator">
            <span>Operator</span>
            <input type="text" id="inp-operator" placeholder="Your name"/>
        </label>
    </div>

    <!-- 3. File upload -->
    <div class="panel">
        <div class="panel-title">3. Upload CSV</div>
        <div class="drop-zone" id="drop-zone">
            Drop a CSV file here or click to browse
            <input type="file" id="file-input" accept=".csv,.txt" class="hidden"/>
        </div>
        <div class="file-info" id="file-info"></div>
    </div>

    <!-- 4. Actions -->
    <div class="panel">
        <div class="panel-title">4. Actions</div>
        <div class="actions">
            <button class="btn btn-outline" id="btn-preview" disabled>Preview</button>
            <button class="btn btn-primary" id="btn-ingest" disabled>Upload to DB</button>
        </div>
    </div>
</aside>

<!-- ============ CONTENT ============ -->
<section class="content" id="content">
    <div class="status" id="status-msg">
        Select a data type, choose a subtype, and upload a CSV file to begin.
    </div>
</section>
</div><!-- /main -->

<script>
(function() {{
"use strict";

/* ---- State ---- */
let META = null;
let FILE_TEXT = null;
let FILE_OBJ = null;

/* ---- Init ---- */
fetch("/api/upload/meta")
  .then(r => r.json())
  .then(data => {{ META = data; }})
  .catch(() => {{
      document.getElementById("status-msg").textContent = "Failed to load metadata.";
  }});

const esc = s => {{
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
}};

/* ---- Family selector ---- */
const selFamily = document.getElementById("sel-family");
const selSubtype = document.getElementById("sel-subtype");

selFamily.addEventListener("change", function() {{
    const fam = this.value;
    selSubtype.innerHTML = "";
    selSubtype.disabled = true;
    updateMetaVisibility(fam);
    updateButtons();

    if (!fam || !META) {{
        selSubtype.innerHTML = '<option value="">Select type first</option>';
        return;
    }}

    selSubtype.disabled = false;
    selSubtype.innerHTML = '<option value="">-- select --</option>';

    if (fam === "gc") {{
        META.gc.forEach(m => {{
            const opt = document.createElement("option");
            opt.value = m.table;
            opt.textContent = m.label;
            selSubtype.appendChild(opt);
        }});
    }} else if (fam === "ft") {{
        META.ft.forEach(m => {{
            const opt = document.createElement("option");
            opt.value = m.key;
            opt.textContent = m.label;
            selSubtype.appendChild(opt);
        }});
    }} else if (fam === "isotope") {{
        META.isotope.forEach(m => {{
            const opt = document.createElement("option");
            opt.value = m.key;
            opt.textContent = m.label;
            selSubtype.appendChild(opt);
        }});
    }}
}});

selSubtype.addEventListener("change", function() {{
    updateButtons();
    showExpectedColumns();
}});

/* ---- Show/hide instrument/datatype fields based on family ---- */
function updateMetaVisibility(fam) {{
    // Instrument and datatype only relevant for GC
    document.getElementById("lbl-instrument").style.display = fam === "gc" ? "" : "none";
    document.getElementById("lbl-datatype").style.display = fam === "gc" ? "" : "none";
    // Operator for FT and isotope
    document.getElementById("lbl-operator").style.display = (fam === "ft" || fam === "isotope") ? "" : "none";
}}

/* ---- Show expected columns when subtype is selected ---- */
function showExpectedColumns() {{
    const fam = selFamily.value;
    const sub = selSubtype.value;
    if (!META || !fam || !sub) return;

    const content = document.getElementById("content");
    let cols = [];

    if (fam === "gc") {{
        const info = META.gc.find(m => m.table === sub);
        if (info) cols = info.columns;
    }} else if (fam === "ft") {{
        // FT has fixed summary columns
        cols = ["peak_count", "source_file", "min_signal_to_noise", "max_signal_to_noise", "min_mass", "max_mass"];
    }} else if (fam === "isotope") {{
        const info = META.isotope.find(m => m.key === sub);
        if (info && info.table) {{
            // Try to find columns from meta — will be populated after first ingest
            cols = [];
        }}
    }}

    if (cols.length > 0) {{
        let html = '<div class="status">Expected columns for <b>' + esc(sub) + '</b>:</div>';
        html += '<div style="padding: 0.5rem 0.75rem;">';
        cols.forEach(c => {{
            html += '<span class="col-badge match">' + esc(c) + '</span> ';
        }});
        html += '</div>';
        content.innerHTML = html;
    }} else {{
        content.innerHTML = '<div class="status">Select a subtype and upload a CSV to see a preview.</div>';
    }}
}}

/* ---- File drag & drop + click ---- */
const dropZone = document.getElementById("drop-zone");
const fileInput = document.getElementById("file-input");
const fileInfo = document.getElementById("file-info");

dropZone.addEventListener("click", () => fileInput.click());
dropZone.addEventListener("dragover", e => {{
    e.preventDefault();
    dropZone.classList.add("dragover");
}});
dropZone.addEventListener("dragleave", () => dropZone.classList.remove("dragover"));
dropZone.addEventListener("drop", e => {{
    e.preventDefault();
    dropZone.classList.remove("dragover");
    if (e.dataTransfer.files.length) handleFile(e.dataTransfer.files[0]);
}});
fileInput.addEventListener("change", () => {{
    if (fileInput.files.length) handleFile(fileInput.files[0]);
}});

function handleFile(f) {{
    if (!f.name.toLowerCase().endsWith(".csv") && !f.name.toLowerCase().endsWith(".txt")) {{
        alert("Please upload a .csv or .txt file.");
        return;
    }}
    FILE_OBJ = f;
    const reader = new FileReader();
    reader.onload = () => {{
        FILE_TEXT = reader.result;
        dropZone.classList.add("has-file");
        dropZone.textContent = f.name;
        fileInfo.textContent = (f.size / 1024).toFixed(1) + " KB";
        updateButtons();
    }};
    reader.readAsText(f);
}}

/* ---- Button state ---- */
function updateButtons() {{
    const ready = selFamily.value && selSubtype.value && FILE_OBJ;
    document.getElementById("btn-preview").disabled = !ready;
    document.getElementById("btn-ingest").disabled = !ready;
}}

/* ---- Preview ---- */
document.getElementById("btn-preview").addEventListener("click", doPreview);
function doPreview() {{
    if (!FILE_OBJ) return;
    const content = document.getElementById("content");
    content.innerHTML = '<div class="status">Parsing\u2026</div>';

    const fd = buildFormData();
    fetch("/api/upload/preview", {{ method: "POST", body: fd }})
      .then(r => r.json())
      .then(data => {{
          if (!data.ok) {{
              content.innerHTML = '<div class="status err">Error: ' + esc(data.error) + '</div>';
              return;
          }}
          renderPreview(data);
      }})
      .catch(err => {{
          content.innerHTML = '<div class="status err">Request failed: ' + esc(String(err)) + '</div>';
      }});
}}

function renderPreview(data) {{
    const content = document.getElementById("content");
    let html = '<div class="status ok">' +
        'Parsed <b>' + data.samples + '</b> sample(s), ' +
        '<b>' + data.entries + '</b> entry(ies) for table <b>' + esc(data.table) + '</b>.' +
        '</div>';

    // Show column mapping if GC or isotope
    if (data.columns_found && data.columns_found.length) {{
        // Try to match against expected columns
        let expected = [];
        const fam = selFamily.value;
        const sub = selSubtype.value;
        if (fam === "gc" && META) {{
            const info = META.gc.find(m => m.table === sub);
            if (info) expected = info.columns;
        }}

        const expectedSet = new Set(expected);
        html += '<div style="padding: 0.5rem 0.75rem;">';
        html += '<b>Columns found (' + data.columns_found.length + '):</b><br/>';
        data.columns_found.forEach(c => {{
            const cls = expectedSet.size > 0
                ? (expectedSet.has(c) ? "col-badge match" : "col-badge new")
                : "col-badge";
            html += '<span class="' + cls + '">' + esc(c) + '</span> ';
        }});
        if (expectedSet.size > 0) {{
            html += '<br/><span style="font-size:0.72rem;color:#64748b;">' +
                '<span class="col-badge match">green</span> = matches existing column, ' +
                '<span class="col-badge new">yellow</span> = new column</span>';
        }}
        html += '</div>';
    }}

    // FT summary
    if (data.summary) {{
        html += '<div style="padding: 0.5rem 0.75rem;"><b>Summary:</b><br/>';
        for (const [k, v] of Object.entries(data.summary)) {{
            html += '<span class="col-badge">' + esc(k) + ': ' + esc(String(v)) + '</span> ';
        }}
        html += '</div>';
    }}

    content.innerHTML = html;
}}

/* ---- Ingest ---- */
document.getElementById("btn-ingest").addEventListener("click", doIngest);
function doIngest() {{
    if (!FILE_OBJ) return;
    if (!confirm("Upload and ingest this file into the database?")) return;

    const content = document.getElementById("content");
    content.innerHTML = '<div class="status">Uploading\u2026</div>';

    const fd = buildFormData();
    fetch("/api/upload/ingest", {{ method: "POST", body: fd }})
      .then(r => r.json())
      .then(data => {{
          if (!data.ok) {{
              content.innerHTML = '<div class="status err">Error: ' + esc(data.error) + '</div>';
              return;
          }}
          content.innerHTML = '<div class="status ok">' +
              'Successfully ingested <b>' + data.upserted + '</b> row(s) into ' +
              '<b>' + esc(data.table) + '</b>.</div>';
      }})
      .catch(err => {{
          content.innerHTML = '<div class="status err">Request failed: ' + esc(String(err)) + '</div>';
      }});
}}

/* ---- Build FormData ---- */
function buildFormData() {{
    const fd = new FormData();
    fd.append("file", FILE_OBJ);
    fd.append("data_family", selFamily.value);
    fd.append("subtype", selSubtype.value);
    fd.append("operator", document.getElementById("inp-operator").value || "unknown");
    fd.append("instrument", document.getElementById("inp-instrument").value || "");
    fd.append("data_type_tag", document.getElementById("sel-datatype").value || "Area");
    return fd;
}}

/* ---- Initial visibility ---- */
updateMetaVisibility("");

}})();
</script>
</body>
</html>"""

    return HTMLResponse(html)
