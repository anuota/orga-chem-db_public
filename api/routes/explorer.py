from __future__ import annotations

import csv
import json
import os
from html import escape
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from api.shared import (
    ALLOWED_TABLES,
    MATRIX_META_FIELDS,
    MATRIX_META_LABELS,
    METHOD_GROUPS,
    canonical_table_name,
    method_label,
    run_query_with_rls,
)

router = APIRouter()

# ===========================================================================
# Data Explorer – user-defined column selection + metadata filters
# ===========================================================================

@router.get("/api/explorer/meta")
def explorer_meta(request: Request):
    """Return metadata for the explorer page:
    - methods: list of method names with their available parameter columns
    - filters: distinct values for metadata fields
    - metadata_fields: list of per-entry metadata fields available for display
    """
    methods_info: list[dict] = []

    # Collect distinct filter values across ALL methods in one query (not N+1)
    all_measured_by: set[str] = set()
    all_names: set[str] = set()
    all_types: set[str] = set()
    all_dates: set[str] = set()
    all_fractions: set[str] = set()
    all_instruments: set[str] = set()
    all_data_types: set[str] = set()

    methods_sorted = sorted(ALLOWED_TABLES)

    # --- Batch 1: distinct filter values (single UNION ALL instead of 25 queries) ---
    filter_parts = [
        f"SELECT name, measured_by, type, date, fraction, instrument, data_type "
        f"FROM public.{m}_entries"
        for m in methods_sorted
    ]
    if filter_parts:
        filter_sql = " UNION ALL ".join(filter_parts)
        try:
            _, filter_rows = run_query_with_rls(filter_sql, request)
            for r in filter_rows:
                mb = r.get("measured_by")
                nm = r.get("name")
                tp = r.get("type")
                dt = r.get("date")
                fr = r.get("fraction")
                inst = r.get("instrument")
                dtype = r.get("data_type")
                if mb:
                    all_measured_by.add(str(mb))
                if nm:
                    all_names.add(str(nm))
                if tp:
                    all_types.add(str(tp))
                if dt:
                    all_dates.add(str(dt))
                if fr:
                    all_fractions.add(str(fr))
                if inst:
                    all_instruments.add(str(inst))
                if dtype:
                    all_data_types.add(str(dtype))
        except Exception:
            pass

    # --- Batch 2: parameter column keys per method (single UNION ALL) ---
    keys_parts = [
        f"SELECT '{m}'::text AS method, jsonb_object_keys(data) AS key "
        f"FROM public.{m}_entries"
        for m in methods_sorted
    ]
    method_keys: dict[str, set[str]] = {m: set() for m in methods_sorted}
    if keys_parts:
        keys_sql = (
            "SELECT method, key FROM ("
            + " UNION ALL ".join(keys_parts)
            + ") _sub GROUP BY method, key ORDER BY method, key"
        )
        try:
            _, keys_rows = run_query_with_rls(keys_sql, request)
            for r in keys_rows:
                m = r["method"]
                if m in method_keys:
                    method_keys[m].add(r["key"])
        except Exception:
            pass

    for m in methods_sorted:
        pk = method_keys.get(m, set())
        # Only include methods that actually have data (keys)
        if pk:
            methods_info.append({
                "method": m,
                "label": method_label(m),
                "columns": sorted(pk),
            })

    # Collect all sample numbers from presence view
    all_samplenumbers: list[str] = []
    try:
        _, sn_rows = run_query_with_rls(
            "SELECT samplenumber FROM public.analysis_presence_simple ORDER BY samplenumber",
            request,
        )
        all_samplenumbers = [str(r["samplenumber"]) for r in sn_rows if r.get("samplenumber")]
    except Exception:
        pass

    # Try to get project list
    all_projects: list[str] = []
    try:
        _, proj_rows = run_query_with_rls(
            "SELECT DISTINCT project_id FROM public.sample_projects ORDER BY project_id",
            request,
        )
        all_projects = [str(r["project_id"]) for r in proj_rows if r.get("project_id")]
    except Exception:
        pass

    # Build method_groups with only methods that have data
    methods_by_name = {m["method"]: m for m in methods_info}
    grouped: list[dict] = []
    for grp in METHOD_GROUPS:
        children = [methods_by_name[m] for m in grp["methods"] if m in methods_by_name]
        if children:
            grouped.append({"label": grp["label"], "methods": children})

    return {
        "methods": methods_info,
        "method_groups": grouped,
        "filters": {
            "samplenumbers": all_samplenumbers,
            "name": sorted(all_names),
            "measured_by": sorted(all_measured_by),
            "type": sorted(all_types),
            "date": sorted(all_dates),
            "fraction": sorted(all_fractions),
            "instrument": sorted(all_instruments),
            "data_type": sorted(all_data_types),
            "project": all_projects,
        },
        "metadata_fields": list(MATRIX_META_FIELDS),
    }




class ExplorerQuery(BaseModel):
    """POST body for the explorer query endpoint."""
    methods: dict[str, list[str]]  # method_name -> list of selected column names (empty = all)
    filters: dict[str, Any] | None = None
    include_metadata_cols: list[str] | None = None
    # include_metadata_cols: list of meta field names to include per-group, e.g.
    #   ["instrument", "fraction", "data_type", "date", "measured_by"]
    # Default (None or absent) = all five shown.


# Canonical order for per-group metadata columns in the wide table
_META_FIELDS = list(MATRIX_META_FIELDS)
_META_LABELS = dict(MATRIX_META_LABELS)


def _flatten_param_value(v: Any) -> Any:
    if isinstance(v, dict):
        return v.get("value", v.get("orig", v))
    return v


def _data_signature(data: dict[str, Any]) -> str:
    flat: dict[str, Any] = {}
    for k, v in data.items():
        flat[str(k)] = _flatten_param_value(v)
    return json.dumps(flat, sort_keys=True, ensure_ascii=True, default=str)


def _merge_meta(existing: Any, incoming: Any) -> str:
    vals: list[str] = []
    for raw in (existing, incoming):
        if raw in (None, ""):
            continue
        txt = str(raw)
        if txt not in vals:
            vals.append(txt)
    return " | ".join(vals)


@router.post("/api/explorer/query")
def explorer_query(body: ExplorerQuery, request: Request):
    """Build and return a wide table based on user-selected methods/columns with filters.

    Supports multiple rows per sample (Option A): if a sample has multiple
    measurements (entries) for a compound group, each entry produces a separate
    row.  Rows are sorted by sample then date (newest first).
    """
    user = getattr(request.state, "user", os.getenv("DEV_USER", "open"))
    filters = body.filters or {}

    # Determine which metadata columns to include
    meta_cols = body.include_metadata_cols
    if meta_cols is None:
        meta_cols = list(_META_FIELDS)  # default: all
    # Filter to known fields and preserve canonical order
    meta_cols = [f for f in _META_FIELDS if f in meta_cols]

    sn_filter: list[str] = filters.get("samplenumber") or []
    filt_measured_by = filters.get("measured_by") or []
    filt_type = filters.get("type") or []
    filt_date_min = (filters.get("date_min") or "").strip()
    filt_date_max = (filters.get("date_max") or "").strip()
    filt_fraction = filters.get("fraction") or []
    filt_project = filters.get("project") or []
    filt_instrument = filters.get("instrument") or []
    filt_data_type = filters.get("data_type") or []

    # If project filter is set, resolve samples from sample_projects first
    project_samples: set[str] | None = None
    if filt_project:
        try:
            placeholders = ",".join(["%s"] * len(filt_project))
            _, proj_rows = run_query_with_rls(
                f"SELECT DISTINCT samplenumber FROM public.sample_projects WHERE project_id IN ({placeholders})",
                request,
                filt_project,
            )
            project_samples = {str(r["samplenumber"]) for r in proj_rows}
        except Exception:
            project_samples = set()

    # Resolve aliases in request methods (e.g. n_alkanes_isoprenoids -> alkanes)
    method_order: list[str] = []
    method_selected: dict[str, set[str] | None] = {}
    for req_method, selected_cols in body.methods.items():
        method = canonical_table_name(req_method)
        if method not in ALLOWED_TABLES:
            continue
        if method not in method_order:
            method_order.append(method)
        if not selected_cols:
            method_selected[method] = None
            continue
        if method in method_selected and method_selected[method] is None:
            continue
        bucket = method_selected.setdefault(method, set())
        if bucket is not None:
            bucket.update(selected_cols)

    # ----- Collect per-method entries, grouped by sample -----
    # method_entries[method] = {sn: [list of filtered entry dicts]}
    method_entries: dict[str, dict[str, list[dict]]] = {}
    # Track param keys per method for column registration
    method_param_keys: dict[str, set[str]] = {}

    for method in method_order:
        selected_spec = method_selected.get(method)
        view_name = f"public.{method}_entries"
        try:
            cols, rows = run_query_with_rls(
                f"SELECT samplenumber, name, measured_by, type, date, fraction, instrument, data_type, data FROM {view_name}",
                request,
            )
        except Exception:
            continue

        selected_set = set(selected_spec) if selected_spec else None
        entries_by_sn: dict[str, list[dict]] = {}
        all_keys: set[str] = set()

        for r in rows:
            sn = r.get("samplenumber")
            if not sn:
                continue

            # Apply metadata filters
            if sn_filter and str(sn) not in sn_filter:
                continue
            if project_samples is not None and sn not in project_samples:
                continue

            mb = r.get("measured_by") or ""
            nm = r.get("name") or ""
            tp = r.get("type") or ""
            dt = r.get("date") or ""
            fr = r.get("fraction") or ""
            inst = r.get("instrument") or ""
            dtype = r.get("data_type") or ""

            if filt_measured_by and str(mb) not in filt_measured_by:
                continue
            if filt_type and str(tp) not in filt_type:
                continue
            if filt_fraction and str(fr) not in filt_fraction:
                continue
            if filt_instrument and str(inst) not in filt_instrument:
                continue
            if filt_data_type and str(dtype) not in filt_data_type:
                continue
            if filt_date_min and str(dt) < filt_date_min:
                continue
            if filt_date_max and str(dt) > filt_date_max:
                continue

            data = r.get("data") or {}
            if not isinstance(data, dict):
                continue

            # Collect param keys
            for k in data.keys():
                if selected_set is None or k in selected_set:
                    all_keys.add(k)

            entries_by_sn.setdefault(sn, []).append({
                "measured_by": mb,
                "name": nm,
                "date": dt,
                "fraction": fr,
                "instrument": inst,
                "data_type": dtype,
                "data": data,
            })

        # Collapse duplicate entries where parameter values are identical and
        # only metadata differs (e.g., duplicated hopanes rows by instrument).
        for sn, entries in list(entries_by_sn.items()):
            seen_by_sig: dict[str, dict] = {}
            order: list[str] = []
            for entry in entries:
                data = entry.get("data") or {}
                if not isinstance(data, dict):
                    continue
                sig = _data_signature(data)
                if sig not in seen_by_sig:
                    seen_by_sig[sig] = dict(entry)
                    order.append(sig)
                    continue

                base = seen_by_sig[sig]
                for mf in ("measured_by", "name", "date", "fraction", "instrument", "data_type"):
                    base[mf] = _merge_meta(base.get(mf), entry.get(mf))

            entries_by_sn[sn] = [seen_by_sig[sig] for sig in order]

        method_entries[method] = entries_by_sn
        method_param_keys[method] = all_keys

    # ----- Build ordered column list -----
    ordered_columns: list[dict] = []
    for method in method_order:
        if method not in method_entries:
            continue
        # Metadata columns first (if requested)
        for mf in meta_cols:
            col_id = f"{method}__meta__{mf}"
            ordered_columns.append({
                "id": col_id,
                "label": _META_LABELS.get(mf, mf),
                "method": method,
                "is_meta": True,
            })
        # Then parameter columns
        selected_spec = method_selected.get(method)
        selected_set = set(selected_spec) if selected_spec else None
        if selected_set:
            keys = sorted(selected_set & method_param_keys.get(method, set()))
        else:
            keys = sorted(method_param_keys.get(method, set()))
        for k in keys:
            col_id = f"{method}__{k}"
            ordered_columns.append({"id": col_id, "label": k, "method": method, "is_meta": False})

    # ----- Build rows: Option A — one row per (sample, entry_index) -----
    # Determine all samples and max entries per sample across methods
    all_samples: set[str] = set()
    sample_max_entries: dict[str, int] = {}
    for method, ebs in method_entries.items():
        for sn, entries in ebs.items():
            all_samples.add(sn)
            sample_max_entries[sn] = max(sample_max_entries.get(sn, 0), len(entries))

    out_rows: list[dict] = []
    for sn in sorted(all_samples):
        n_entries = sample_max_entries.get(sn, 1)
        for idx in range(n_entries):
            row: dict[str, Any] = {"samplenumber": sn}
            for method in method_order:
                if method not in method_entries:
                    continue
                entries = method_entries[method].get(sn, [])
                entry = entries[idx] if idx < len(entries) else None

                # Metadata columns
                for mf in meta_cols:
                    col_id = f"{method}__meta__{mf}"
                    row[col_id] = (entry.get(mf) or "") if entry else ""

                # Parameter columns
                selected_spec = method_selected.get(method)
                selected_set = set(selected_spec) if selected_spec else None
                if entry:
                    data = entry.get("data") or {}
                    for k, v in data.items():
                        if selected_set and k not in selected_set:
                            continue
                        col_id = f"{method}__{k}"
                        flat_v = v
                        if isinstance(v, dict):
                            flat_v = v.get("value", v.get("orig", str(v)))
                        row[col_id] = flat_v

            out_rows.append(row)

    out_columns = [{"id": "samplenumber", "label": "Sample", "method": ""}] + ordered_columns
    return {"columns": out_columns, "rows": out_rows, "total": len(out_rows)}


@router.get("/web/explorer", response_class=HTMLResponse)
def explorer_html():
    """Data Explorer page — user-defined column selection with metadata filters."""
    title = "OrgChem – Data Explorer"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
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
            display: flex;
            align-items: center;
            gap: 1rem;
            flex-wrap: wrap;
            justify-content: space-between;
        }}
        .topbar h1 {{ margin: 0; font-size: 1.25rem; white-space: nowrap; }}
        .topbar-links {{ display: flex; gap: 0.75rem; font-size: 0.85rem; }}
        .topbar-links a {{ color: #2563eb; text-decoration: none; }}
        .topbar-links a:hover {{ text-decoration: underline; }}

        /* --- Main layout: sidebar + table area --- */
        .main {{
            display: grid;
            grid-template-columns: 300px 1fr;
            height: calc(100vh - 52px);
        }}
        @media (max-width: 800px) {{
            .main {{ grid-template-columns: 1fr; }}
            .sidebar {{ max-height: 40vh; }}
        }}

        /* --- Sidebar --- */
        .sidebar {{
            background: #fff;
            border-right: 1px solid #e5e7eb;
            overflow-y: auto;
            padding: 0.75rem;
            display: flex;
            flex-direction: column;
            gap: 0.75rem;
        }}
        .sidebar-section {{
            border: 1px solid #e5e7eb;
            border-radius: 6px;
            overflow: hidden;
        }}
        .sidebar-section-header {{
            background: #f1f5f9;
            padding: 0.4rem 0.6rem;
            font-size: 0.82rem;
            font-weight: 700;
            color: #334155;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: space-between;
            user-select: none;
        }}
        .sidebar-section-header:hover {{ background: #e2e8f0; }}
        .sidebar-section-header .chevron {{
            font-size: 0.7rem;
            transition: transform 0.15s;
        }}
        .sidebar-section-header.collapsed .chevron {{ transform: rotate(-90deg); }}
        .sidebar-section-body {{
            padding: 0.35rem 0.6rem;
            font-size: 0.8rem;
            max-height: 250px;
            overflow-y: auto;
        }}
        .sidebar-section-body.hidden {{ display: none; }}

        /* --- Method category groups --- */
        .method-category {{
            margin-bottom: 0.6rem;
        }}
        .method-category-header {{
            font-weight: 700;
            font-size: 0.82rem;
            color: #0f172a;
            padding: 0.25rem 0.4rem;
            background: #e2e8f0;
            border-radius: 4px;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 0.35rem;
            user-select: none;
        }}
        .method-category-header:hover {{ background: #cbd5e1; }}
        .method-category-header .cat-chevron {{
            font-size: 0.7rem;
            transition: transform 0.15s;
        }}
        .method-category-body {{ padding-left: 0.3rem; }}
        .method-category-body.hidden {{ display: none; }}

        /* --- Method groups --- */
        .method-group {{
            margin-bottom: 0.4rem;
        }}
        .method-group-header {{
            display: flex;
            align-items: center;
            gap: 0.3rem;
            padding: 0.2rem 0;
            cursor: pointer;
        }}
        .method-group-header input[type="checkbox"] {{ margin: 0; }}
        .method-group-header label {{
            font-weight: 600;
            font-size: 0.82rem;
            color: #1e293b;
            cursor: pointer;
        }}
        .method-group-toggle {{
            margin-left: auto;
            font-size: 0.7rem;
            color: #6b7280;
            cursor: pointer;
            user-select: none;
        }}
        .method-cols {{
            padding-left: 1.2rem;
            max-height: 180px;
            overflow-y: auto;
        }}
        .method-cols.hidden {{ display: none; }}
        .method-cols label {{
            display: block;
            padding: 0.1rem 0;
            cursor: pointer;
            color: #374151;
        }}
        .method-cols label:hover {{ color: #1d4ed8; }}
        .method-cols input[type="checkbox"] {{ margin-right: 0.3rem; }}

        /* --- Filters --- */
        .filter-group {{ margin-bottom: 0.45rem; }}
        .filter-group label.filter-label {{
            display: block;
            font-weight: 600;
            font-size: 0.78rem;
            color: #475569;
            margin-bottom: 0.15rem;
        }}
        .filter-group input[type="text"],
        .filter-group input[type="search"],
        .filter-group input[type="date"] {{
            width: 100%;
            padding: 0.25rem 0.4rem;
            border: 1px solid #d1d5db;
            border-radius: 4px;
            font-size: 0.82rem;
        }}
        .filter-group select {{
            width: 100%;
            padding: 0.25rem 0.4rem;
            border: 1px solid #d1d5db;
            border-radius: 4px;
            font-size: 0.82rem;
            max-height: 100px;
        }}
        .date-range {{
            display: flex;
            gap: 0.3rem;
            align-items: center;
        }}
        .date-range input {{ flex: 1; }}
        .date-range span {{ font-size: 0.75rem; color: #9ca3af; }}

        /* --- Searchable checklist --- */
        .checklist-wrap {{
            max-height: 150px;
            overflow-y: auto;
            border: 1px solid #e5e7eb;
            border-radius: 4px;
            background: #fff;
            margin-top: 0.2rem;
        }}
        .checklist-wrap label {{
            display: flex;
            align-items: center;
            gap: 0.3rem;
            padding: 0.15rem 0.4rem;
            font-size: 0.8rem;
            cursor: pointer;
            white-space: nowrap;
        }}
        .checklist-wrap label:hover {{ background: #f0f4ff; }}
        .checklist-wrap label.hidden {{ display: none; }}
        .checklist-wrap label.drag-highlight {{ background: #c7d2fe; }}
        .checklist-wrap input[type="checkbox"] {{ margin: 0; flex-shrink: 0; }}
        .checklist-wrap.is-dragging {{ user-select: none; cursor: crosshair; }}
        .checklist-actions {{
            display: flex;
            align-items: center;
            gap: 0.3rem;
            margin-top: 0.2rem;
        }}
        .checklist-count {{
            font-size: 0.72rem;
            color: #6b7280;
            margin-left: auto;
        }}

        /* --- Action buttons --- */
        .action-bar {{
            display: flex; gap: 0.5rem; flex-wrap: wrap;
        }}
        .btn {{
            padding: 0.4rem 0.8rem;
            border: 1px solid #2563eb;
            border-radius: 4px;
            background: #2563eb;
            color: #fff;
            font-size: 0.82rem;
            cursor: pointer;
            white-space: nowrap;
        }}
        .btn:hover {{ background: #1d4ed8; }}
        .btn-outline {{
            background: #fff;
            color: #2563eb;
        }}
        .btn-outline:hover {{ background: #eff6ff; }}
        .btn-sm {{
            padding: 0.25rem 0.5rem;
            font-size: 0.75rem;
        }}

        /* --- Table area --- */
        .table-area {{
            overflow: hidden;
            display: flex;
            flex-direction: column;
        }}
        .table-toolbar {{
            padding: 0.5rem 1rem;
            background: #fff;
            border-bottom: 1px solid #e5e7eb;
            display: flex;
            align-items: center;
            gap: 0.75rem;
            font-size: 0.85rem;
            color: #6b7280;
            flex-wrap: wrap;
        }}
        .table-wrap {{
            flex: 1;
            overflow: auto;
            background: #fff;
        }}
        table {{
            border-collapse: collapse;
            table-layout: auto;
            width: max-content;
            min-width: 100%;
            font-size: 0.8rem;
        }}
        thead th {{
            position: sticky;
            top: 0;
            z-index: 3;
        }}
        /* Second header row sticks below the first;
           start at 0 – JS will set the real value immediately */
        thead tr:nth-child(2) th {{
            top: 0;
        }}
        th, td {{
            border: 1px solid #e5e7eb;
            padding: 0.2rem 0.4rem;
            text-align: left;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 180px;
        }}
        th {{
            background: #f1f5f9;
            font-weight: 600;
            font-size: 0.75rem;
        }}
        th.group-header {{
            text-align: center;
            font-size: 0.78rem;
            background: var(--mg-hdr, #e0e7ff);
            color: var(--mg-txt, #3730a3);
        }}
        th.param-header {{
            max-width: 120px;
            background: var(--mg-hdr, #f1f5f9);
            color: var(--mg-txt, #334155);
            opacity: 0.85;
        }}

        /* --- Sticky sample column --- */
        td.sample-sticky, th.sample-sticky {{
            position: sticky;
            left: 0;
            z-index: 4;
            background: #f1f5f9;
            min-width: 7ch;
            border-right: 2px solid #cbd5e1;
        }}
        thead th.sample-sticky {{ z-index: 5; }}
        tbody td.sample-sticky {{ background: #fff; font-weight: 600; }}
        tbody tr:nth-child(even) td.sample-sticky {{ background: #f9fafb; }}
        tbody tr:hover td.sample-sticky {{ background: #e5f3ff; }}

        /* --- Method group header colors --- */
        .mg-0  {{ --mg-hdr: #bfdbfe; --mg-txt: #1e40af; }}
        .mg-1  {{ --mg-hdr: #bbf7d0; --mg-txt: #166534; }}
        .mg-2  {{ --mg-hdr: #fef08a; --mg-txt: #854d0e; }}
        .mg-3  {{ --mg-hdr: #fbcfe8; --mg-txt: #9d174d; }}
        .mg-4  {{ --mg-hdr: #ddd6fe; --mg-txt: #5b21b6; }}
        .mg-5  {{ --mg-hdr: #fed7aa; --mg-txt: #9a3412; }}
        .mg-6  {{ --mg-hdr: #a5f3fc; --mg-txt: #155e75; }}
        .mg-7  {{ --mg-hdr: #fecaca; --mg-txt: #991b1b; }}
        .mg-8  {{ --mg-hdr: #e2e8f0; --mg-txt: #334155; }}
        .mg-9  {{ --mg-hdr: #99f6e4; --mg-txt: #115e59; }}
        .mg-10 {{ --mg-hdr: #e9d5ff; --mg-txt: #6b21a8; }}
        .mg-11 {{ --mg-hdr: #fde68a; --mg-txt: #92400e; }}
        .mg-12 {{ --mg-hdr: #cbd5e1; --mg-txt: #475569; }}
        .mg-13 {{ --mg-hdr: #fda4af; --mg-txt: #9f1239; }}
        /* --- Body column alternation within groups --- */
        td.col-even {{ background: #f3f4f6; }}
        td.col-odd  {{ background: #fff; }}
        tbody tr:hover td {{ background: #e5f3ff !important; }}
        tbody tr:hover td.sample-sticky {{ background: #e5f3ff !important; }}
        .value-cell {{ text-align: right; }}
        /* metadata column styling */
        .meta-col-header {{ font-style: italic; font-weight: 500; }}
        .meta-cell {{ font-style: italic; text-align: left; color: #475569; }}

        .empty-msg {{
            padding: 3rem;
            text-align: center;
            color: #9ca3af;
            font-size: 1rem;
        }}
        .loading {{ opacity: 0.5; pointer-events: none; }}
    </style>
</head>
<body>
    <div class="topbar">
        <h1>{escape(title)}</h1>
        <div class="topbar-links">
            <a href="/web/presence">Presence matrix</a>
            <a href="/web/matrix">Methods index</a>
            <a href="/web/labdata">Lab data page</a>
            <a href="/web/samples/filter">Sample filters</a>
            <a href="/web/compounds">Compound info</a>
            <a href="/web/ratios">Calculate Ratios</a>
            <a href="/web/upload">Upload Data</a>
        </div>
    </div>
    <div class="main">
        <!-- ========= SIDEBAR ========= -->
        <aside class="sidebar" id="sidebar">
            <!-- 1. Compounds (method groups + columns) -->
            <div class="sidebar-section">
                <div class="sidebar-section-header" data-target="methods-body">
                    <span>Compounds</span>
                    <span class="chevron">&#9660;</span>
                </div>
                <div class="sidebar-section-body" id="methods-body">
                    <div style="display:flex;gap:0.3rem;margin-bottom:0.4rem;">
                        <button class="btn btn-sm" id="select-all-methods">All</button>
                        <button class="btn btn-sm btn-outline" id="deselect-all-methods">None</button>
                    </div>
                    <div id="methods-list">(loading…)</div>
                </div>
            </div>
            <!-- 2. Samples -->
            <div class="sidebar-section">
                <div class="sidebar-section-header" data-target="samples-body">
                    <span>Samples</span>
                    <span class="chevron">&#9660;</span>
                </div>
                <div class="sidebar-section-body" id="samples-body">
                    <input type="search" id="f-samplenumber-search" placeholder="Search samples…" />
                    <div class="checklist-actions">
                        <button class="btn btn-sm" onclick="checklistAll('f-samplenumber-list')">All</button>
                        <button class="btn btn-sm btn-outline" onclick="checklistNone('f-samplenumber-list')">None</button>
                        <span class="checklist-count" id="f-samplenumber-count"></span>
                    </div>
                    <div class="checklist-wrap" id="f-samplenumber-list"></div>
                </div>
            </div>
            <!-- 3. Operator -->
            <div class="sidebar-section">
                <div class="sidebar-section-header collapsed" data-target="operator-body">
                    <span>Operator</span>
                    <span class="chevron">&#9660;</span>
                </div>
                <div class="sidebar-section-body hidden" id="operator-body">
                    <input type="search" id="f-measured-by-search" placeholder="Search operators…" />
                    <div class="checklist-actions">
                        <button class="btn btn-sm" onclick="checklistAll('f-measured-by-list')">All</button>
                        <button class="btn btn-sm btn-outline" onclick="checklistNone('f-measured-by-list')">None</button>
                        <span class="checklist-count" id="f-measured-by-count"></span>
                    </div>
                    <div class="checklist-wrap" id="f-measured-by-list"></div>
                </div>
            </div>
            <!-- 4. Date Range -->
            <div class="sidebar-section">
                <div class="sidebar-section-header collapsed" data-target="daterange-body">
                    <span>Date Range</span>
                    <span class="chevron">&#9660;</span>
                </div>
                <div class="sidebar-section-body hidden" id="daterange-body">
                    <div class="date-range">
                        <input type="date" id="f-date-min" />
                        <span>–</span>
                        <input type="date" id="f-date-max" />
                    </div>
                </div>
            </div>
            <!-- 5. Fraction -->
            <div class="sidebar-section">
                <div class="sidebar-section-header collapsed" data-target="fraction-body">
                    <span>Fraction</span>
                    <span class="chevron">&#9660;</span>
                </div>
                <div class="sidebar-section-body hidden" id="fraction-body">
                    <select id="f-fraction" multiple></select>
                </div>
            </div>
            <!-- 6. Project -->
            <div class="sidebar-section">
                <div class="sidebar-section-header collapsed" data-target="project-body">
                    <span>Project</span>
                    <span class="chevron">&#9660;</span>
                </div>
                <div class="sidebar-section-body hidden" id="project-body">
                    <select id="f-project" multiple></select>
                </div>
            </div>
            <!-- 7. Other filters -->
            <div class="sidebar-section">
                <div class="sidebar-section-header collapsed" data-target="other-body">
                    <span>Other Filters</span>
                    <span class="chevron">&#9660;</span>
                </div>
                <div class="sidebar-section-body hidden" id="other-body">
                    <div class="filter-group">
                        <label class="filter-label">Type</label>
                        <select id="f-type" multiple></select>
                    </div>
                    <div class="filter-group">
                        <label class="filter-label">Instrument</label>
                        <select id="f-instrument" multiple></select>
                    </div>
                    <div class="filter-group">
                        <label class="filter-label">Data Type</label>
                        <select id="f-data-type" multiple></select>
                    </div>
                </div>
            </div>
            <!-- 8. View: metadata columns -->
            <div class="sidebar-section">
                <div class="sidebar-section-header collapsed" data-target="metacols-body">
                    <span>View: Columns</span>
                    <span class="chevron">&#9660;</span>
                </div>
                <div class="sidebar-section-body hidden" id="metacols-body">
                    <p style="font-size:0.72rem;color:#64748b;margin:0 0 0.3rem;">Toggle metadata columns per compound group</p>
                    <label style="display:block;margin:0.15rem 0;"><input type="checkbox" class="meta-col-cb" value="instrument" checked /> Instrument</label>
                    <label style="display:block;margin:0.15rem 0;"><input type="checkbox" class="meta-col-cb" value="fraction" checked /> Fraction</label>
                    <label style="display:block;margin:0.15rem 0;"><input type="checkbox" class="meta-col-cb" value="data_type" checked /> Data Type</label>
                    <label style="display:block;margin:0.15rem 0;"><input type="checkbox" class="meta-col-cb" value="name" checked /> Name</label>
                    <label style="display:block;margin:0.15rem 0;"><input type="checkbox" class="meta-col-cb" value="measured_by" checked /> Operator</label>
                    <label style="display:block;margin:0.15rem 0;"><input type="checkbox" class="meta-col-cb" value="date" checked /> Date</label>
                </div>
            </div>
            <!-- Actions -->
            <div class="action-bar">
                <button class="btn" id="btn-apply">Apply</button>
                <button class="btn btn-outline" id="btn-csv">Export CSV</button>
                <button class="btn btn-outline" id="btn-reset">Reset</button>
            </div>
        </aside>

        <!-- ========= TABLE AREA ========= -->
        <div class="table-area">
            <div class="table-toolbar">
                <span id="result-info">Configure columns and filters, then click <strong>Apply</strong>.</span>
            </div>
            <div class="table-wrap" id="table-wrap">
                <div class="empty-msg" id="empty-msg">Select method groups on the left and click Apply to view data.</div>
            </div>
        </div>
    </div>

    <script>
    function checklistAll(listId) {{
        const el = document.getElementById(listId);
        if (!el) return;
        el.querySelectorAll('.checklist-cb').forEach(cb => {{ cb.checked = true; }});
        const countEl = document.getElementById(listId.replace('-list', '-count'));
        if (countEl) {{
            const total = el.querySelectorAll('.checklist-cb').length;
            countEl.textContent = total + '/' + total + ' selected';
        }}
    }}
    function checklistNone(listId) {{
        const el = document.getElementById(listId);
        if (!el) return;
        el.querySelectorAll('.checklist-cb').forEach(cb => {{ cb.checked = false; }});
        const countEl = document.getElementById(listId.replace('-list', '-count'));
        if (countEl) countEl.textContent = '';
    }}
    (function() {{
        // ---------- State ----------
        let META = null; // from /api/explorer/meta
        let LAST_RESULT = null; // from /api/explorer/query

        // ---------- DOM refs ----------
        const methodsList = document.getElementById('methods-list');
        const resultInfo = document.getElementById('result-info');
        const tableWrap = document.getElementById('table-wrap');
        const emptyMsg = document.getElementById('empty-msg');
        const btnApply = document.getElementById('btn-apply');
        const btnCsv = document.getElementById('btn-csv');
        const btnReset = document.getElementById('btn-reset');
        const btnSelectAll = document.getElementById('select-all-methods');
        const btnDeselectAll = document.getElementById('deselect-all-methods');

        // filter elements
        const fSampleSearch = document.getElementById('f-samplenumber-search');
        const fSampleList = document.getElementById('f-samplenumber-list');
        const fSampleCount = document.getElementById('f-samplenumber-count');
        const fMeasuredBySearch = document.getElementById('f-measured-by-search');
        const fMeasuredByList = document.getElementById('f-measured-by-list');
        const fMeasuredByCount = document.getElementById('f-measured-by-count');
        const fType = document.getElementById('f-type');
        const fDateMin = document.getElementById('f-date-min');
        const fDateMax = document.getElementById('f-date-max');
        const fFraction = document.getElementById('f-fraction');
        const fProject = document.getElementById('f-project');
        const fInstrument = document.getElementById('f-instrument');
        const fDataType = document.getElementById('f-data-type');

        // ---------- Sidebar collapse ----------
        document.querySelectorAll('.sidebar-section-header').forEach(hdr => {{
            hdr.addEventListener('click', () => {{
                const target = document.getElementById(hdr.dataset.target);
                if (target) {{
                    target.classList.toggle('hidden');
                    hdr.classList.toggle('collapsed');
                }}
            }});
        }});

        // ---------- Populate filter selects ----------
        function fillSelect(el, values) {{
            el.innerHTML = '';
            values.forEach(v => {{
                const opt = document.createElement('option');
                opt.value = v;
                opt.textContent = v;
                el.appendChild(opt);
            }});
        }}

        function getSelectedValues(selectEl) {{
            return Array.from(selectEl.selectedOptions).map(o => o.value);
        }}

        // ---------- Searchable checklist helpers ----------
        function fillChecklist(listEl, values, searchEl, countEl) {{
            listEl.innerHTML = '';
            listEl._lastClicked = null; // for shift-click
            values.forEach((v, idx) => {{
                const lbl = document.createElement('label');
                lbl.dataset.idx = idx;
                const cb = document.createElement('input');
                cb.type = 'checkbox';
                cb.value = v;
                cb.className = 'checklist-cb';
                cb.dataset.idx = idx;
                cb.addEventListener('change', () => updateChecklistCount(listEl, countEl));
                lbl.appendChild(cb);
                lbl.appendChild(document.createTextNode(v));
                listEl.appendChild(lbl);
            }});
            updateChecklistCount(listEl, countEl);

            // --- Shift+click range selection ---
            listEl.addEventListener('click', (e) => {{
                const cb = e.target.closest('.checklist-cb');
                if (!cb) return;
                const idx = parseInt(cb.dataset.idx, 10);
                if (e.shiftKey && listEl._lastClicked !== null) {{
                    const allCbs = getVisibleCheckboxes(listEl);
                    const lastIdx = listEl._lastClicked;
                    const from = Math.min(lastIdx, idx);
                    const to = Math.max(lastIdx, idx);
                    const newState = cb.checked;
                    allCbs.forEach(c => {{
                        const i = parseInt(c.dataset.idx, 10);
                        if (i >= from && i <= to) {{
                            c.checked = newState;
                        }}
                    }});
                    updateChecklistCount(listEl, countEl);
                }}
                listEl._lastClicked = idx;
            }});

            // --- Click+drag (rubber-band) selection ---
            let dragStartIdx = null;
            let dragState = null; // true = checking, false = unchecking

            listEl.addEventListener('mousedown', (e) => {{
                // Only trigger on left button, not on checkbox itself
                if (e.button !== 0) return;
                const lbl = e.target.closest('.checklist-wrap > label');
                if (!lbl || e.target.tagName === 'INPUT') return;
                e.preventDefault();
                dragStartIdx = parseInt(lbl.dataset.idx, 10);
                const cb = lbl.querySelector('.checklist-cb');
                // Save pre-drag state for all checkboxes
                listEl.querySelectorAll('.checklist-cb').forEach(c => {{ c._preDrag = c.checked; }});
                // toggle the start item
                cb.checked = !cb.checked;
                dragState = cb.checked;
                listEl.classList.add('is-dragging');
                lbl.classList.add('drag-highlight');
                listEl._lastClicked = dragStartIdx;
                updateChecklistCount(listEl, countEl);
            }});

            listEl.addEventListener('mousemove', (e) => {{
                if (dragStartIdx === null) return;
                e.preventDefault();
                const y = e.clientY;
                const allLabels = Array.from(listEl.querySelectorAll(':scope > label:not(.hidden)'));
                let hoverIdx = null;
                for (const l of allLabels) {{
                    const rect = l.getBoundingClientRect();
                    if (y >= rect.top && y <= rect.bottom) {{
                        hoverIdx = parseInt(l.dataset.idx, 10);
                        break;
                    }}
                }}
                if (hoverIdx === null) return;
                const from = Math.min(dragStartIdx, hoverIdx);
                const to = Math.max(dragStartIdx, hoverIdx);
                allLabels.forEach(l => {{
                    const i = parseInt(l.dataset.idx, 10);
                    const inRange = i >= from && i <= to;
                    l.classList.toggle('drag-highlight', inRange);
                    const c = l.querySelector('.checklist-cb');
                    if (inRange) c.checked = dragState;
                    else if (c._preDrag !== undefined) c.checked = c._preDrag;
                }});
                updateChecklistCount(listEl, countEl);
            }});

            function endDrag() {{
                if (dragStartIdx === null) return;
                dragStartIdx = null;
                dragState = null;
                listEl.classList.remove('is-dragging');
                listEl.querySelectorAll('.drag-highlight').forEach(l => l.classList.remove('drag-highlight'));
                updateChecklistCount(listEl, countEl);
            }}
            listEl.addEventListener('mouseup', endDrag);
            listEl.addEventListener('mouseleave', endDrag);

            // --- Search filter ---
            if (searchEl) {{
                searchEl.addEventListener('input', () => {{
                    const q = (searchEl.value || '').trim().toLowerCase();
                    listEl.querySelectorAll('label').forEach(lbl => {{
                        const txt = lbl.textContent.toLowerCase();
                        lbl.classList.toggle('hidden', q !== '' && !txt.includes(q));
                    }});
                }});
            }}
        }}

        function getVisibleCheckboxes(listEl) {{
            return Array.from(listEl.querySelectorAll(':scope > label:not(.hidden) .checklist-cb'));
        }}

        function getChecklistValues(listEl) {{
            return Array.from(listEl.querySelectorAll('.checklist-cb:checked')).map(cb => cb.value);
        }}

        function updateChecklistCount(listEl, countEl) {{
            if (!countEl) return;
            const total = listEl.querySelectorAll('.checklist-cb').length;
            const checked = listEl.querySelectorAll('.checklist-cb:checked').length;
            countEl.textContent = checked ? checked + '/' + total + ' selected' : '';
        }}

        // ---------- Build methods list ----------
        function renderMethodItem(m, parent) {{
            const group = document.createElement('div');
            group.className = 'method-group';

            const hdr = document.createElement('div');
            hdr.className = 'method-group-header';

            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.checked = true;
            cb.dataset.method = m.method;
            cb.className = 'method-group-cb';

            const lbl = document.createElement('label');
            lbl.textContent = m.label;
            lbl.addEventListener('click', () => {{ cb.checked = !cb.checked; cb.dispatchEvent(new Event('change')); }});

            const toggle = document.createElement('span');
            toggle.className = 'method-group-toggle';
            toggle.textContent = '\\u25B6';
            toggle.title = 'Show/hide columns';

            hdr.appendChild(cb);
            hdr.appendChild(lbl);
            hdr.appendChild(toggle);
            group.appendChild(hdr);

            const colsDiv = document.createElement('div');
            colsDiv.className = 'method-cols hidden';

            const colActions = document.createElement('div');
            colActions.style.cssText = 'display:flex;gap:0.3rem;margin-bottom:0.2rem;';
            const allBtn = document.createElement('button');
            allBtn.className = 'btn btn-sm';
            allBtn.textContent = 'All';
            allBtn.addEventListener('click', (e) => {{
                e.stopPropagation();
                colsDiv.querySelectorAll('.col-cb').forEach(c => c.checked = true);
            }});
            const noneBtn = document.createElement('button');
            noneBtn.className = 'btn btn-sm btn-outline';
            noneBtn.textContent = 'None';
            noneBtn.addEventListener('click', (e) => {{
                e.stopPropagation();
                colsDiv.querySelectorAll('.col-cb').forEach(c => c.checked = false);
            }});
            colActions.appendChild(allBtn);
            colActions.appendChild(noneBtn);
            colsDiv.appendChild(colActions);

            m.columns.forEach(col => {{
                const colLbl = document.createElement('label');
                const colCb = document.createElement('input');
                colCb.type = 'checkbox';
                colCb.checked = true;
                colCb.className = 'col-cb';
                colCb.dataset.method = m.method;
                colCb.dataset.col = col;
                colLbl.appendChild(colCb);
                colLbl.appendChild(document.createTextNode(col));
                colsDiv.appendChild(colLbl);
            }});

            group.appendChild(colsDiv);

            toggle.addEventListener('click', () => {{
                colsDiv.classList.toggle('hidden');
                toggle.textContent = colsDiv.classList.contains('hidden') ? '\\u25B6' : '\\u25BC';
            }});

            cb.addEventListener('change', () => {{
                colsDiv.querySelectorAll('.col-cb').forEach(c => c.checked = cb.checked);
            }});

            parent.appendChild(group);
        }}

        function renderMethods(methods) {{
            methodsList.innerHTML = '';
            // Use grouped layout if available
            const groups = META.method_groups;
            if (groups && groups.length) {{
                groups.forEach(grp => {{
                    const cat = document.createElement('div');
                    cat.className = 'method-category';

                    const catHdr = document.createElement('div');
                    catHdr.className = 'method-category-header';
                    const chevron = document.createElement('span');
                    chevron.className = 'cat-chevron';
                    chevron.textContent = '\\u25BC';
                    catHdr.appendChild(chevron);
                    catHdr.appendChild(document.createTextNode(grp.label));

                    const catBody = document.createElement('div');
                    catBody.className = 'method-category-body';

                    catHdr.addEventListener('click', () => {{
                        catBody.classList.toggle('hidden');
                        chevron.textContent = catBody.classList.contains('hidden') ? '\\u25B6' : '\\u25BC';
                    }});

                    cat.appendChild(catHdr);
                    grp.methods.forEach(m => renderMethodItem(m, catBody));
                    cat.appendChild(catBody);
                    methodsList.appendChild(cat);
                }});
            }} else {{
                methods.forEach(m => renderMethodItem(m, methodsList));
            }}
        }}

        // ---------- Collect user selection ----------
        function getSelection() {{
            const methods = {{}};
            document.querySelectorAll('.method-group-cb').forEach(cb => {{
                if (!cb.checked) return;
                const m = cb.dataset.method;
                const cols = [];
                document.querySelectorAll('.col-cb[data-method="' + m + '"]').forEach(c => {{
                    if (c.checked) cols.push(c.dataset.col);
                }});
                if (cols.length > 0) methods[m] = cols;
            }});
            return methods;
        }}

        function getFilters() {{
            const f = {{}};
            const sn = getChecklistValues(fSampleList);
            if (sn.length) f.samplenumber = sn;
            const mb = getChecklistValues(fMeasuredByList);
            if (mb.length) f.measured_by = mb;
            const tp = getSelectedValues(fType);
            if (tp.length) f.type = tp;
            const dMin = (fDateMin.value || '').trim();
            const dMax = (fDateMax.value || '').trim();
            if (dMin) f.date_min = dMin;
            if (dMax) f.date_max = dMax;
            const fr = getSelectedValues(fFraction);
            if (fr.length) f.fraction = fr;
            const pr = getSelectedValues(fProject);
            if (pr.length) f.project = pr;
            const inst = getSelectedValues(fInstrument);
            if (inst.length) f.instrument = inst;
            const dt = getSelectedValues(fDataType);
            if (dt.length) f.data_type = dt;
            return f;
        }}

        function getMetadataCols() {{
            const cols = [];
            document.querySelectorAll('.meta-col-cb').forEach(cb => {{
                if (cb.checked) cols.push(cb.value);
            }});
            return cols;
        }}

        function getVisibleColumns(result) {{
            const {{ columns, rows }} = result;
            const activeMeta = new Set(getMetadataCols());
            return columns.filter((c, i) => {{
                if (i === 0) return true; // always keep sample column

                // Respect metadata column toggles
                if (c.is_meta) {{
                    const parts = String(c.id || '').split('__meta__');
                    const field = parts.length > 1 ? parts[parts.length - 1] : '';
                    if (!activeMeta.has(field)) return false;
                }}

                // Hide columns empty across all rows
                return rows.some(row => {{
                    const v = row[c.id];
                    return v !== null && v !== undefined && v !== '';
                }});
            }});
        }}

        // ---------- Render result table ----------
        function renderTable(result) {{
            LAST_RESULT = result;
            const {{ columns, rows, total }} = result;

            if (!rows.length) {{
                resultInfo.innerHTML = '<strong>0</strong> samples';
                tableWrap.innerHTML = '<div class="empty-msg">No data matches the current selection.</div>';
                return;
            }}

            const nonEmptyCols = getVisibleColumns(result);

            const hiddenCount = (columns.length - 1) - (nonEmptyCols.length - 1);
            let infoHtml = '<strong>' + total + '</strong> samples &middot; ' + (nonEmptyCols.length - 1) + ' columns';
            if (hiddenCount > 0) {{
                infoHtml += ' <span style="color:#9ca3af;">(' + hiddenCount + ' columns hidden)</span>';
            }}
            resultInfo.innerHTML = infoHtml;

            // Group columns by method for group header row (using filtered cols)
            const methodGroups = [];
            let currentMethod = null;
            let currentSpan = 0;
            nonEmptyCols.forEach((c, i) => {{
                if (i === 0) return; // skip samplenumber
                if (c.method !== currentMethod) {{
                    if (currentMethod !== null) {{
                        methodGroups.push({{ method: currentMethod, span: currentSpan }});
                    }}
                    currentMethod = c.method;
                    currentSpan = 1;
                }} else {{
                    currentSpan++;
                }}
            }});
            if (currentMethod !== null) {{
                methodGroups.push({{ method: currentMethod, span: currentSpan }});
            }}

            // Build method -> color-class map
            const methodColorMap = {{}};
            let colorIdx = 0;
            methodGroups.forEach(g => {{
                methodColorMap[g.method] = 'mg-' + (colorIdx % 14);
                colorIdx++;
            }});

            // Track each column's index within its method group (for grey/white alternation)
            const colGroupIdx = [];
            let prevMethod = null;
            let withinIdx = 0;
            nonEmptyCols.forEach((c, i) => {{
                if (i === 0) {{ colGroupIdx.push(0); return; }}
                if (c.method !== prevMethod) {{ withinIdx = 0; prevMethod = c.method; }}
                else {{ withinIdx++; }}
                colGroupIdx.push(withinIdx);
            }});

            let html = '<table>';

            // Row 1: Group method names
            html += '<thead>';
            html += '<tr><th rowspan="2" class="sample-sticky">Sample</th>';
            methodGroups.forEach(g => {{
                const label = displayMethodLabel(g.method);
                const cls = methodColorMap[g.method] || '';
                html += '<th class="group-header ' + cls + '" colspan="' + g.span + '">' + escapeHtml(label) + '</th>';
            }});
            html += '</tr>';

            // Row 2: Parameter names
            html += '<tr>';
            nonEmptyCols.forEach((c, i) => {{
                if (i === 0) return;
                const cls = methodColorMap[c.method] || '';
                const metaCls = c.is_meta ? ' meta-col-header' : '';
                html += '<th class="param-header ' + cls + metaCls + '" title="' + escapeHtml(c.method + ': ' + c.label) + '">' + escapeHtml(c.label) + '</th>';
            }});
            html += '</tr></thead>';

            // Body
            html += '<tbody>';
            rows.forEach(row => {{
                html += '<tr>';
                html += '<td class="sample-sticky">' + escapeHtml(row.samplenumber || '') + '</td>';
                nonEmptyCols.forEach((c, i) => {{
                    if (i === 0) return;
                    const v = row[c.id];
                    const txt = (v === null || v === undefined || v === '') ? '' : String(v);
                    const altCls = (colGroupIdx[i] % 2 === 0) ? 'col-even' : 'col-odd';
                    const metaCls = c.is_meta ? ' meta-cell' : '';
                    html += '<td class="value-cell ' + altCls + metaCls + '">' + escapeHtml(txt) + '</td>';
                }});
                html += '</tr>';
            }});
            html += '</tbody></table>';

            tableWrap.innerHTML = html;

            // Measure first header row height and fix second-row sticky offset.
            // Use the <tr> element so we get the exact row height (incl. collapsed borders).
            requestAnimationFrame(() => {{
                const firstRow = tableWrap.querySelector('thead tr:first-child');
                if (firstRow) {{
                    const h = firstRow.getBoundingClientRect().height;
                    tableWrap.querySelectorAll('thead tr:nth-child(2) th').forEach(th => {{
                        th.style.top = h + 'px';
                    }});
                }}
            }});
        }}

        function escapeHtml(s) {{
            const d = document.createElement('div');
            d.textContent = s;
            return d.innerHTML;
        }}

        function displayMethodLabel(method) {{
            const m = String(method || '').trim();
            if (m === 'ft_icr_ms') return 'FT-ICR-MS';
            if (m === 'isotope_co2_werte') return 'Isotope CO2 Werte';
            if (m === 'isotope_hd_werte') return 'Isotope HD Werte';
            return m.replace(/_/g, ' ').replace(/\\b\\w/g, c => c.toUpperCase());
        }}

        // ---------- API calls ----------
        async function loadMeta() {{
            try {{
                const r = await fetch('/api/explorer/meta');
                META = await r.json();
                renderMethods(META.methods);
                fillChecklist(fSampleList, META.filters.samplenumbers || [], fSampleSearch, fSampleCount);
                fillChecklist(fMeasuredByList, META.filters.measured_by || [], fMeasuredBySearch, fMeasuredByCount);
                fillSelect(fType, META.filters.type || []);
                fillSelect(fFraction, META.filters.fraction || []);
                fillSelect(fProject, META.filters.project || []);
                fillSelect(fInstrument, META.filters.instrument || []);
                fillSelect(fDataType, META.filters.data_type || []);
            }} catch(e) {{
                methodsList.textContent = 'Error loading metadata: ' + e.message;
            }}
        }}

        async function runQuery() {{
            const methods = getSelection();
            if (Object.keys(methods).length === 0) {{
                alert('Select at least one method group with columns.');
                return;
            }}
            const filters = getFilters();
            const include_metadata_cols = getMetadataCols();
            resultInfo.textContent = 'Loading…';
            tableWrap.classList.add('loading');
            try {{
                const r = await fetch('/api/explorer/query', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ methods, filters, include_metadata_cols }}),
                }});
                const data = await r.json();
                renderTable(data);
            }} catch(e) {{
                resultInfo.textContent = 'Error: ' + e.message;
                tableWrap.innerHTML = '<div class="empty-msg">Failed to load data.</div>';
            }} finally {{
                tableWrap.classList.remove('loading');
            }}
        }}

        // ---------- CSV export ----------
        function exportCsv() {{
            if (!LAST_RESULT || !LAST_RESULT.rows.length) {{
                alert('No data to export. Click Apply first.');
                return;
            }}
            const {{ rows }} = LAST_RESULT;
            const columns = getVisibleColumns(LAST_RESULT);
            const header = columns.map(c => c.id === 'samplenumber' ? 'samplenumber' : c.method + ':' + c.label);
            const csvRows = [header.join(',')];
            rows.forEach(row => {{
                const vals = columns.map(c => {{
                    const v = row[c.id];
                    if (v === null || v === undefined) return '';
                    const s = String(v);
                    return s.includes(',') || s.includes('"') || s.includes('\\n')
                        ? '"' + s.replace(/"/g, '""') + '"'
                        : s;
                }});
                csvRows.push(vals.join(','));
            }});
            const blob = new Blob([csvRows.join('\\n')], {{ type: 'text/csv' }});
            const a = document.createElement('a');
            a.href = URL.createObjectURL(blob);
            a.download = 'explorer_data.csv';
            a.click();
        }}

        // ---------- Reset ----------
        function resetAll() {{
            fSampleSearch.value = '';
            fMeasuredBySearch.value = '';
            fDateMin.value = '';
            fDateMax.value = '';
            // Reset checklists
            [fSampleList, fMeasuredByList].forEach(list => {{
                list.querySelectorAll('.checklist-cb').forEach(cb => {{ cb.checked = false; }});
                list.querySelectorAll('label').forEach(lbl => lbl.classList.remove('hidden'));
            }});
            updateChecklistCount(fSampleList, fSampleCount);
            updateChecklistCount(fMeasuredByList, fMeasuredByCount);
            [fType, fFraction, fProject, fInstrument, fDataType].forEach(sel => {{
                Array.from(sel.options).forEach(o => o.selected = false);
            }});
            // Reset metadata column checkboxes to checked
            document.querySelectorAll('.meta-col-cb').forEach(cb => {{ cb.checked = true; }});
            document.querySelectorAll('.method-group-cb').forEach(cb => {{
                cb.checked = true;
                cb.dispatchEvent(new Event('change'));
            }});
            LAST_RESULT = null;
            tableWrap.innerHTML = '<div class="empty-msg">Select method groups on the left and click Apply to view data.</div>';
            resultInfo.innerHTML = 'Configure columns and filters, then click <strong>Apply</strong>.';
        }}

        // ---------- Wire events ----------
        btnApply.addEventListener('click', runQuery);
        btnCsv.addEventListener('click', exportCsv);
        btnReset.addEventListener('click', resetAll);
        btnSelectAll.addEventListener('click', () => {{
            document.querySelectorAll('.method-group-cb').forEach(cb => {{
                cb.checked = true;
                cb.dispatchEvent(new Event('change'));
            }});
        }});
        btnDeselectAll.addEventListener('click', () => {{
            document.querySelectorAll('.method-group-cb').forEach(cb => {{
                cb.checked = false;
                cb.dispatchEvent(new Event('change'));
            }});
        }});

        // Enter key in sample search field triggers apply
        fSampleSearch.addEventListener('keydown', (e) => {{
            if (e.key === 'Enter') runQuery();
        }});

        // ---------- Init ----------
        loadMeta();
    }})();
    </script>
</body>
</html>"""
    return HTMLResponse(content=html)


# ===========================================================================
# Compound information pages
# ===========================================================================
from starlette.responses import FileResponse

from api.compound_info import (
    compound_index,
    graphics_abs_path,
    load_all_compounds,
)


@router.get("/api/compounds")
def api_compounds():
    """Return the full list of compound info records."""
    return {"compounds": load_all_compounds()}


@router.get("/api/compounds/graphics/{path:path}")
def compound_graphic(path: str):
    """Serve a structure graphic PNG."""
    abs_path = graphics_abs_path(path)
    if abs_path is None:
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse("Not found", status_code=404)
    return FileResponse(str(abs_path), media_type="image/png")


@router.get("/web/compounds", response_class=HTMLResponse)
def web_compounds_index():
    """Compound index page — searchable list grouped by class."""
    compounds = load_all_compounds()
    title = "OrgChem \u2013 Compound Information"

    # Group by class
    classes: dict[str, list[dict]] = {}
    for c in compounds:
        cls = c["compound_class"] or "Other"
        classes.setdefault(cls, []).append(c)

    rows_html: list[str] = []
    for cls in sorted(classes.keys()):
        for c in classes[cls]:
            abbrev = escape(c["abbrev1"]) or "&mdash;"
            name = escape(c["compound_name"]) or "&mdash;"
            cas = escape(c["cas"]) or ""
            method = escape(c["method1"]) or ""
            cls_txt = escape(cls)
            gfx_icon = "&#128444;" if c["structure_graphic"] else ""
            key = c["abbrev1"] or c["compound_name"]
            link = f'/web/compounds/{escape(key, quote=True)}'
            rows_html.append(
                f'<tr class="compound-row" data-class="{cls_txt}">'
                f'<td><a href="{link}">{abbrev}</a></td>'
                f'<td><a href="{link}">{name}</a></td>'
                f'<td>{cls_txt}</td>'
                f'<td>{method}</td>'
                f'<td>{cas}</td>'
                f'<td style="text-align:center">{gfx_icon}</td>'
                f'</tr>'
            )

    body = "\n".join(rows_html)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <title>{escape(title)}</title>
    <style>
        :root {{ color-scheme: light; }}
        body {{
            font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
            margin: 0; padding: 1.5rem 2rem; background: #f5f6fa;
        }}
        .card {{
            background: #fff; border-radius: 8px; padding: 1rem 1.5rem;
            box-shadow: 0 1px 4px rgba(15,23,42,0.06);
        }}
        h1 {{ margin-top: 0; font-size: 1.4rem; }}
        .meta {{
            font-size: 0.85rem; color: #6b7280; margin-bottom: 0.75rem;
            display: flex; gap: 0.75rem; flex-wrap: wrap; align-items: center;
        }}
        .meta a {{ color: #2563eb; text-decoration: none; }}
        .meta a:hover {{ text-decoration: underline; }}
        #search {{
            padding: 0.35rem 0.6rem; font-size: 0.85rem; border: 1px solid #d1d5db;
            border-radius: 4px; width: 260px;
        }}
        .table-wrap {{
            max-height: 78vh; overflow: auto; border-radius: 6px;
            box-shadow: inset 0 0 0 1px #e5e7eb; background: #fff;
        }}
        table {{ border-collapse: collapse; width: 100%; font-size: 0.82rem; }}
        th, td {{ border: 1px solid #e5e7eb; padding: 0.25rem 0.5rem; text-align: left; }}
        th {{ background: #f1f5f9; position: sticky; top: 0; font-weight: 600; z-index: 1; }}
        tbody tr:hover {{ background: #e5f3ff; }}
        a {{ color: #2563eb; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .hidden {{ display: none; }}
    </style>
</head>
<body>
    <div class="card">
        <h1>{escape(title)}</h1>
        <div class="meta">
            <span>{len(compounds)} compounds from {len(classes)} classes</span>
            <span>|</span>
            <a href="/web/explorer">Data Explorer</a>
            <span>|</span>
            <a href="/web/presence">Presence Matrix</a>
            <span>|</span>
            <a href="/web/labdata">Lab data</a>
            <span>|</span>
            <input type="text" id="search" placeholder="Search by name, abbreviation, CAS…" />
        </div>
        <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>Abbreviation</th>
                        <th>Compound Name</th>
                        <th>Class</th>
                        <th>Method</th>
                        <th>CAS</th>
                        <th>Img</th>
                    </tr>
                </thead>
                <tbody id="tbody">{body}</tbody>
            </table>
        </div>
    </div>
    <script>
    (function() {{
        const search = document.getElementById('search');
        const rows = document.querySelectorAll('.compound-row');
        search.addEventListener('input', () => {{
            const q = search.value.trim().toLowerCase();
            rows.forEach(r => {{
                r.classList.toggle('hidden', q !== '' && !r.textContent.toLowerCase().includes(q));
            }});
        }});
    }})();
    </script>
</body>
</html>"""
    return HTMLResponse(content=html)


@router.get("/web/compounds/{compound_key}", response_class=HTMLResponse)
def web_compound_detail(compound_key: str):
    """Compound detail page showing all available info and structure graphic."""
    from urllib.parse import quote as url_quote

    idx = compound_index()
    norm = compound_key.strip().lower().replace(" ", "").replace("-", "")
    entry = idx.get(norm)
    if not entry:
        return HTMLResponse(
            content=f'<html><body><h1>Compound not found</h1>'
            f'<p>No compound info for <b>{escape(compound_key)}</b>.</p>'
            f'<p><a href="/web/compounds">Back to compound list</a></p></body></html>',
            status_code=404,
        )

    name = entry["compound_name"] or entry["abbrev1"]
    title = f"OrgChem \u2013 {name}"

    # Info rows
    info_rows: list[str] = []
    fields = [
        ("Compound Name", "compound_name"),
        ("Abbreviation", "abbrev1"),
        ("Abbreviation 2", "abbrev2"),
        ("Compound Class", "compound_class"),
        ("Compound Class 2", "compound_class2"),
        ("Method 1", "method1"),
        ("Method 2", "method2"),
        ("Ion Trace (Method 1)", "method1_iontrace"),
        ("Ion Trace (Method 2)", "method2_iontrace"),
        ("Peak", "peak"),
        ("CAS", "cas"),
        ("Molecular Formula", "formula"),
        ("InChI", "inchi"),
        ("Source File", "source"),
    ]
    for label, key in fields:
        val = entry.get(key, "")
        if not val:
            continue
        display = escape(val)
        if key == "cas" and val:
            display = f'<a href="https://commonchemistry.cas.org/detail?cas_rn={url_quote(val)}" target="_blank" rel="noopener">{escape(val)}</a>'
        elif key == "inchi" and val:
            display = f'<span style="font-family:monospace;font-size:0.78rem;word-break:break-all">{escape(val)}</span>'
        info_rows.append(f'<tr><th>{escape(label)}</th><td>{display}</td></tr>')

    info_html = "\\n".join(info_rows)

    # Structure graphic
    graphic_html = ""
    if entry["structure_graphic"]:
        gfx_url = f"/api/compounds/graphics/{entry['structure_graphic']}"
        graphic_html = (
            f'<div class="graphic-box">'
            f'<h2>Structure</h2>'
            f'<img src="{escape(gfx_url)}" alt="Structure of {escape(name)}" />'
            f'</div>'
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <title>{escape(title)}</title>
    <style>
        :root {{ color-scheme: light; }}
        body {{
            font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
            margin: 0; padding: 1.5rem 2rem; background: #f5f6fa;
        }}
        .card {{
            background: #fff; border-radius: 8px; padding: 1.2rem 1.5rem;
            box-shadow: 0 1px 4px rgba(15,23,42,0.06); margin-bottom: 1rem;
        }}
        h1 {{ margin-top: 0; font-size: 1.4rem; }}
        h2 {{ font-size: 1.1rem; margin-top: 0; }}
        .meta {{
            font-size: 0.85rem; color: #6b7280; margin-bottom: 0.5rem;
        }}
        .meta a {{ color: #2563eb; text-decoration: none; }}
        .meta a:hover {{ text-decoration: underline; }}
        .info-table {{ border-collapse: collapse; width: 100%; font-size: 0.85rem; }}
        .info-table th {{
            text-align: right; padding: 0.3rem 0.8rem 0.3rem 0;
            color: #475569; font-weight: 600; white-space: nowrap; vertical-align: top;
            width: 160px;
        }}
        .info-table td {{ padding: 0.3rem 0; color: #1e293b; }}
        .graphic-box {{ text-align: center; }}
        .graphic-box img {{
            max-width: 400px; max-height: 350px; border: 1px solid #e5e7eb;
            border-radius: 6px; padding: 0.5rem; background: #fff;
        }}
        a {{ color: #2563eb; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <div class="card">
        <h1>{escape(name)}</h1>
        <div class="meta">
            <a href="/web/compounds">&larr; All compounds</a>
            &nbsp;|&nbsp;
            <a href="/web/explorer">Data Explorer</a>
            &nbsp;|&nbsp;
            <a href="/web/presence">Presence Matrix</a>
        </div>
        <table class="info-table">{info_html}</table>
    </div>
    {graphic_html if graphic_html else ""}
</body>
</html>"""
    return HTMLResponse(content=html)
