def make_entries_view_ddl(
    table: str, json_col: str, view_name: str | None = None
) -> str:
    """
    Create a view that flattens `<table>(samplenumber, <json_col> JSONB)` into rows of
      (samplenumber, measured_by, type, date, fraction, notes, data)
    by expanding `<json_col>->'entries'`.
    """
    if view_name is None:
        view_name = f"{table}_entries"

    return f"""
DROP VIEW IF EXISTS public.{view_name};
CREATE VIEW public.{view_name} AS
SELECT
  t.samplenumber AS samplenumber,
    e->>'name'        AS name,
  e->>'measured_by' AS measured_by,
  e->>'type'        AS type,
  e->>'date'        AS date,
  e->>'fraction'    AS fraction,
  e->>'instrument'  AS instrument,
  e->>'data_type'   AS data_type,
    COALESCE(e->>'method', e->>'data_type') AS method,
  e->>'notes'       AS notes,
  e->'data'         AS data
FROM public.{table} t
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.{json_col}->'entries', '[]'::jsonb)) AS e;
"""


def make_presence_view_ddl(view_name: str, tables: list[str]) -> str:
    """
    Generate DDL to create or replace a view that shows, for each (samplenumber),
    which analysis tables contain data. Outputs boolean columns like has_<table>.
    """
    if not tables:
        raise ValueError("tables list cannot be empty")
    # de-duplicate while preserving order
    seen = set()
    uniq = []
    for t in tables:
        if t not in seen:
            seen.add(t)
            uniq.append(t)

    unions = "\n    UNION\n    ".join(
        [f"SELECT samplenumber FROM public.{t}" for t in uniq]
    )

    exists_cols = ",\n       ".join(
        [
            f"EXISTS (SELECT 1 FROM public.{t} t WHERE t.samplenumber = sk.samplenumber) AS has_{t}"
            for t in uniq
        ]
    )

    return f"""
CREATE OR REPLACE VIEW public.{view_name} AS
WITH sample_keys AS (
    {unions}
)
SELECT sk.samplenumber,
       {exists_cols}
FROM sample_keys sk
GROUP BY sk.samplenumber
ORDER BY sk.samplenumber;
"""


def make_presence_view_with_links(
    view_name: str, table_jsoncols: dict[str, str]
) -> str:
    """
    Create/replace a view that, for each samplenumber, reports for each table:
      - has_<table> (boolean)
      - <table>_count (integer)
      - <table>_entries (jsonb)
    """
    if not table_jsoncols:
        raise ValueError("table_jsoncols cannot be empty")
    # de-duplicate while preserving order
    seen = set()
    uniq_items: list[tuple[str, str]] = []
    for t, jc in table_jsoncols.items():
        if t not in seen:
            seen.add(t)
            uniq_items.append((t, jc))

    unions = "\n    UNION\n    ".join(
        [f"SELECT samplenumber FROM public.{t}" for t, _ in uniq_items]
    )

    parts = []
    for t, jc in uniq_items:
        parts.append(
            f"""
            -- {t}
            EXISTS (
                SELECT 1 FROM public.{t} t
                WHERE t.samplenumber = sk.samplenumber
            ) AS has_{t},
            (
                SELECT COALESCE(COUNT(*), 0)
                FROM public.{t} t
                CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.{jc}->'entries', '[]'::jsonb)) AS e
                WHERE t.samplenumber = sk.samplenumber
            ) AS {t}_count,
            (
                SELECT COALESCE(jsonb_agg(e), '[]'::jsonb)
                FROM public.{t} t
                CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.{jc}->'entries', '[]'::jsonb)) AS e
                WHERE t.samplenumber = sk.samplenumber
            ) AS {t}_entries
            """.strip()
        )

    cols_block = ",\n       ".join(parts)

    return f"""
CREATE OR REPLACE VIEW public.{view_name} AS
WITH sample_keys AS (
    {unions}
)
SELECT sk.samplenumber,
       {cols_block}
FROM sample_keys sk
GROUP BY sk.samplenumber
ORDER BY sk.samplenumber;
"""
