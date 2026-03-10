# --- 2) Table DDLs (idempotent) ---


# Template for family tables (e.g., steranes, etc.)
def make_family_table_ddl(table: str, json_col: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS public.{table} (
    samplenumber TEXT NOT NULL,
    {json_col}   JSONB NOT NULL,
    PRIMARY KEY (samplenumber)
);
"""
