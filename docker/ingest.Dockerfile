# syntax=docker/dockerfile:1.7
FROM python:3.13-slim

WORKDIR /app

# System certs (psycopg2-binary bundles libpq)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY db_code/requirements.txt /app/db_requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install -r /app/db_requirements.txt

# Copy just what we need
COPY db_code/ /app/db_code/
COPY web/ /app/web/
COPY config/ /app/config/

# Let db_code.load_env() pick this up when needed
ENV ENV_FILE=/app/.env

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app
# Run full ingest, seed ACL, then load FT/Isotope lab datasets if configured.
CMD ["bash", "-lc", "python db_code/create_db_milti_table.py --reset; python -m db_code.acl_seed /app/config/acl_seed.toml; if [ -d \"${ORG_CHEM_FT_ROOT:-/ftdata}\" ]; then python -m db_code.services.special_ingest ft --root-dir \"${ORG_CHEM_FT_ROOT:-/ftdata}\"; python -m db_code.services.special_ingest isotope --root-dir \"${ORG_CHEM_FT_ROOT:-/ftdata}\"; else echo 'FT root not found; skipping FT/isotope ingest'; fi"]
