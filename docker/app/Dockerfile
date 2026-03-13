# syntax=docker/dockerfile:1.7
FROM python:3.13-slim AS builder
WORKDIR /app
ENV PIP_DEFAULT_TIMEOUT=120 PIP_DISABLE_PIP_VERSION_CHECK=1

# Copy requirements for all submodules used by the app image
COPY api/requirements.txt /app/api_requirements.txt
COPY db_code/requirements.txt /app/db_code_requirements.txt

# Install Python dependencies into user site-packages (to copy later)
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --user --prefer-binary --timeout=120 --retries=5 \
    -r /app/api_requirements.txt \
    -r /app/db_code_requirements.txt

FROM python:3.13-slim
WORKDIR /app
ENV PIP_DEFAULT_TIMEOUT=120 PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PATH=/root/.local/bin:$PATH
# Make repo root importable as a package root
ENV PYTHONPATH=/app

# Bring in site-packages from the builder stage
COPY --from=builder /root/.local /root/.local

# Copy application source
COPY api/ /app/api/
COPY web/ /app/web/
COPY db_code/ /app/db_code/
COPY app/data/ /app/app/data/

# Uvicorn entrypoint
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
