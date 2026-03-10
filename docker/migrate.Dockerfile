# syntax=docker/dockerfile:1.7
FROM python:3.13-slim
WORKDIR /work

# Install only what migrunner needs
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install psycopg2-binary==2.9.11

# Copy migration runner and migrations directory
COPY migrunner.py /work/migrunner.py
COPY migrations/ /work/migrations/

ENV PYTHONUNBUFFERED=1
CMD ["python", "/work/migrunner.py"]
