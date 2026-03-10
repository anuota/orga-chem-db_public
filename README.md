# Scientific Sample Database Framework

This repository contains a **public demonstration version** of a database framework designed for managing **sample-based scientific data**.

The project provides a structured backend for storing, querying, and managing analytical results and associated metadata using **PostgreSQL** and a modular Python codebase.

This repository includes the **core architecture and example configuration**, but **does not contain private data, credentials, or institutional deployment settings**.

---

# Overview

The framework is designed to support workflows where many analytical results are linked to physical samples or measurements.

Typical use cases include:

- laboratory measurements
- analytical chemistry datasets
- geoscience sample databases
- multi-stage analysis pipelines

The project focuses on providing:

- a clear database schema
- reproducible schema migrations
- programmatic database access
- containerized development environments
- optional API access for applications

---

# Main Components

- **PostgreSQL database** – primary data storage  
- **Migration runner** – reproducible schema management  
- **Repository layer (Python + psycopg)** – structured database access  
- **Optional API service** – programmatic interface to the database  
- **Docker environment** – reproducible local setup  
- **CI workflows** – automated testing  

---

# Repository Structure

```
api/                     API service
db_code/                 database layer and utilities
compose/                 Docker configuration
migrunner.py             database migration runner
manage.py                project management utilities
.env.example             example environment configuration
```

---

# Quick Start (Demo Setup)

### 1. Clone the repository

```bash
git clone git clone https://github.com/anuota/orga-chem-db_public.git
cd <repo>
```

---

### 2. Create environment configuration

```bash
cp .env.example .env
```

Edit `.env` if needed.

Example configuration:

```
PGHOST=localhost
PGPORT=5432
PGDATABASE=example_database
PGUSER=example_user
PGPASSWORD=change-me
```

---

### 3. Start services with Docker

```bash
docker compose up
```

This starts the PostgreSQL database and supporting services defined in the `compose/` directory.

---

### 4. Run database migrations

```bash
python migrunner.py
```

This will initialize the database schema.

---

# Continuous Integration

The repository includes CI workflows that automatically:

- start a temporary PostgreSQL instance
- apply database migrations
- run automated tests

No production credentials are stored in this repository.

---

# Public Demo Version

This repository is a **sanitized public version** of an internal development project.

The following elements are intentionally **not included**:

- private datasets
- institutional deployment infrastructure
- production configuration files
- credentials or access tokens

---

# License

See the `LICENSE` file for licensing information.

---

# Contributing

This repository is primarily provided as a **reference implementation and demonstration** of the database framework.

Issues and suggestions are welcome.
