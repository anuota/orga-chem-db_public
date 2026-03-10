# Org Chem DB

Postgres + Python ingestion + FastAPI API. RLS/ACL enforced in DB.

## Dev quick start
1. Use one profile:
   - local Mac: copy `.env.local.example` to `.env.local`
   - Debian server: copy `.env.server.example` to `.env.server`
2. Set secrets and `ORG_CHEM_HOST_DATA_DIR` in that profile file.
3. Start with the matching env file:
   - local: `docker compose --env-file .env.local -f compose/docker-compose.prod.yml up --build`
   - server: `docker compose --env-file .env.server -f compose/docker-compose.prod.yml up --build`
4. Visit `http://localhost:8000/` (redirects to `/web/presence`)

## Structure
- migrations/: SQL migrations (DDL, views, RLS)
- db_code/: ingestion utilities and tests
- api/: FastAPI app (middleware sets app.user)
- docker/, compose/: containerization

## CI/CD (GitLab)
- Pipeline stages: `lint -> unit -> migrate -> build -> deploy`.
- `deploy_gfz` runs manually on `main` and deploys the exact commit SHA to GFZ server via SSH.

Required CI variables for deploy:
- `GFZ_DEPLOY_HOST` (example: `internal-host.gfz-potsdam.de`)
- `GFZ_DEPLOY_USER`
- `GFZ_DEPLOY_PATH` (absolute path to cloned repo on server)
- `GFZ_DEPLOY_SSH_KEY` (private key for the deploy user, masked/protected)

Required CI variables for container registry push:
- `CI_REGISTRY`, `CI_REGISTRY_USER`, `CI_REGISTRY_PASSWORD` (usually provided by GitLab)
