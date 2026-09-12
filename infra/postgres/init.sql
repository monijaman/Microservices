-- This script runs ONCE, automatically, the first time the postgres
-- container starts (Postgres's docker image runs every .sql/.sh file in
-- /docker-entrypoint-initdb.d/ on a fresh volume).
--
-- POSTGRES_DB=orderdb is already created for us by the postgres image
-- itself (see docker-compose.yml), so here we only need to create the
-- other two databases. Each service will connect to exactly one of these
-- and will never reach into another service's database directly — that
-- isolation is the whole point of "database per service".

CREATE DATABASE inventorydb OWNER appuser;
CREATE DATABASE paymentdb OWNER appuser;
