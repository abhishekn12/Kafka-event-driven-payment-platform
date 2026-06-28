-- This script runs once on first container startup
-- Creates separate logical databases for each service
-- (They share one Postgres server but are fully isolated from each other)

-- Payment service database
CREATE DATABASE paymentdb;

-- Inventory service database
CREATE DATABASE inventorydb;

-- Notification service has no DB, so only these two needed
-- orderdb is already created by POSTGRES_DB env var in docker-compose.yml

-- Grant the postgres user access to all databases
GRANT ALL PRIVILEGES ON DATABASE orderdb TO postgres;
GRANT ALL PRIVILEGES ON DATABASE paymentdb TO postgres;
GRANT ALL PRIVILEGES ON DATABASE inventorydb TO postgres;