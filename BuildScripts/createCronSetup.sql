CREATE EXTENSION 	IF NOT EXISTS "pg_cron";

ALTER EXTENSION "pg_cron" SET SCHEMA public;

GRANT USAGE ON SCHEMA cron TO postgres;
GRANT USAGE ON SCHEMA cron TO :MODULEOWNER;
GRANT ALL ON SCHEMA cron TO :MODULEOWNER;
GRANT ALL PRIVILEGES ON SCHEMA cron TO :MODULEOWNER;
GRANT ALL ON ALL TABLES in schema cron TO :MODULEOWNER;
GRANT ALL ON ALL sequences in schema cron TO :MODULEOWNER;