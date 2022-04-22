CREATE SERVER IF NOT EXISTS pgmv$cron_instance FOREIGN DATA WRAPPER postgres_fdw options ( dbname 'postgres', port :'PORT', host :'HOSTNAME', connect_timeout '2', keepalives_count '5' );
CREATE USER MAPPING IF NOT EXISTS for :MODULEOWNER SERVER pgmv$cron_instance OPTIONS (user :'MODULEOWNER', password :'MODULEOWNERPASS');
GRANT USAGE ON FOREIGN SERVER pgmv$cron_instance TO :MODULEOWNER;
CREATE SERVER IF NOT EXISTS pgmv$_instance FOREIGN DATA WRAPPER postgres_fdw options ( dbname :'DBNAME', port :'PORT', host :'HOSTNAME', connect_timeout '2', keepalives_count '5' );
CREATE USER MAPPING IF NOT EXISTS for :MODULEOWNER SERVER pgmv$_instance OPTIONS (user :'MODULEOWNER', password :'MODULEOWNERPASS');
GRANT USAGE ON FOREIGN SERVER pgmv$_instance TO :MODULEOWNER;