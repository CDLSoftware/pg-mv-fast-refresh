#!/bin/sh
# Remove last line "shared_preload_libraries='citus'"
sed -i '$ d' /var/lib/postgresql/data/postgresql.conf
cat <<EOT >> /var/lib/postgresql/data/postgresql.conf
shared_preload_libraries='pg_cron'
cron.database_name='postgres'
EOT
# Required to load pg_cron
/usr/lib/postgresql/12/bin/pg_ctl -D /var/lib/postgresql/data restart