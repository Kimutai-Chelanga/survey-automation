#!/bin/bash
# Apply database migrations inside the running PostgreSQL container

echo "Applying batch_logs and batch_screenshots migration..."

# Copy migration file into the postgres container
docker cp migrations/001_add_batch_tables.sql postgres_db:/tmp/migration.sql

# Execute the SQL inside the container
docker exec -i postgres_db psql -U airflow -d messages -f /tmp/migration.sql

if [ $? -eq 0 ]; then
    echo "✅ Migration applied successfully."
else
    echo "❌ Migration failed."
    exit 1
fi

# Clean up
docker exec -i postgres_db rm /tmp/migration.sql

echo "Done."