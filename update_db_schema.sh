#!/bin/bash
# update_db_schema.sh
# Idempotent migration script – adds missing columns to the running PostgreSQL container.

set -e

CONTAINER_NAME="postgres_db"
DB_USER="airflow"
DB_NAME="messages"

echo "🔍 Checking if container '$CONTAINER_NAME' is running..."
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "❌ Container '$CONTAINER_NAME' is not running. Start Docker Compose first."
    exit 1
fi

echo "✅ Container found. Applying schema updates..."

# Run SQL migration inside the container
docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" <<'EOF'
-- Add missing 'country' column to proxy_configs (safe, if already exists does nothing)
ALTER TABLE proxy_configs ADD COLUMN IF NOT EXISTS country VARCHAR(5) DEFAULT 'US';

-- Also ensure accounts table has all required columns (defensive)
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS has_cookies BOOLEAN DEFAULT FALSE;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS cookies_last_updated TIMESTAMP;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS active_proxy_id INTEGER;

-- Optional: Add foreign key if missing (won't error if already present)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_accounts_proxy'
    ) THEN
        ALTER TABLE accounts ADD CONSTRAINT fk_accounts_proxy
            FOREIGN KEY (active_proxy_id) REFERENCES proxy_configs(proxy_id) ON DELETE SET NULL;
    END IF;
END $$;

-- Verify column addition
SELECT '✅ Migration complete. Columns now in proxy_configs:' as status;
SELECT column_name FROM information_schema.columns
WHERE table_name = 'proxy_configs' ORDER BY ordinal_position;
EOF

if [ $? -eq 0 ]; then
    echo "🎉 Database schema updated successfully! No restart required."
    echo "💡 Refresh your Streamlit app – the 'country' column error should be gone."
else
    echo "⚠️ Something went wrong. Check the error message above."
    exit 1
fi