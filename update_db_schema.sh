#!/bin/bash
# Idempotent migration script for Survey Automation schema
# Creates batch_logs, batch_screenshots, enforces Top Surveys only, and sets default URL.

set -e

CONTAINER_NAME="postgres_db"
DB_USER="airflow"
DB_NAME="messages"

echo "🔍 Checking if container '$CONTAINER_NAME' is running..."
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "❌ Container '$CONTAINER_NAME' is not running. Start Docker Compose first."
    exit 1
fi

echo "✅ Container found. Applying full schema migration..."

# Run the SQL migration inside the container
docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" <<'EOF'

-- =====================================================
-- 1. Create batch_logs and batch_screenshots tables
-- =====================================================
CREATE TABLE IF NOT EXISTS batch_logs (
    log_id     SERIAL PRIMARY KEY,
    batch_id   VARCHAR(100) NOT NULL,
    account_id INTEGER REFERENCES accounts(account_id) ON DELETE CASCADE,
    site_id    INTEGER REFERENCES survey_sites(site_id) ON DELETE CASCADE,
    log_level  VARCHAR(20)  NOT NULL DEFAULT 'INFO',
    message    TEXT         NOT NULL,
    created_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS batch_screenshots (
    screenshot_id SERIAL PRIMARY KEY,
    batch_id      VARCHAR(100) NOT NULL,
    account_id    INTEGER REFERENCES accounts(account_id) ON DELETE CASCADE,
    site_id       INTEGER REFERENCES survey_sites(site_id) ON DELETE CASCADE,
    survey_num    INTEGER      NOT NULL DEFAULT 0,
    stage         VARCHAR(100) NOT NULL,
    label         VARCHAR(255) NOT NULL,
    file_path     TEXT         NOT NULL,
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_batch_logs_batch    ON batch_logs(batch_id);
CREATE INDEX IF NOT EXISTS idx_batch_logs_account  ON batch_logs(account_id);
CREATE INDEX IF NOT EXISTS idx_batch_logs_site     ON batch_logs(site_id);
CREATE INDEX IF NOT EXISTS idx_batch_logs_created  ON batch_logs(created_at);

CREATE INDEX IF NOT EXISTS idx_batch_screenshots_batch   ON batch_screenshots(batch_id);
CREATE INDEX IF NOT EXISTS idx_batch_screenshots_account ON batch_screenshots(account_id);
CREATE INDEX IF NOT EXISTS idx_batch_screenshots_site    ON batch_screenshots(site_id);
CREATE INDEX IF NOT EXISTS idx_batch_screenshots_stage   ON batch_screenshots(stage);

-- =====================================================
-- 2. Enforce only 'Top Surveys' site
-- =====================================================
DELETE FROM survey_sites WHERE site_name != 'Top Surveys';

INSERT INTO survey_sites (site_name, description, is_active)
SELECT 'Top Surveys', 'High‑paying survey platform', TRUE
WHERE NOT EXISTS (SELECT 1 FROM survey_sites WHERE site_name = 'Top Surveys');

-- =====================================================
-- 3. Set default URL for all accounts (Top Surveys)
-- =====================================================
DO $$
DECLARE
    site_id_var INTEGER;
    acc_record RECORD;
BEGIN
    SELECT site_id INTO site_id_var FROM survey_sites WHERE site_name = 'Top Surveys';
    IF site_id_var IS NULL THEN
        RAISE EXCEPTION 'Top Surveys site not found after insert';
    END IF;

    FOR acc_record IN SELECT account_id FROM accounts LOOP
        INSERT INTO account_urls (account_id, site_id, url, is_default, is_used, notes)
        VALUES (acc_record.account_id, site_id_var, 'https://app.topsurveys.app/', TRUE, FALSE, 'Default Top Surveys URL')
        ON CONFLICT (account_id, site_id, url) DO UPDATE
        SET is_default = TRUE, is_used = FALSE, notes = EXCLUDED.notes, updated_at = CURRENT_TIMESTAMP;
    END LOOP;
END $$;

-- =====================================================
-- 4. Ensure all other required columns (idempotent)
-- =====================================================
ALTER TABLE proxy_configs ADD COLUMN IF NOT EXISTS country VARCHAR(5) DEFAULT 'US';
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS has_cookies BOOLEAN DEFAULT FALSE;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS cookies_last_updated TIMESTAMP;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS active_proxy_id INTEGER;

-- Optional foreign key (skip if already exists)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_accounts_proxy'
    ) THEN
        ALTER TABLE accounts ADD CONSTRAINT fk_accounts_proxy
            FOREIGN KEY (active_proxy_id) REFERENCES proxy_configs(proxy_id) ON DELETE SET NULL;
    END IF;
END $$;

-- =====================================================
-- 5. Verify critical columns in account_cookies (for CookieManager)
-- =====================================================
ALTER TABLE account_cookies ADD COLUMN IF NOT EXISTS domain VARCHAR(255);
ALTER TABLE account_cookies ADD COLUMN IF NOT EXISTS cookies_json TEXT;

CREATE INDEX IF NOT EXISTS idx_account_cookies_account_id ON account_cookies(account_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_name='account_cookies' AND constraint_type='UNIQUE'
          AND constraint_name='uq_account_cookies_account_domain'
    ) THEN
        ALTER TABLE account_cookies ADD CONSTRAINT uq_account_cookies_account_domain
            UNIQUE (account_id, domain);
    END IF;
END $$;

-- Final verification
SELECT '✅ Migration complete! Tables and constraints are ready.' as status;
EOF

if [ $? -eq 0 ]; then
    echo "🎉 All schema changes applied successfully!"
    echo "💡 Next steps:"
    echo "   - Restart your Streamlit app (or refresh the browser)"
    echo "   - Run a new survey batch – logs and screenshots will now be stored in the database."
    echo "   - The Dashboard will no longer show 'relation does not exist' errors."
else
    echo "⚠️ Something went wrong. Check the error message above."
    exit 1
fi