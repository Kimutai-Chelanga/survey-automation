#!/bin/bash

# =====================================================
# Docker PostgreSQL Schema Migration Runner
# Specifically for Docker Compose environments
# =====================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Docker PostgreSQL Schema Migration${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null && ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker/docker-compose not found${NC}"
    exit 1
fi

# Use docker compose (newer) or docker-compose (older)
if docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    DOCKER_COMPOSE="docker-compose"
fi

echo -e "${GREEN}✓ Using: $DOCKER_COMPOSE${NC}"
echo ""

# Check if postgres container is running
if ! $DOCKER_COMPOSE ps postgres | grep -q "Up"; then
    echo -e "${RED}✗ PostgreSQL container is not running${NC}"
    echo ""
    echo "Start it with:"
    echo "  $DOCKER_COMPOSE up -d postgres"
    exit 1
fi

echo -e "${GREEN}✓ PostgreSQL container is running${NC}"
echo ""

# Create migration SQL file
MIGRATION_FILE="migration.sql"
echo -e "${YELLOW}Creating migration SQL...${NC}"

cat > "$MIGRATION_FILE" << 'EOF'
-- PostgreSQL Schema Migration for Docker Environment
BEGIN;

-- Add missing columns
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'links' AND column_name = 'executed') THEN
        ALTER TABLE links ADD COLUMN executed BOOLEAN DEFAULT FALSE;
        RAISE NOTICE '✓ Added executed column';
    ELSE
        RAISE NOTICE '- executed column exists';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'links' AND column_name = 'workflow_processed_time') THEN
        ALTER TABLE links ADD COLUMN workflow_processed_time TIMESTAMP;
        RAISE NOTICE '✓ Added workflow_processed_time column';
    ELSE
        RAISE NOTICE '- workflow_processed_time column exists';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'links' AND column_name = 'workflow_status') THEN
        ALTER TABLE links ADD COLUMN workflow_status VARCHAR(50) DEFAULT 'pending';
        RAISE NOTICE '✓ Added workflow_status column';
    ELSE
        RAISE NOTICE '- workflow_status column exists';
    END IF;
END $$;

-- Add indexes
CREATE INDEX IF NOT EXISTS idx_links_executed ON links(executed);
CREATE INDEX IF NOT EXISTS idx_links_workflow_status ON links(workflow_status);
CREATE INDEX IF NOT EXISTS idx_links_reversal_query ON links(workflow_type, account_id, tweeted_date, executed);
CREATE INDEX IF NOT EXISTS idx_links_workflow_processed_time ON links(workflow_processed_time) WHERE workflow_processed_time IS NOT NULL;

-- Update existing data
DO $$
DECLARE
    updated_count INTEGER;
BEGIN
    UPDATE links SET executed = TRUE WHERE workflow_processed_time IS NOT NULL AND executed = FALSE;
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    IF updated_count > 0 THEN
        RAISE NOTICE '✓ Updated % links to executed=TRUE', updated_count;
    END IF;

    UPDATE links SET workflow_status = 'completed' WHERE workflow_status IS NULL OR workflow_status = '';
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    IF updated_count > 0 THEN
        RAISE NOTICE '✓ Normalized % workflow_status values', updated_count;
    END IF;
END $$;

-- Add constraint
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.constraint_column_usage WHERE constraint_name = 'links_workflow_status_check') THEN
        ALTER TABLE links DROP CONSTRAINT links_workflow_status_check;
    END IF;
    ALTER TABLE links ADD CONSTRAINT links_workflow_status_check CHECK (workflow_status IN ('pending', 'processing', 'completed', 'failed', 'error'));
EXCEPTION WHEN duplicate_object THEN
    RAISE NOTICE '- Constraint already exists';
END $$;

-- Create views
DROP VIEW IF EXISTS links_execution_status CASCADE;
CREATE VIEW links_execution_status AS
SELECT 
    workflow_type,
    workflow_status,
    executed,
    COUNT(*) as count,
    MIN(workflow_processed_time) as earliest_processed,
    MAX(workflow_processed_time) as latest_processed,
    MIN(tweeted_date) as earliest_tweet,
    MAX(tweeted_date) as latest_tweet
FROM links
GROUP BY workflow_type, workflow_status, executed
ORDER BY workflow_type, executed DESC, workflow_status;

DROP VIEW IF EXISTS links_reversal_candidates CASCADE;
CREATE VIEW links_reversal_candidates AS
SELECT 
    links_id, link, tweet_id, workflow_type, workflow_status,
    account_id, tweeted_date, workflow_processed_time, executed
FROM links
WHERE executed = TRUE
ORDER BY workflow_processed_time DESC;

-- Summary
DO $$
DECLARE
    total_links INTEGER;
    executed_links INTEGER;
    pending_links INTEGER;
    completed_links INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_links FROM links;
    SELECT COUNT(*) INTO executed_links FROM links WHERE executed = TRUE;
    SELECT COUNT(*) INTO pending_links FROM links WHERE workflow_status = 'pending';
    SELECT COUNT(*) INTO completed_links FROM links WHERE workflow_status = 'completed';

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'MIGRATION COMPLETED SUCCESSFULLY';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Total links: %', total_links;
    RAISE NOTICE 'Executed links: %', executed_links;
    RAISE NOTICE 'Pending links: %', pending_links;
    RAISE NOTICE 'Completed links: %', completed_links;
    RAISE NOTICE '========================================';
END $$;

COMMIT;
EOF

echo -e "${GREEN}✓ Created $MIGRATION_FILE${NC}"
echo ""

# Create backup
BACKUP_FILE="backup_$(date +%Y%m%d_%H%M%S).sql"
echo -e "${YELLOW}Creating backup...${NC}"

if $DOCKER_COMPOSE exec -T postgres pg_dump -U airflow messages > "$BACKUP_FILE" 2>/dev/null; then
    echo -e "${GREEN}✓ Backup saved to $BACKUP_FILE${NC}"
else
    echo -e "${YELLOW}⚠ Backup failed, but continuing...${NC}"
fi
echo ""

# Copy migration file to container
echo -e "${YELLOW}Copying migration to container...${NC}"
$DOCKER_COMPOSE cp "$MIGRATION_FILE" postgres:/tmp/migration.sql

echo -e "${GREEN}✓ File copied${NC}"
echo ""

# Run migration
echo -e "${YELLOW}Running migration...${NC}"
echo ""

if $DOCKER_COMPOSE exec -T postgres psql -U airflow -d messages -f /tmp/migration.sql; then
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}✓✓✓ MIGRATION COMPLETED ✓✓✓${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo "Files created:"
    echo "  - Migration: $MIGRATION_FILE"
    [ -f "$BACKUP_FILE" ] && echo "  - Backup: $BACKUP_FILE"
    echo ""
    echo "Verify with:"
    echo "  $DOCKER_COMPOSE exec postgres psql -U airflow -d messages -c 'SELECT * FROM links_execution_status;'"
    echo ""
    echo "Test reversal function:"
    echo "  $DOCKER_COMPOSE exec postgres psql -U airflow -d messages -c '\\d links'"
    echo ""
else
    echo ""
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}✗ MIGRATION FAILED${NC}"
    echo -e "${RED}========================================${NC}"
    echo ""
    if [ -f "$BACKUP_FILE" ]; then
        echo "Restore backup with:"
        echo "  cat $BACKUP_FILE | $DOCKER_COMPOSE exec -T postgres psql -U airflow -d messages"
    fi
    exit 1
fi

# Cleanup temp file in container
$DOCKER_COMPOSE exec postgres rm -f /tmp/migration.sql 2>/dev/null || true

# Keep local migration file for reference
echo -e "${BLUE}Migration file kept: $MIGRATION_FILE${NC}"
echo -e "${BLUE}You can delete it after verification${NC}"