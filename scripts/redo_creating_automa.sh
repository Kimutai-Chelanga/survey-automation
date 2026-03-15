#!/bin/bash

# Reset DAG State Script - Reverses all PostgreSQL and MongoDB changes
# This script allows you to rerun the create_automa_account_centric DAG from scratch

# Database connection details
DB_HOST="${DB_HOST:-postgres}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-messages}"
DB_USER="${DB_USER:-airflow}"
DB_PASSWORD="${DB_PASSWORD:-airflow}"

# MongoDB connection details
MONGO_HOST="${MONGO_HOST:-mongodb}"
MONGO_PORT="${MONGO_PORT:-27017}"
MONGO_DB="${MONGO_DB:-messages_db}"
MONGO_USER="${MONGO_USER:-admin}"
MONGO_PASS="${MONGO_PASS:-admin123}"
MONGO_AUTH_DB="${MONGO_AUTH_DB:-admin}"

# Docker containers
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgres_db}"
MONGO_CONTAINER="${MONGO_CONTAINER:-mongodb}"

# Color output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "========================================================================"
echo "DAG State Reset Script - PostgreSQL & MongoDB"
echo "========================================================================"
echo ""
echo -e "${YELLOW}Resetting all workflow generation state...${NC}"
echo ""

# Check if Docker containers are running
echo -e "${BLUE}Checking Docker containers...${NC}"
if ! docker ps | grep -q "$POSTGRES_CONTAINER"; then
    echo -e "${RED}✗ PostgreSQL container '$POSTGRES_CONTAINER' is not running${NC}"
    exit 1
fi

if ! docker ps | grep -q "$MONGO_CONTAINER"; then
    echo -e "${RED}✗ MongoDB container '$MONGO_CONTAINER' is not running${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker containers are running${NC}"
echo ""

echo "========================================================================"
echo "STEP 1: PostgreSQL - Resetting Content Tables"
echo "========================================================================"
echo ""

# Function to reset content table
reset_content_table() {
    local table=$1
    local id_field=$2
    local description=$3
    
    echo -e "${BLUE}Processing: $description${NC}"
    
    # Get counts before reset
    TOTAL=$(docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c \
        "SELECT COUNT(*) FROM $table;" | tr -d ' ')
    USED=$(docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c \
        "SELECT COUNT(*) FROM $table WHERE used = TRUE;" | tr -d ' ')
    PROCESSED=$(docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c \
        "SELECT COUNT(*) FROM $table WHERE processed_by_workflow = TRUE;" | tr -d ' ')
    
    echo "  Current state: $TOTAL total, $USED used, $PROCESSED processed by workflow"
    
    if [ "$TOTAL" -gt 0 ]; then
        # Reset all workflow-related fields
        docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME << EOF
UPDATE $table SET 
    used = FALSE,
    used_time = NULL,
    processed_by_workflow = FALSE,
    workflow_processed_time = NULL,
    workflow_id = NULL,
    mongo_workflow_id = NULL,
    workflow_status = 'pending'
WHERE TRUE;
EOF
        
        echo -e "  ${GREEN}✓ Reset $TOTAL records to initial state${NC}"
    else
        echo -e "  ${GREEN}✓ No records to reset${NC}"
    fi
    
    echo ""
}

# Reset each content type
reset_content_table "messages" "messages_id" "Messages Table"
reset_content_table "replies" "replies_id" "Replies Table"
reset_content_table "retweets" "retweets_id" "Retweets Table"
reset_content_table "links" "links_id" "Links Table"

echo "========================================================================"
echo "STEP 2: PostgreSQL - Clearing Workflow Logs"
echo "========================================================================"
echo ""

# Clear workflow_sync_log
echo -e "${BLUE}Clearing workflow_sync_log...${NC}"
SYNC_LOG_COUNT=$(docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c \
    "SELECT COUNT(*) FROM workflow_sync_log;" | tr -d ' ')
echo "  Found $SYNC_LOG_COUNT sync log entries"

if [ "$SYNC_LOG_COUNT" -gt 0 ]; then
    docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME -c \
        "DELETE FROM workflow_sync_log;"
    echo -e "  ${GREEN}✓ Deleted $SYNC_LOG_COUNT sync log entries${NC}"
else
    echo -e "  ${GREEN}✓ No sync log entries to delete${NC}"
fi
echo ""

# Clear workflow_generation_log
echo -e "${BLUE}Clearing workflow_generation_log...${NC}"
GEN_LOG_COUNT=$(docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c \
    "SELECT COUNT(*) FROM workflow_generation_log;" | tr -d ' ')
echo "  Found $GEN_LOG_COUNT generation log entries"

if [ "$GEN_LOG_COUNT" -gt 0 ]; then
    docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME -c \
        "DELETE FROM workflow_generation_log;"
    echo -e "  ${GREEN}✓ Deleted $GEN_LOG_COUNT generation log entries${NC}"
else
    echo -e "  ${GREEN}✓ No generation log entries to delete${NC}"
fi
echo ""

# Clear workflow_runs
echo -e "${BLUE}Clearing workflow_runs...${NC}"
RUNS_COUNT=$(docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME -t -c \
    "SELECT COUNT(*) FROM workflow_runs;" | tr -d ' ')
echo "  Found $RUNS_COUNT workflow run entries"

if [ "$RUNS_COUNT" -gt 0 ]; then
    docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME -c \
        "DELETE FROM workflow_runs;"
    echo -e "  ${GREEN}✓ Deleted $RUNS_COUNT workflow run entries${NC}"
else
    echo -e "  ${GREEN}✓ No workflow run entries to delete${NC}"
fi
echo ""

# Reset account statistics
echo -e "${BLUE}Resetting account workflow statistics...${NC}"
docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME << EOF
UPDATE accounts SET 
    total_replies_processed = 0,
    total_messages_processed = 0,
    total_retweets_processed = 0,
    total_links_processed = 0,
    last_workflow_sync = NULL
WHERE TRUE;
EOF
echo -e "${GREEN}✓ Reset account statistics${NC}"
echo ""

echo "========================================================================"
echo "STEP 3: MongoDB - Deleting Workflow Collections"
echo "========================================================================"
echo ""

# Check MongoDB connection
echo -e "${BLUE}Checking MongoDB connection...${NC}"
if docker exec -i $MONGO_CONTAINER mongosh --username $MONGO_USER --password $MONGO_PASS \
    --authenticationDatabase $MONGO_AUTH_DB \
    --eval "db.adminCommand('ping')" --quiet > /dev/null 2>&1; then
    echo -e "${GREEN}✓ MongoDB connection successful${NC}"
    echo ""
else
    echo -e "${RED}✗ Failed to connect to MongoDB${NC}"
    echo "  Host: $MONGO_HOST:$MONGO_PORT"
    echo "  Database: $MONGO_DB"
    echo "  User: $MONGO_USER"
    exit 1
fi

# Function to clear MongoDB collection
clear_mongo_collection() {
    local collection=$1
    local description=$2
    
    echo -e "${BLUE}Clearing $description...${NC}"
    
    # Get count before deletion
    COUNT=$(docker exec -i $MONGO_CONTAINER mongosh \
        --username $MONGO_USER --password $MONGO_PASS \
        --authenticationDatabase $MONGO_AUTH_DB \
        --quiet $MONGO_DB \
        --eval "db.$collection.countDocuments({})")
    
    echo "  Found $COUNT documents in $collection"
    
    if [ "$COUNT" -gt 0 ]; then
        # Delete all documents
        docker exec -i $MONGO_CONTAINER mongosh \
            --username $MONGO_USER --password $MONGO_PASS \
            --authenticationDatabase $MONGO_AUTH_DB \
            --quiet $MONGO_DB \
            --eval "db.$collection.deleteMany({})"
        
        echo -e "  ${GREEN}✓ Deleted $COUNT documents from $collection${NC}"
    else
        echo -e "  ${GREEN}✓ No documents to delete${NC}"
    fi
    echo ""
}

# Clear all workflow-related MongoDB collections
clear_mongo_collection "automa_workflows" "Automa Workflows Collection"
clear_mongo_collection "workflow_metadata" "Workflow Metadata Collection"
clear_mongo_collection "workflow_executions" "Workflow Executions Collection (if exists)"
clear_mongo_collection "content_workflow_links" "Content Workflow Links Collection (if exists)"
clear_mongo_collection "account_stats" "Account Stats Collection (if exists)"

echo "========================================================================"
echo "STEP 4: Verification - Checking Reset State"
echo "========================================================================"
echo ""

echo -e "${BLUE}PostgreSQL Content State:${NC}"
docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME << EOF
SELECT 
    'Messages' as content_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN used = TRUE THEN 1 END) as used_count,
    COUNT(CASE WHEN processed_by_workflow = TRUE THEN 1 END) as processed_count
FROM messages
UNION ALL
SELECT 
    'Replies' as content_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN used = TRUE THEN 1 END) as used_count,
    COUNT(CASE WHEN processed_by_workflow = TRUE THEN 1 END) as processed_count
FROM replies
UNION ALL
SELECT 
    'Retweets' as content_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN used = TRUE THEN 1 END) as used_count,
    COUNT(CASE WHEN processed_by_workflow = TRUE THEN 1 END) as processed_count
FROM retweets;
EOF

echo ""
echo -e "${BLUE}PostgreSQL Log Tables State:${NC}"
docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME << EOF
SELECT 
    'workflow_sync_log' as table_name,
    COUNT(*) as record_count
FROM workflow_sync_log
UNION ALL
SELECT 
    'workflow_generation_log' as table_name,
    COUNT(*) as record_count
FROM workflow_generation_log
UNION ALL
SELECT 
    'workflow_runs' as table_name,
    COUNT(*) as record_count
FROM workflow_runs;
EOF

echo ""
echo -e "${BLUE}MongoDB Collections State:${NC}"
docker exec -i $MONGO_CONTAINER mongosh \
    --username $MONGO_USER --password $MONGO_PASS \
    --authenticationDatabase $MONGO_AUTH_DB \
    --quiet $MONGO_DB << 'EOF'
print("Collection Name                | Document Count");
print("-------------------------------------------");
print("automa_workflows               | " + db.automa_workflows.countDocuments({}));
print("workflow_metadata              | " + db.workflow_metadata.countDocuments({}));
print("workflow_executions            | " + db.workflow_executions.countDocuments({}));
print("content_workflow_links         | " + db.content_workflow_links.countDocuments({}));
EOF

echo ""
echo "========================================================================"
echo -e "${GREEN}✓ DAG State Reset Complete!${NC}"
echo "========================================================================"
echo ""
echo "Summary:"
echo "  • All content marked as unused and unprocessed"
echo "  • All workflow processing flags cleared"
echo "  • All MongoDB workflow collections cleared"
echo "  • All PostgreSQL workflow logs cleared"
echo "  • Account statistics reset"
echo ""
echo -e "${YELLOW}You can now rerun the create_automa_account_centric DAG${NC}"
echo ""