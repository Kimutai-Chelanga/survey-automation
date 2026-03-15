#!/bin/bash

# Reset Filter Links DAG State Script
# Reverses all PostgreSQL and MongoDB changes made by filter_links DAG
# This allows you to rerun the filter_links DAG from scratch

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
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo "========================================================================"
echo "Filter Links DAG State Reset Script"
echo "========================================================================"
echo ""
echo -e "${YELLOW}This script will reverse all database changes made by filter_links DAG${NC}"
echo -e "${YELLOW}including workflow assignments and link processing${NC}"
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
echo "STEP 1: PostgreSQL - Resetting Links Table"
echo "========================================================================"
echo ""

echo -e "${BLUE}Getting current links state...${NC}"
docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME << 'EOF'
SELECT 
    COUNT(*) as total_links,
    COUNT(CASE WHEN tweeted_time IS NOT NULL THEN 1 END) as with_timestamp,
    COUNT(CASE WHEN tweeted_date IS NOT NULL THEN 1 END) as with_date,
    COUNT(CASE WHEN within_limit = TRUE THEN 1 END) as within_limit,
    COUNT(CASE WHEN used = TRUE THEN 1 END) as used,
    COUNT(CASE WHEN filtered = TRUE THEN 1 END) as filtered,
    COUNT(CASE WHEN processed_by_workflow = TRUE THEN 1 END) as processed,
    COUNT(CASE WHEN executed = TRUE THEN 1 END) as executed
FROM links;
EOF

echo ""
echo -e "${CYAN}Resetting links to unprocessed state...${NC}"

docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME << 'EOF'
-- Reset all filter_links DAG related fields
UPDATE links SET 
    -- Timestamp fields (set by update_tweeted_time_and_workflows)
    tweeted_time = NULL,
    tweeted_date = NULL,
    
    -- Flag fields
    within_limit = FALSE,
    used = FALSE,
    used_time = NULL,
    filtered = FALSE,
    filtered_time = NULL,
    
    -- Workflow processing fields
    processed_by_workflow = FALSE,
    workflow_processed_time = NULL,
    workflow_status = NULL,
    workflow_type = NULL,
    workflow_id = NULL,
    mongo_workflow_id = NULL,
    
    -- Execution fields
    executed = FALSE,
    execution_time = NULL
WHERE TRUE;

-- Show results
SELECT 
    COUNT(*) as total_links,
    COUNT(CASE WHEN tweeted_time IS NOT NULL THEN 1 END) as with_timestamp,
    COUNT(CASE WHEN within_limit = TRUE THEN 1 END) as within_limit,
    COUNT(CASE WHEN used = TRUE THEN 1 END) as used,
    COUNT(CASE WHEN processed_by_workflow = TRUE THEN 1 END) as processed
FROM links;
EOF

echo -e "${GREEN}✓ Links table reset complete${NC}"
echo ""

echo "========================================================================"
echo "STEP 2: MongoDB - Resetting Workflow Metadata"
echo "========================================================================"
echo ""

echo -e "${BLUE}Getting current workflow_metadata state...${NC}"
docker exec -i $MONGO_CONTAINER mongosh \
    --username $MONGO_USER --password $MONGO_PASS \
    --authenticationDatabase $MONGO_AUTH_DB \
    --quiet $MONGO_DB << 'EOF'
print("Current workflow_metadata state:");
const stats = db.workflow_metadata.aggregate([
    {
        $group: {
            _id: null,
            total: { $sum: 1 },
            with_links: { $sum: { $cond: ["$has_link", 1, 0] } },
            ready_to_execute: { $sum: { $cond: [{ $eq: ["$status", "ready_to_execute"] }, 1, 0] } },
            executed: { $sum: { $cond: ["$executed", 1, 0] } },
            successful: { $sum: { $cond: ["$success", 1, 0] } }
        }
    }
]).toArray();
printjson(stats);
EOF

echo ""
echo -e "${CYAN}Resetting workflow_metadata to unassigned state...${NC}"

docker exec -i $MONGO_CONTAINER mongosh \
    --username $MONGO_USER --password $MONGO_PASS \
    --authenticationDatabase $MONGO_AUTH_DB \
    --quiet $MONGO_DB << 'EOF'
// Reset workflow_metadata - remove all link assignments
const result = db.workflow_metadata.updateMany(
    {},
    {
        $set: {
            // Reset link assignment
            has_link: false,
            link_url: null,
            link_assigned_at: null,
            
            // Reset content tracking
            postgres_content_id: null,
            content_preview: null,
            content_hash: null,
            
            // Reset execution status
            status: "generated",
            execute: false,
            executed: false,
            success: false,
            execution_attempts: 0,
            
            // Update timestamp
            updated_at: new Date(),
            
            // Clear assignment metadata
            assignment_method: null,
            assignment_source: null,
            tweeted_date: null
        },
        $unset: {
            link_id: "",
            workflow_processed_time: "",
            execution_time: "",
            actual_execution_time: "",
            error_message: ""
        }
    }
);

print("Reset " + result.modifiedCount + " workflow_metadata records");
EOF

echo -e "${GREEN}✓ Workflow metadata reset complete${NC}"
echo ""

echo "========================================================================"
echo "STEP 3: MongoDB - Resetting Automa Workflows"
echo "========================================================================"
echo ""

echo -e "${BLUE}Getting current automa_workflows state...${NC}"
docker exec -i $MONGO_CONTAINER mongosh \
    --username $MONGO_USER --password $MONGO_PASS \
    --authenticationDatabase $MONGO_AUTH_DB \
    --quiet $MONGO_DB << 'EOF'
const stats = db.automa_workflows.aggregate([
    {
        $group: {
            _id: null,
            total: { $sum: 1 },
            with_real_link: { $sum: { $cond: ["$has_real_link", 1, 0] } },
            link_assigned: { $sum: { $cond: ["$link_assigned", 1, 0] } }
        }
    }
]).toArray();
printjson(stats);
EOF

echo ""
echo -e "${CYAN}Resetting automa_workflows link assignments...${NC}"

docker exec -i $MONGO_CONTAINER mongosh \
    --username $MONGO_USER --password $MONGO_PASS \
    --authenticationDatabase $MONGO_AUTH_DB \
    --quiet $MONGO_DB << 'EOF'
// Reset automa_workflows - remove link assignments
const result = db.automa_workflows.updateMany(
    { has_real_link: true },
    {
        $set: {
            has_real_link: false,
            link_assigned: false
        },
        $unset: {
            assigned_link: "",
            assigned_content_id: "",
            assignment_time: ""
        }
    }
);

print("Reset " + result.modifiedCount + " automa_workflows records");
EOF

echo -e "${GREEN}✓ Automa workflows reset complete${NC}"
echo ""

echo "========================================================================"
echo "STEP 4: MongoDB - Clearing Content Workflow Links"
echo "========================================================================"
echo ""

echo -e "${BLUE}Counting content_workflow_links...${NC}"
LINKS_COUNT=$(docker exec -i $MONGO_CONTAINER mongosh \
    --username $MONGO_USER --password $MONGO_PASS \
    --authenticationDatabase $MONGO_AUTH_DB \
    --quiet $MONGO_DB \
    --eval "db.content_workflow_links.countDocuments({})")

echo "  Found $LINKS_COUNT content workflow links"

if [ "$LINKS_COUNT" -gt 0 ]; then
    echo -e "${CYAN}Deleting content_workflow_links...${NC}"
    docker exec -i $MONGO_CONTAINER mongosh \
        --username $MONGO_USER --password $MONGO_PASS \
        --authenticationDatabase $MONGO_AUTH_DB \
        --quiet $MONGO_DB \
        --eval "db.content_workflow_links.deleteMany({})"
    echo -e "${GREEN}✓ Deleted $LINKS_COUNT content workflow links${NC}"
else
    echo -e "${GREEN}✓ No content workflow links to delete${NC}"
fi
echo ""

echo "========================================================================"
echo "STEP 5: MongoDB - Clearing Multi-Type Execution Batches"
echo "========================================================================"
echo ""

echo -e "${BLUE}Counting multi_type_execution_batches...${NC}"
BATCHES_COUNT=$(docker exec -i $MONGO_CONTAINER mongosh \
    --username $MONGO_USER --password $MONGO_PASS \
    --authenticationDatabase $MONGO_AUTH_DB \
    --quiet $MONGO_DB \
    --eval "db.multi_type_execution_batches.countDocuments({})")

echo "  Found $BATCHES_COUNT execution batches"

if [ "$BATCHES_COUNT" -gt 0 ]; then
    echo -e "${CYAN}Deleting multi_type_execution_batches...${NC}"
    docker exec -i $MONGO_CONTAINER mongosh \
        --username $MONGO_USER --password $MONGO_PASS \
        --authenticationDatabase $MONGO_AUTH_DB \
        --quiet $MONGO_DB \
        --eval "db.multi_type_execution_batches.deleteMany({})"
    echo -e "${GREEN}✓ Deleted $BATCHES_COUNT execution batches${NC}"
else
    echo -e "${GREEN}✓ No execution batches to delete${NC}"
fi
echo ""

echo "========================================================================"
echo "STEP 6: MongoDB - Resetting Workflow Executions"
echo "========================================================================"
echo ""

echo -e "${BLUE}Counting workflow_executions...${NC}"
EXEC_COUNT=$(docker exec -i $MONGO_CONTAINER mongosh \
    --username $MONGO_USER --password $MONGO_PASS \
    --authenticationDatabase $MONGO_AUTH_DB \
    --quiet $MONGO_DB \
    --eval "db.workflow_executions.countDocuments({})")

echo "  Found $EXEC_COUNT workflow execution records"

if [ "$EXEC_COUNT" -gt 0 ]; then
    echo -e "${CYAN}Deleting workflow_executions...${NC}"
    docker exec -i $MONGO_CONTAINER mongosh \
        --username $MONGO_USER --password $MONGO_PASS \
        --authenticationDatabase $MONGO_AUTH_DB \
        --quiet $MONGO_DB \
        --eval "db.workflow_executions.deleteMany({})"
    echo -e "${GREEN}✓ Deleted $EXEC_COUNT workflow execution records${NC}"
else
    echo -e "${GREEN}✓ No workflow executions to delete${NC}"
fi
echo ""

echo "========================================================================"
echo "STEP 7: Verification - Checking Reset State"
echo "========================================================================"
echo ""

echo -e "${BLUE}PostgreSQL Links State:${NC}"
docker exec -i $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME << 'EOF'
SELECT 
    COUNT(*) as total_links,
    COUNT(CASE WHEN tweeted_time IS NOT NULL THEN 1 END) as with_timestamp,
    COUNT(CASE WHEN tweeted_date IS NOT NULL THEN 1 END) as with_date,
    COUNT(CASE WHEN within_limit = TRUE THEN 1 END) as within_limit,
    COUNT(CASE WHEN used = TRUE THEN 1 END) as used,
    COUNT(CASE WHEN filtered = TRUE THEN 1 END) as filtered,
    COUNT(CASE WHEN processed_by_workflow = TRUE THEN 1 END) as processed,
    COUNT(CASE WHEN executed = TRUE THEN 1 END) as executed,
    COUNT(CASE WHEN workflow_id IS NOT NULL THEN 1 END) as with_workflow_id
FROM links;
EOF

echo ""
echo -e "${BLUE}MongoDB Collections State:${NC}"
docker exec -i $MONGO_CONTAINER mongosh \
    --username $MONGO_USER --password $MONGO_PASS \
    --authenticationDatabase $MONGO_AUTH_DB \
    --quiet $MONGO_DB << 'EOF'
print("\nCollection Name                      | Document Count");
print("--------------------------------------------------------");
print("workflow_metadata (total)            | " + db.workflow_metadata.countDocuments({}));
print("workflow_metadata (has_link=true)    | " + db.workflow_metadata.countDocuments({has_link: true}));
print("workflow_metadata (executed=true)    | " + db.workflow_metadata.countDocuments({executed: true}));
print("automa_workflows (has_real_link)     | " + db.automa_workflows.countDocuments({has_real_link: true}));
print("content_workflow_links               | " + db.content_workflow_links.countDocuments({}));
print("multi_type_execution_batches         | " + db.multi_type_execution_batches.countDocuments({}));
print("workflow_executions                  | " + db.workflow_executions.countDocuments({}));
EOF

echo ""
echo "========================================================================"
echo -e "${GREEN}✓ Filter Links DAG State Reset Complete!${NC}"
echo "========================================================================"
echo ""
echo "Summary of Changes:"
echo "  ✓ All links reset to unprocessed state"
echo "  ✓ All timestamps and dates cleared"
echo "  ✓ All workflow assignments removed"
echo "  ✓ All execution tracking cleared"
echo "  ✓ workflow_metadata reset to unassigned state"
echo "  ✓ automa_workflows link assignments cleared"
echo "  ✓ content_workflow_links collection cleared"
echo "  ✓ multi_type_execution_batches collection cleared"
echo "  ✓ workflow_executions collection cleared"
echo ""
echo -e "${YELLOW}You can now rerun the filter_links DAG from scratch${NC}"
echo ""
echo "Next Steps:"
echo "  1. Trigger the extraction DAG to get fresh links"
echo "  2. Run filter_links DAG to process and assign workflows"
echo "  3. Run executor DAG to execute the assigned workflows"
echo ""