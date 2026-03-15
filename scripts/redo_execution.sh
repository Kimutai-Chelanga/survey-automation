#!/bin/bash

echo "🔄 Setting up links and workflows to meet execution criteria..."

# 1. Update PostgreSQL links
echo "📊 Updating PostgreSQL links..."
docker exec -it postgres_db psql -U airflow -d messages -c "
UPDATE links 
SET within_limit = TRUE,
    filtered = TRUE,
    used = TRUE,
    processed_by_workflow = TRUE,
    executed = FALSE,
    workflow_status = 'completed',
    tweeted_time = NULL,
    used_time = NULL
WHERE links_id IS NOT NULL;
"

# 2. Update MongoDB workflow_metadata
echo "📊 Updating MongoDB workflow_metadata..."
docker exec -it mongodb mongosh -u admin -p admin123 --authenticationDatabase admin messages_db --eval "
db.workflow_metadata.updateMany(
  {},
  {
    \$set: {
      has_link: true,
      has_content: true,
      status: 'ready_to_execute',
      executed: false,
      updated_at: new Date()
    }
  }
)
"

# 3. Verify PostgreSQL
echo "✅ Verifying PostgreSQL links..."
docker exec -it postgres_db psql -U airflow -d messages -c "
SELECT 
    COUNT(*) as total_links,
    SUM(CASE WHEN within_limit = TRUE THEN 1 ELSE 0 END) as within_limit_count,
    SUM(CASE WHEN filtered = TRUE THEN 1 ELSE 0 END) as filtered_count,
    SUM(CASE WHEN used = TRUE THEN 1 ELSE 0 END) as used_count,
    SUM(CASE WHEN processed_by_workflow = TRUE THEN 1 ELSE 0 END) as processed_count,
    SUM(CASE WHEN executed = FALSE THEN 1 ELSE 0 END) as not_executed_count,
    SUM(CASE WHEN workflow_status = 'completed' THEN 1 ELSE 0 END) as completed_count
FROM links;
"

# 4. Verify MongoDB
echo "✅ Verifying MongoDB workflow_metadata..."
docker exec -it mongodb mongosh -u admin -p admin123 --authenticationDatabase admin messages_db --eval "
db.workflow_metadata.aggregate([
  {
    \$group: {
      _id: {
        has_link: '\$has_link',
        has_content: '\$has_content',
        status: '\$status',
        executed: '\$executed'
      },
      count: { \$sum: 1 }
    }
  }
])
"

echo "✅ Setup complete!"