#!/bin/bash

echo "Adding generated_workflow_id + workflow_linked to replies, messages, retweets..."

# 1. Add columns + indexes to replies
echo "replies table..."
docker exec -it postgres_db psql -U airflow -d messages -c "
ALTER TABLE replies 
    ADD COLUMN IF NOT EXISTS generated_workflow_id UUID DEFAULT gen_random_uuid() UNIQUE,
    ADD COLUMN IF NOT EXISTS workflow_linked BOOLEAN DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_replies_workflow_linked ON replies(workflow_linked, used);
CREATE INDEX IF NOT EXISTS idx_replies_generated_workflow ON replies(generated_workflow_id);
"

# 2. Same for messages
echo "messages table..."
docker exec -it postgres_db psql -U airflow -d messages -c "
ALTER TABLE messages 
    ADD COLUMN IF NOT EXISTS generated_workflow_id UUID DEFAULT gen_random_uuid() UNIQUE,
    ADD COLUMN IF NOT EXISTS workflow_linked BOOLEAN DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_messages_workflow_linked ON messages(workflow_linked, used);
CREATE INDEX IF NOT EXISTS idx_messages_generated_workflow ON messages(generated_workflow_id);
"

# 3. Same for retweets
echo "retweets table..."
docker exec -it postgres_db psql -U airflow -d messages -c "
ALTER TABLE retweets 
    ADD COLUMN IF NOT EXISTS generated_workflow_id UUID DEFAULT gen_random_uuid() UNIQUE,
    ADD COLUMN IF NOT EXISTS workflow_linked BOOLEAN DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_retweets_workflow_linked ON retweets(workflow_linked, used);
CREATE INDEX IF NOT EXISTS idx_retweets_generated_workflow ON retweets(generated_workflow_id);
"

# 4. Mark existing mongo-linked rows
echo "Marking already-linked content..."
docker exec -it postgres_db psql -U airflow -d messages -c "
UPDATE replies   SET workflow_linked = TRUE WHERE mongo_workflow_id IS NOT NULL AND workflow_linked = FALSE;
UPDATE messages  SET workflow_linked = TRUE WHERE mongo_workflow_id IS NOT NULL AND workflow_linked = FALSE;
UPDATE retweets  SET workflow_linked = TRUE WHERE mongo_workflow_id IS NOT NULL AND workflow_linked = FALSE;
"

# 5. Create the two views you wanted
echo "Creating views..."
docker exec -it postgres_db psql -U airflow -d messages -c "
CREATE OR REPLACE VIEW v_content_ready_for_workflow AS
SELECT 'replies'   as content_type, replies_id   as content_id, content, account_id, prompt_id, generated_workflow_id, workflow_id as workflow_template_id, created_time, used, workflow_linked FROM replies   WHERE used = FALSE AND workflow_linked = FALSE
UNION ALL
SELECT 'messages'  as content_type, messages_id  as content_id, content, account_id, prompt_id, generated_workflow_id, workflow_id as workflow_template_id, created_time, used, workflow_linked FROM messages  WHERE used = FALSE AND workflow_linked = FALSE
UNION ALL
SELECT 'retweets'  as content_type, retweets_id  as content_id, content, account_id, prompt_id, generated_workflow_id, workflow_id as workflow_template_id, created_time, used, workflow_linked FROM retweets  WHERE used = FALSE AND workflow_linked = FALSE;

CREATE OR REPLACE VIEW v_workflow_linked_content AS
SELECT 'replies'  as content_type, replies_id  as content_id, content, account_id, prompt_id, generated_workflow_id, workflow_id as workflow_template_id, mongo_workflow_id, workflow_status, created_time, used, workflow_linked, workflow_processed_time FROM replies  WHERE workflow_linked = TRUE
UNION ALL
SELECT 'messages' as content_type, messages_id as content_id, content, account_id, prompt_id, generated_workflow_id, workflow_id as workflow_template_id, mongo_workflow_id, workflow_status, created_time, used, workflow_linked, workflow_processed_time FROM messages WHERE workflow_linked = TRUE
UNION ALL
SELECT 'retweets' as content_type, retweets_id as content_id, content, account_id, prompt_id, generated_workflow_id, workflow_id as workflow_template_id, mongo_workflow_id, workflow_status, created_time, used, workflow_linked, workflow_processed_time FROM retweets WHERE workflow_linked = TRUE;
"

# 6. Add the stats function
echo "Adding stats function..."
docker exec -it postgres_db psql -U airflow -d messages -c "
CREATE OR REPLACE FUNCTION get_content_workflow_statistics(p_account_id INTEGER DEFAULT NULL)
RETURNS TABLE(
    content_type TEXT,
    total_count BIGINT,
    used_count BIGINT,
    unused_count BIGINT,
    workflow_linked_count BIGINT,
    workflow_unlinked_count BIGINT,
    usage_rate NUMERIC,
    workflow_linkage_rate NUMERIC
) AS \$\$
BEGIN
    RETURN QUERY
    WITH stats AS (
        SELECT 'replies'   as ct, COUNT(*) as total, SUM(CASE WHEN used THEN 1 ELSE 0 END) as used, SUM(CASE WHEN NOT used THEN 1 ELSE 0 END) as unused, SUM(CASE WHEN workflow_linked THEN 1 ELSE 0 END) as linked, SUM(CASE WHEN NOT workflow_linked THEN 1 ELSE 0 END) as unlinked FROM replies   WHERE (p_account_id IS NULL OR account_id = p_account_id)
        UNION ALL
        SELECT 'messages'  as ct, COUNT(*) as total, SUM(CASE WHEN used THEN 1 ELSE 0 END) as used, SUM(CASE WHEN NOT used THEN 1 ELSE 0 END) as unused, SUM(CASE WHEN workflow_linked THEN 1 ELSE 0 END) as linked, SUM(CASE WHEN NOT workflow_linked THEN 1 ELSE 0 END) as unlinked FROM messages  WHERE (p_account_id IS NULL OR account_id = p_account_id)
        UNION ALL
        SELECT 'retweets'  as ct, COUNT(*) as total, SUM(CASE WHEN used THEN 1 ELSE 0 END) as used, SUM(CASE WHEN NOT used THEN 1 ELSE 0 END) as unused, SUM(CASE WHEN workflow_linked THEN 1 ELSE 0 END) as linked, SUM(CASE WHEN NOT workflow_linked THEN 1 ELSE 0 END) as unlinked FROM retweets  WHERE (p_account_id IS NULL OR account_id = p_account_id)
    )
    SELECT ct, total, used, unused, linked, unlinked,
           CASE WHEN total > 0 THEN ROUND((used::NUMERIC / total * 100), 2) ELSE 0 END,
           CASE WHEN total > 0 THEN ROUND((linked::NUMERIC / total * 100), 2) ELSE 0 END
    FROM stats;
END;
\$\$ LANGUAGE plpgsql;
"

# 7. Final verification – exactly like you do
echo "Verification – everything added correctly?"
docker exec -it postgres_db psql -U airflow -d messages -c "
SELECT 
    'replies'  as table, COUNT(*) as rows, COUNT(generated_workflow_id) as has_uuid, SUM(CASE WHEN workflow_linked THEN 1 ELSE 0 END) as linked FROM replies
UNION ALL
SELECT 'messages' as table, COUNT(*) as rows, COUNT(generated_workflow_id) as has_uuid, SUM(CASE WHEN workflow_linked THEN 1 ELSE 0 END) as linked FROM messages
UNION ALL
SELECT 'retweets' as table, COUNT(*) as rows, COUNT(generated_workflow_id) as has_uuid, SUM(CASE WHEN workflow_linked THEN 1 ELSE 0 END) as linked FROM retweets;
"

echo "All done. Your schema is now 100% ready for the new workflow system!"