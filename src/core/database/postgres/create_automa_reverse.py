"""
PostgreSQL reversal operations for Create Automa DAG.
FIXED to match actual database schema and DAG operations.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any
from .connection import get_postgres_connection

logger = logging.getLogger(__name__)


def get_reversal_preview(
    account_id: Optional[int] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Preview what will be reversed for Create Automa operations.
    Shows content that has workflow connections that will be removed.
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                # Build WHERE conditions - using mongo_workflow_id as indicator
                conditions = []
                params = []
                
                if account_id:
                    conditions.append("account_id = %s")
                    params.append(account_id)
                
                if date_from:
                    conditions.append("workflow_processed_time >= %s")
                    params.append(date_from)
                
                if date_to:
                    conditions.append("workflow_processed_time <= %s")
                    params.append(date_to)
                
                where_clause = " AND ".join(conditions) if conditions else "TRUE"
                
                # Count replies with workflows (using mongo_workflow_id IS NOT NULL)
                cursor.execute(
                    f"""SELECT COUNT(*) FROM replies 
                        WHERE mongo_workflow_id IS NOT NULL AND {where_clause}""",
                    params
                )
                replies_count = cursor.fetchone()[0]
                
                # Count messages with workflows
                cursor.execute(
                    f"""SELECT COUNT(*) FROM messages 
                        WHERE mongo_workflow_id IS NOT NULL AND {where_clause}""",
                    params
                )
                messages_count = cursor.fetchone()[0]
                
                # Count retweets with workflows
                cursor.execute(
                    f"""SELECT COUNT(*) FROM retweets 
                        WHERE mongo_workflow_id IS NOT NULL AND {where_clause}""",
                    params
                )
                retweets_count = cursor.fetchone()[0]
                
                total_count = replies_count + messages_count + retweets_count
                
                # Count log entries that will be deleted
                log_conditions = []
                log_params = []
                
                if account_id:
                    log_conditions.append("account_id = %s")
                    log_params.append(account_id)
                
                if date_from:
                    log_conditions.append("generated_time >= %s")
                    log_params.append(date_from)
                
                if date_to:
                    log_conditions.append("generated_time <= %s")
                    log_params.append(date_to)
                
                log_where = " AND ".join(log_conditions) if log_conditions else "TRUE"
                
                # Check if workflow_generation_log table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'workflow_generation_log'
                    )
                """)
                gen_log_exists = cursor.fetchone()[0]
                
                generation_log_count = 0
                if gen_log_exists:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM workflow_generation_log WHERE {log_where}",
                        log_params
                    )
                    generation_log_count = cursor.fetchone()[0]
                
                # Check if workflow_sync_log table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'workflow_sync_log'
                    )
                """)
                sync_log_exists = cursor.fetchone()[0]
                
                sync_log_count = 0
                if sync_log_exists:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM workflow_sync_log WHERE {log_where}",
                        log_params
                    )
                    sync_log_count = cursor.fetchone()[0]
                
                # Get sample records
                sample_records = []
                
                # Sample replies
                cursor.execute(
                    f"""SELECT replies_id, content, mongo_workflow_id, workflow_id, account_id
                        FROM replies 
                        WHERE mongo_workflow_id IS NOT NULL AND {where_clause}
                        LIMIT 3""",
                    params
                )
                for row in cursor.fetchall():
                    sample_records.append({
                        'type': 'reply',
                        'id': row[0],
                        'content_preview': row[1][:50] + '...' if row[1] else '',
                        'mongo_workflow_id': str(row[2]) if row[2] else None,
                        'workflow_id': row[3],
                        'account_id': row[4]
                    })
                
                return {
                    'success': True,
                    'total_to_reverse': total_count,
                    'breakdown_by_type': {
                        'replies': replies_count,
                        'messages': messages_count,
                        'retweets': retweets_count
                    },
                    'log_cleanup': {
                        'generation_logs': generation_log_count,
                        'sync_logs': sync_log_count,
                        'gen_log_exists': gen_log_exists,
                        'sync_log_exists': sync_log_exists
                    },
                    'sample_records': sample_records
                }
                
    except Exception as e:
        logger.error(f"Error getting Create Automa preview: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': str(e),
            'total_to_reverse': 0
        }


def reverse_create_automa_postgres(
    account_id: Optional[int] = None,
    dag_run_id: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    COMPLETE reversal of Create Automa operations in PostgreSQL.
    Based on actual database schema and what the DAG actually does:
    
    What the DAG sets:
    - mongo_workflow_id = ObjectId
    - workflow_id = UUID (might be None)
    - workflow_status = 'connected'
    - processed_by_workflow = TRUE (might not exist)
    - workflow_processed_time = timestamp
    - used = TRUE
    - used_time = timestamp
    
    What we reset:
    - All of the above back to NULL/FALSE/pending
    - Also clean up log tables if they exist
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                # Build WHERE conditions - using mongo_workflow_id IS NOT NULL
                conditions = ["mongo_workflow_id IS NOT NULL"]
                params = []
                
                if account_id:
                    conditions.append("account_id = %s")
                    params.append(account_id)
                
                if date_from:
                    conditions.append("workflow_processed_time >= %s")
                    params.append(date_from)
                
                if date_to:
                    conditions.append("workflow_processed_time <= %s")
                    params.append(date_to)
                
                where_clause = " AND ".join(conditions)
                
                # Get mongo_workflow_ids before resetting (for log cleanup)
                cursor.execute(
                    f"SELECT DISTINCT mongo_workflow_id FROM replies WHERE {where_clause}",
                    params
                )
                reply_workflow_ids = [row[0] for row in cursor.fetchall() if row[0]]
                
                cursor.execute(
                    f"SELECT DISTINCT mongo_workflow_id FROM messages WHERE {where_clause}",
                    params
                )
                message_workflow_ids = [row[0] for row in cursor.fetchall() if row[0]]
                
                cursor.execute(
                    f"SELECT DISTINCT mongo_workflow_id FROM retweets WHERE {where_clause}",
                    params
                )
                retweet_workflow_ids = [row[0] for row in cursor.fetchall() if row[0]]
                
                all_workflow_ids = list(set(reply_workflow_ids + message_workflow_ids + retweet_workflow_ids))
                
                # Check which columns exist in replies table
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'replies'
                """)
                reply_columns = [row[0] for row in cursor.fetchall()]
                
                # Build UPDATE statement based on existing columns
                update_fields = [
                    "used = FALSE",
                    "used_time = NULL",
                    "workflow_status = 'pending'",
                    "mongo_workflow_id = NULL"
                ]
                
                # Add optional fields if they exist
                if 'workflow_processed_time' in reply_columns:
                    update_fields.append("workflow_processed_time = NULL")
                if 'processed_by_workflow' in reply_columns:
                    update_fields.append("processed_by_workflow = FALSE")
                if 'workflow_id' in reply_columns:
                    update_fields.append("workflow_id = NULL")
                if 'workflow_name' in reply_columns:
                    update_fields.append("workflow_name = NULL")
                if 'workflow_linked' in reply_columns:
                    update_fields.append("workflow_linked = FALSE")
                
                update_clause = ", ".join(update_fields)
                
                # Reset replies
                cursor.execute(
                    f"""UPDATE replies SET {update_clause}
                        WHERE {where_clause}""",
                    params
                )
                replies_reset = cursor.rowcount
                
                # Reset messages
                cursor.execute(
                    f"""UPDATE messages SET {update_clause}
                        WHERE {where_clause}""",
                    params
                )
                messages_reset = cursor.rowcount
                
                # Reset retweets
                cursor.execute(
                    f"""UPDATE retweets SET {update_clause}
                        WHERE {where_clause}""",
                    params
                )
                retweets_reset = cursor.rowcount
                
                # ============================================
                # CLEANUP LOG TABLES (if they exist)
                # ============================================
                
                # Build conditions for log cleanup
                log_conditions = []
                log_params = []
                
                if account_id:
                    log_conditions.append("account_id = %s")
                    log_params.append(account_id)
                
                if date_from:
                    log_conditions.append("generated_time >= %s")
                    log_params.append(date_from)
                
                if date_to:
                    log_conditions.append("generated_time <= %s")
                    log_params.append(date_to)
                
                log_where = " AND ".join(log_conditions) if log_conditions else "TRUE"
                
                # Check if workflow_generation_log exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'workflow_generation_log'
                    )
                """)
                gen_log_exists = cursor.fetchone()[0]
                
                generation_log_deleted = 0
                if gen_log_exists and log_where:
                    cursor.execute(
                        f"DELETE FROM workflow_generation_log WHERE {log_where}",
                        log_params
                    )
                    generation_log_deleted = cursor.rowcount
                
                # Check if workflow_sync_log exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'workflow_sync_log'
                    )
                """)
                sync_log_exists = cursor.fetchone()[0]
                
                sync_log_deleted = 0
                if sync_log_exists and all_workflow_ids:
                    # Convert ObjectIds to strings for the query
                    workflow_id_strings = [str(wid) for wid in all_workflow_ids]
                    cursor.execute(
                        """DELETE FROM workflow_sync_log 
                           WHERE mongo_workflow_id = ANY(%s)""",
                        (workflow_id_strings,)
                    )
                    sync_log_deleted = cursor.rowcount
                
                conn.commit()
                
                total_reset = replies_reset + messages_reset + retweets_reset
                
                logger.info(f"COMPLETE reversal of Create Automa in PostgreSQL:")
                logger.info(f"  Content reset:")
                logger.info(f"    - Reset {replies_reset} replies")
                logger.info(f"    - Reset {messages_reset} messages")
                logger.info(f"    - Reset {retweets_reset} retweets")
                logger.info(f"  Log cleanup:")
                logger.info(f"    - Deleted {generation_log_deleted} generation log entries")
                logger.info(f"    - Deleted {sync_log_deleted} sync log entries")
                logger.info(f"  Total: {total_reset} content records + {generation_log_deleted + sync_log_deleted} log entries")
                
                return {
                    'success': True,
                    'total_reversed': total_reset,
                    'details': {
                        'replies_reset': replies_reset,
                        'messages_reset': messages_reset,
                        'retweets_reset': retweets_reset,
                        'generation_log_deleted': generation_log_deleted,
                        'sync_log_deleted': sync_log_deleted,
                        'workflow_ids_cleaned': len(all_workflow_ids),
                        'columns_updated': update_fields
                    },
                    'message': f'Successfully reset {total_reset} workflow connections and deleted {generation_log_deleted + sync_log_deleted} log entries'
                }
                
    except Exception as e:
        logger.error(f"Error reversing Create Automa in PostgreSQL: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': str(e),
            'total_reversed': 0
        }