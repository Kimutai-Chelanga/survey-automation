import logging
from typing import List, Optional, Dict, Any, Tuple
from psycopg2.extras import RealDictCursor

from .workflow_stats import get_comprehensive_data_with_workflow_filter
from .connection import get_postgres_connection

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

logger = logging.getLogger(__name__)

def get_replies_stats_cached() -> Dict[str, Any]:
    """Get statistics about replies from the database, cached for performance."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                try:
                    cursor.execute("SELECT COUNT(*) AS count FROM replies")
                    stats['total_replies'] = cursor.fetchone()['count']

                    cursor.execute("SELECT COUNT(*) AS count FROM replies WHERE used = TRUE")
                    stats['used_replies'] = cursor.fetchone()['count']

                    stats['unused_replies'] = stats['total_replies'] - stats['used_replies']

                except Exception as e:
                    logger.error(f"Error fetching reply stats: {e}")
                    if STREAMLIT_AVAILABLE:
                        st.error(f"❌ Error fetching reply stats: {str(e)}")
                    return {
                        'total_replies': 0,
                        'used_replies': 0,
                        'unused_replies': 0
                    }
                return stats
    except Exception as e:
        logger.error(f"Database connection error in get_replies_stats_cached: {e}")
        return {
            'total_replies': 0,
            'used_replies': 0,
            'unused_replies': 0
        }

def get_detailed_replies_stats() -> Dict[str, Any]:
    """Fetches detailed statistics about replies from PostgreSQL."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                cursor.execute("SELECT COUNT(*) AS count FROM replies")
                stats['total_replies'] = cursor.fetchone()['count']

                cursor.execute("SELECT COUNT(*) AS count FROM replies WHERE used = TRUE")
                stats['used_replies'] = cursor.fetchone()['count']

                stats['unused_replies'] = stats['total_replies'] - stats['used_replies']

                # workflow_linked_replies is the same as used_replies
                stats['workflow_linked_replies'] = stats['used_replies']

                logger.info("Successfully retrieved detailed reply statistics from PostgreSQL.")
                return stats
    except Exception as e:
        logger.error(f"Error fetching detailed reply stats: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching reply stats: {str(e)}")
        return {
            'total_replies': 0,
            'used_replies': 0,
            'unused_replies': 0,
            'workflow_linked_replies': 0
        }

# Alias function for compatibility with UI components
def get_detailed_stats() -> Dict[str, Any]:
    """Alias for get_detailed_replies_stats() for UI compatibility."""
    return get_detailed_replies_stats()

def get_unused_comprehensive_replies_with_workflow_filter(account_id: int = None, 
                                                          workflow_linkage: str = "All",
                                                          workflow_id: str = None,
                                                          limit: int = 100) -> list:
    """Get unused replies with workflow filtering"""
    return get_comprehensive_data_with_workflow_filter(
        'replies', account_id, workflow_linkage, workflow_id, used_only=False, limit=limit
    )



def get_unused_replies(limit: int = None, offset: int = 0) -> List[Tuple[int, str]]:
    """Fetches unused replies from the PostgreSQL replies table with pagination."""
    replies = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT replies_id, content FROM replies WHERE used = FALSE ORDER BY created_time ASC"
                params = []
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                replies = cursor.fetchall()
                logger.info(f"Retrieved {len(replies)} unused replies from PostgreSQL.")
                return replies
    except Exception as e:
        logger.error(f"Error fetching unused replies from PostgreSQL: {e}")
        return []





def get_comprehensive_data(limit: int = None, offset: int = 0, used: Optional[bool] = None, account_id: int = None) -> List[Dict[str, Any]]:
    """Alias for get_comprehensive_replies() for UI compatibility with optional account filtering."""
    if account_id:
        return get_replies_by_account_id(account_id, limit=limit, offset=offset)
    else:
        return get_comprehensive_replies(limit=limit, offset=offset, used=used)

def mark_reply_as_used(reply_id: int):
    """Marks a reply as used in the PostgreSQL replies table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''UPDATE replies 
                       SET used = TRUE, 
                           used_time = CURRENT_TIMESTAMP
                       WHERE replies_id = %s''',
                    (reply_id,)
                )
                conn.commit()
                logger.info(f"✅ Reply {reply_id} marked as used in PostgreSQL.")
    except Exception as e:
        logger.error(f"❌ Error marking reply {reply_id} as used: {e}")
        raise

def update_reply_mongo_id(reply_id: int, mongo_object_id: str):
    """Updates the mongo_object_id for a given reply_id in the PostgreSQL replies table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''UPDATE replies SET mongo_object_id = %s WHERE replies_id = %s''',
                    (mongo_object_id, reply_id)
                )
                conn.commit()
                logger.info(f"✅ Reply {reply_id} updated with mongo_object_id: {mongo_object_id}")
    except Exception as e:
        logger.error(f"❌ Error updating mongo_object_id for reply {reply_id}: {e}")
        raise



def update_reply_workflow_connection(reply_id: int, mongo_workflow_id: str, workflow_name: str):
    """Updates the workflow connection details for a reply in PostgreSQL."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """UPDATE replies 
                       SET mongo_workflow_id = %s, 
                           workflow_name = %s
                       WHERE replies_id = %s""",
                    (mongo_workflow_id, workflow_name, reply_id)
                )
                conn.commit()
                logger.info(f"Updated reply {reply_id} with mongo_workflow_id: {mongo_workflow_id}")
    except Exception as e:
        logger.error(f"Error updating workflow connection for reply {reply_id}: {e}")
        # Remove Streamlit dependency
        raise

# ============================================================================
# UPDATED METHODS FOR replies.py - Replace these in your replies.py file
# ============================================================================

def save_reply_to_db(content: str, account_id: Optional[int] = None) -> int:
    """Inserts a single reply into the PostgreSQL replies table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''INSERT INTO replies (content, used, created_time, account_id, mongo_object_id, workflow_status)
                        VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s) RETURNING replies_id''',
                    (content, False, account_id, None, 'pending')
                )
                reply_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"✅ Reply saved to PostgreSQL with replies_id: {reply_id}")
                return reply_id
    except Exception as e:
        logger.error(f"❌ Error saving reply to PostgreSQL: {e}")
        raise

def save_replies_to_db(replies: List[str], account_id: Optional[int] = None) -> int:
    """Inserts multiple replies into the PostgreSQL replies table."""
    inserted_count = 0
    failed_count = 0
    
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                for i, reply in enumerate(replies):
                    cleaned_reply = reply.strip()
                    if not cleaned_reply:
                        logger.warning(f"⚠️ Skipping empty reply at index {i}")
                        failed_count += 1
                        continue
                        
                    if len(cleaned_reply) < 10:
                        logger.warning(f"⚠️ Skipping too short reply at index {i}: '{cleaned_reply}'")
                        failed_count += 1
                        continue
                        
                    skip_patterns = ['here are', 'below are', 'list of', 'examples of', 'note:', 'please remember']
                    if any(pattern in cleaned_reply.lower() for pattern in skip_patterns):
                        logger.warning(f"⚠️ Skipping non-content at index {i}: '{cleaned_reply[:50]}...'")
                        failed_count += 1
                        continue
                        
                    try:
                        logger.info(f"🔍 Attempting to insert reply {i+1}: '{cleaned_reply[:100]}...'")
                        cursor.execute(
                            '''INSERT INTO replies (content, used, created_time, account_id, mongo_object_id, workflow_status)
                                VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s) RETURNING replies_id''',
                            (cleaned_reply, False, account_id, None, 'pending')
                        )
                        
                        result = cursor.fetchone()
                        if result is not None:
                            reply_id = result[0] if isinstance(result, (list, tuple)) else result['replies_id']
                            logger.info(f"✅ Reply saved to PostgreSQL with replies_id: {reply_id}")
                            inserted_count += 1
                        else:
                            logger.error(f"❌ INSERT returned None result for reply {i+1}")
                            failed_count += 1
                            
                    except Exception as insert_error:
                        logger.error(f"❌ Failed to insert reply {i+1} '{cleaned_reply[:50]}...': {str(insert_error)}")
                        failed_count += 1
                        continue
                        
                conn.commit()
                logger.info(f"📊 Insertion summary: {inserted_count} successful, {failed_count} failed")
                
                if inserted_count > 0:
                    logger.info(f"✅ Successfully saved {inserted_count} replies to PostgreSQL")
                if failed_count > 0:
                    logger.warning(f"⚠️ {failed_count} replies failed to save")
                    
                if inserted_count == 0 and failed_count > 0:
                    raise Exception(f"All {failed_count} replies failed to save to database")
                    
                return inserted_count
                
    except Exception as e:
        logger.error(f"❌ Error saving replies to PostgreSQL: {str(e)}")
        raise

def get_comprehensive_replies(limit: int = None, offset: int = 0, used: Optional[bool] = None) -> List[Dict[str, Any]]:
    """Fetches all replies with associated workflow information from PostgreSQL with pagination."""
    replies = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        r.replies_id,
                        r.content,
                        r.used,
                        r.used_time,
                        r.created_time,
                        r.mongo_object_id,
                        r.account_id,
                        r.workflow_id,
                        r.workflow_status,
                        r.processed_by_workflow,
                        r.workflow_processed_time
                    FROM replies r
                    WHERE 1=1
                """
                params = []
                if used is not None:
                    query += " AND r.used = %s"
                    params.append(used)
                query += " ORDER BY r.created_time DESC"
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                replies = cursor.fetchall()
                logger.info(f"Retrieved {len(replies)} comprehensive reply records from PostgreSQL.")
                return replies
    except Exception as e:
        logger.error(f"Error fetching comprehensive replies from PostgreSQL: {e}")
        return []

def get_reply_by_id(reply_id: int) -> Optional[Dict[str, Any]]:
    """Fetches a single reply by ID from the PostgreSQL replies table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT replies_id, content, used, used_time, created_time, 
                              account_id, mongo_object_id, workflow_id, workflow_status,
                              processed_by_workflow, workflow_processed_time
                       FROM replies WHERE replies_id = %s""",
                    (reply_id,)
                )
                reply = cursor.fetchone()
                if reply:
                    logger.info(f"Retrieved reply {reply_id} from PostgreSQL.")
                    return dict(reply)
                else:
                    logger.warning(f"Reply {reply_id} not found in PostgreSQL.")
                    return None
    except Exception as e:
        logger.error(f"Error fetching reply {reply_id} from PostgreSQL: {e}")
        return None

def get_replies_with_workflows(limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches replies that have associated workflows from PostgreSQL."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT r.replies_id, r.content, r.used, r.used_time, r.created_time,
                           r.account_id, r.mongo_object_id, r.workflow_id, r.workflow_status,
                           r.processed_by_workflow, r.workflow_processed_time
                    FROM replies r
                    WHERE r.mongo_object_id IS NOT NULL 
                      AND r.workflow_id IS NOT NULL
                    ORDER BY r.created_time DESC
                """
                params = []
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                replies = cursor.fetchall()
                logger.info(f"Retrieved {len(replies)} replies with workflows from PostgreSQL.")
                return [dict(reply) for reply in replies]
    except Exception as e:
        logger.error(f"Error fetching replies with workflows from PostgreSQL: {e}")
        return []

def get_replies_by_account_id(account_id: int, limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches replies for a specific account from PostgreSQL."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT replies_id, content, used, used_time, created_time,
                           account_id, mongo_object_id, workflow_id, workflow_status,
                           processed_by_workflow, workflow_processed_time
                    FROM replies
                    WHERE account_id = %s
                    ORDER BY created_time DESC
                """
                params = [account_id]
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                replies = cursor.fetchall()
                logger.info(f"Retrieved {len(replies)} replies for account {account_id} from PostgreSQL.")
                return [dict(reply) for reply in replies]
    except Exception as e:
        logger.error(f"Error fetching replies for account {account_id} from PostgreSQL: {e}")
        return []

# Add these new functions to your replies.py file

def get_unused_comprehensive_replies(limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches ONLY unused replies with associated workflow information from PostgreSQL."""
    replies = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        r.replies_id,
                        r.content,
                        r.used,
                        r.used_time,
                        r.created_time,
                        r.mongo_object_id,
                        r.account_id,
                        r.workflow_id,
                        r.workflow_status,
                        r.processed_by_workflow,
                        r.workflow_processed_time
                    FROM replies r
                    WHERE r.used = FALSE
                    ORDER BY r.created_time DESC
                """
                params = []
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                replies = cursor.fetchall()
                logger.info(f"Retrieved {len(replies)} UNUSED comprehensive reply records from PostgreSQL.")
                return [dict(reply) for reply in replies]
    except Exception as e:
        logger.error(f"Error fetching unused comprehensive replies from PostgreSQL: {e}")
        return []


def get_unused_comprehensive_replies_by_account(account_id: int, limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches ONLY unused replies for a specific account with workflow information."""
    replies = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        r.replies_id,
                        r.content,
                        r.used,
                        r.used_time,
                        r.created_time,
                        r.mongo_object_id,
                        r.account_id,
                        r.workflow_id,
                        r.workflow_status,
                        r.processed_by_workflow,
                        r.workflow_processed_time
                    FROM replies r
                    WHERE r.used = FALSE AND r.account_id = %s
                    ORDER BY r.created_time DESC
                """
                params = [account_id]
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                replies = cursor.fetchall()
                logger.info(f"Retrieved {len(replies)} UNUSED replies for account {account_id} from PostgreSQL.")
                return [dict(reply) for reply in replies]
    except Exception as e:
        logger.error(f"Error fetching unused replies for account {account_id} from PostgreSQL: {e}")
        return []


def get_all_comprehensive_replies(limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches ALL replies (used and unused) with workflow information."""
    replies = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        r.replies_id,
                        r.content,
                        r.used,
                        r.used_time,
                        r.created_time,
                        r.mongo_object_id,
                        r.account_id,
                        r.workflow_id,
                        r.workflow_status,
                        r.processed_by_workflow,
                        r.workflow_processed_time
                    FROM replies r
                    ORDER BY r.created_time DESC
                """
                params = []
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                replies = cursor.fetchall()
                logger.info(f"Retrieved {len(replies)} total comprehensive reply records from PostgreSQL.")
                return [dict(reply) for reply in replies]
    except Exception as e:
        logger.error(f"Error fetching all comprehensive replies from PostgreSQL: {e}")
        return []

def save_replies_bulk_with_relations(replies: List[str], account_id: int, prompt_id: Optional[int] = None,
                                   workflow_id: Optional[int] = None) -> int:
    """Bulk insert replies with account and prompt relationships."""
    inserted_count = 0
    failed_count = 0
    
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                for i, reply in enumerate(replies):
                    cleaned_reply = reply.strip()
                    if not cleaned_reply or len(cleaned_reply) < 10:
                        failed_count += 1
                        continue
                        
                    # Skip non-content patterns
                    skip_patterns = ['here are', 'below are', 'list of', 'examples of', 'note:', 'please remember']
                    if any(pattern in cleaned_reply.lower() for pattern in skip_patterns):
                        failed_count += 1
                        continue
                        
                    try:
                        cursor.execute(
                            '''INSERT INTO replies (content, account_id, prompt_id, workflow_id, used, 
                                                  created_time, mongo_object_id, workflow_status)
                                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s) RETURNING replies_id''',
                            (cleaned_reply, account_id, prompt_id, workflow_id, False, None, 'pending')
                        )
                        
                        result = cursor.fetchone()
                        if result:
                            inserted_count += 1
                            logger.info(f"✅ Reply {i+1} saved with account_id: {account_id}")
                        else:
                            failed_count += 1
                            
                    except Exception as insert_error:
                        logger.error(f"❌ Failed to insert reply {i+1}: {str(insert_error)}")
                        failed_count += 1
                        continue
                        
                conn.commit()
                logger.info(f"📊 Replies bulk insertion: {inserted_count} successful, {failed_count} failed for account {account_id}")
                return inserted_count
                
    except Exception as e:
        logger.error(f"❌ Error in bulk replies insertion for account {account_id}: {str(e)}")
        raise

def get_comprehensive_dashboard_data(account_id: Optional[int] = None) -> Dict[str, Any]:
    """Get comprehensive data for dashboard visualization."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                dashboard_data = {}
                
                # Account filter condition
                account_filter = "WHERE a.account_id = %s" if account_id else ""
                account_params = [account_id] if account_id else []
                
                # Overall statistics
                cursor.execute(f"""
                    SELECT 
                        COUNT(DISTINCT a.account_id) as total_accounts,
                        COUNT(DISTINCT p.prompt_id) as total_prompts,
                        COUNT(DISTINCT w.workflow_id) as total_workflows,
                        COUNT(r.replies_id) as total_replies,
                        COUNT(CASE WHEN r.used = TRUE THEN 1 END) as used_replies
                    FROM accounts a
                    LEFT JOIN prompts p ON a.account_id = p.account_id
                    LEFT JOIN workflows w ON a.account_id = w.account_id  
                    LEFT JOIN replies r ON a.account_id = r.account_id
                    {account_filter}
                """, account_params)
                dashboard_data['overview'] = dict(cursor.fetchone())
                
                # Top performing prompts
                cursor.execute(f"""
                    SELECT 
                        p.name,
                        p.prompt_type,
                        a.username,
                        COUNT(r.replies_id) as reply_count,
                        COUNT(CASE WHEN r.used = TRUE THEN 1 END) as used_count,
                        ROUND(COUNT(CASE WHEN r.used = TRUE THEN 1 END)::numeric / 
                              NULLIF(COUNT(r.replies_id), 0) * 100, 2) as usage_rate
                    FROM prompts p
                    JOIN accounts a ON p.account_id = a.account_id
                    LEFT JOIN replies r ON p.prompt_id = r.prompt_id
                    {account_filter.replace('a.account_id', 'p.account_id') if account_filter else ''}
                    GROUP BY p.prompt_id, p.name, p.prompt_type, a.username
                    HAVING COUNT(r.replies_id) > 0
                    ORDER BY usage_rate DESC, reply_count DESC
                    LIMIT 10
                """, account_params)
                dashboard_data['top_prompts'] = [dict(row) for row in cursor.fetchall()]
                
                # Activity timeline (last 30 days)
                cursor.execute(f"""
                    SELECT 
                        DATE(r.created_time) as date,
                        COUNT(r.replies_id) as replies_created,
                        COUNT(CASE WHEN r.used = TRUE THEN 1 END) as replies_used
                    FROM replies r
                    JOIN accounts a ON r.account_id = a.account_id
                    WHERE r.created_time >= NOW() - INTERVAL '30 days'
                    {('AND ' + account_filter.replace('WHERE ', '')) if account_filter else ''}
                    GROUP BY DATE(r.created_time)
                    ORDER BY date
                """, account_params)
                dashboard_data['activity_timeline'] = [dict(row) for row in cursor.fetchall()]
                
                # Workflow status distribution
                cursor.execute(f"""
                    SELECT 
                        r.workflow_status,
                        COUNT(*) as count
                    FROM replies r
                    JOIN accounts a ON r.account_id = a.account_id
                    {account_filter}
                    GROUP BY r.workflow_status
                """, account_params)
                dashboard_data['workflow_status_dist'] = {row['workflow_status']: row['count'] for row in cursor.fetchall()}
                
                return dashboard_data
                
    except Exception as e:
        logger.error(f"Error fetching comprehensive dashboard data: {e}")
        return {}
def delete_all() -> int:
    """Deletes all replies from the PostgreSQL replies table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM replies")
                deleted_count = cursor.rowcount
                conn.commit()
                logger.info(f"Deleted {deleted_count} replies from PostgreSQL.")
                return deleted_count
    except Exception as e:
        logger.error(f"❌ Error deleting replies from PostgreSQL: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

# Add these methods to your replies.py file

def save_reply_with_account_prompt(content: str, account_id: int, prompt_id: Optional[int] = None, 
                                 workflow_id: Optional[int] = None) -> int:
    """Inserts a reply with proper account and prompt relationships."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''INSERT INTO replies (content, account_id, prompt_id, workflow_id, used, 
                                          created_time, mongo_object_id, workflow_status)
                        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s) RETURNING replies_id''',
                    (content, account_id, prompt_id, workflow_id, False, None, 'pending')
                )
                reply_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"✅ Reply saved to PostgreSQL with replies_id: {reply_id}, account_id: {account_id}")
                return reply_id
    except Exception as e:
        logger.error(f"❌ Error saving reply with account/prompt relationship: {e}")
        raise

def save_replies_bulk_with_relations(replies: List[str], account_id: int, prompt_id: Optional[int] = None,
                                   workflow_id: Optional[int] = None) -> int:
    """Bulk insert replies with account and prompt relationships."""
    inserted_count = 0
    failed_count = 0
    
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                for i, reply in enumerate(replies):
                    cleaned_reply = reply.strip()
                    if not cleaned_reply or len(cleaned_reply) < 10:
                        failed_count += 1
                        continue
                        
                    try:
                        cursor.execute(
                            '''INSERT INTO replies (content, account_id, prompt_id, workflow_id, used, 
                                                  created_time, mongo_object_id, workflow_status)
                                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s) RETURNING replies_id''',
                            (cleaned_reply, account_id, prompt_id, workflow_id, False, None, 'pending')
                        )
                        
                        result = cursor.fetchone()
                        if result:
                            inserted_count += 1
                            logger.info(f"✅ Reply {i+1} saved with account_id: {account_id}")
                        else:
                            failed_count += 1
                            
                    except Exception as insert_error:
                        logger.error(f"❌ Failed to insert reply {i+1}: {str(insert_error)}")
                        failed_count += 1
                        continue
                        
                conn.commit()
                logger.info(f"📊 Bulk insertion: {inserted_count} successful, {failed_count} failed")
                return inserted_count
                
    except Exception as e:
        logger.error(f"❌ Error in bulk replies insertion: {str(e)}")
        raise

def get_replies_by_account(account_id: int, limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches replies for a specific account with full relationship data."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        r.replies_id,
                        r.content,
                        r.used,
                        r.used_time,
                        r.created_time,
                        r.workflow_status,
                        r.processed_by_workflow,
                        r.workflow_processed_time,
                        a.username,
                        a.profile_id,
                        p.name as prompt_name,
                        p.prompt_type,
                        w.name as workflow_name,
                        w.workflow_type
                    FROM replies r
                    JOIN accounts a ON r.account_id = a.account_id
                    LEFT JOIN prompts p ON r.prompt_id = p.prompt_id
                    LEFT JOIN workflows w ON r.workflow_id = w.workflow_id
                    WHERE r.account_id = %s
                    ORDER BY r.created_time DESC
                """
                params = [account_id]
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                replies = cursor.fetchall()
                logger.info(f"Retrieved {len(replies)} replies for account {account_id}")
                return [dict(reply) for reply in replies]
    except Exception as e:
        logger.error(f"Error fetching replies for account {account_id}: {e}")
        return []

def get_replies_by_prompt(prompt_id: int, limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches all replies generated from a specific prompt."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        r.replies_id,
                        r.content,
                        r.used,
                        r.used_time,
                        r.created_time,
                        r.workflow_status,
                        a.username,
                        a.profile_id,
                        p.name as prompt_name,
                        p.content as prompt_content
                    FROM replies r
                    JOIN accounts a ON r.account_id = a.account_id
                    JOIN prompts p ON r.prompt_id = p.prompt_id
                    WHERE r.prompt_id = %s
                    ORDER BY r.created_time DESC
                """
                params = [prompt_id]
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                replies = cursor.fetchall()
                logger.info(f"Retrieved {len(replies)} replies for prompt {prompt_id}")
                return [dict(reply) for reply in replies]
    except Exception as e:
        logger.error(f"Error fetching replies for prompt {prompt_id}: {e}")
        return []

def get_account_reply_statistics(account_id: int) -> Dict[str, Any]:
    """Get comprehensive reply statistics for a specific account."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                
                # Basic counts
                cursor.execute("SELECT COUNT(*) AS count FROM replies WHERE account_id = %s", (account_id,))
                stats['total_replies'] = cursor.fetchone()['count']
                
                cursor.execute("SELECT COUNT(*) AS count FROM replies WHERE account_id = %s AND used = TRUE", (account_id,))
                stats['used_replies'] = cursor.fetchone()['count']
                
                stats['unused_replies'] = stats['total_replies'] - stats['used_replies']
                
                # Workflow status breakdown
                cursor.execute("""
                    SELECT workflow_status, COUNT(*) as count
                    FROM replies 
                    WHERE account_id = %s 
                    GROUP BY workflow_status
                """, (account_id,))
                stats['workflow_status_breakdown'] = {row['workflow_status']: row['count'] for row in cursor.fetchall()}
                
                # Prompt breakdown
                cursor.execute("""
                    SELECT p.name, p.prompt_type, COUNT(r.replies_id) as reply_count
                    FROM replies r
                    JOIN prompts p ON r.prompt_id = p.prompt_id
                    WHERE r.account_id = %s
                    GROUP BY p.prompt_id, p.name, p.prompt_type
                    ORDER BY reply_count DESC
                """, (account_id,))
                stats['prompt_breakdown'] = [dict(row) for row in cursor.fetchall()]
                
                # Recent activity (last 7 days)
                cursor.execute("""
                    SELECT DATE(created_time) as date, COUNT(*) as count
                    FROM replies 
                    WHERE account_id = %s 
                      AND created_time >= NOW() - INTERVAL '7 days'
                    GROUP BY DATE(created_time)
                    ORDER BY date
                """, (account_id,))
                stats['recent_activity'] = [dict(row) for row in cursor.fetchall()]
                
                return stats
    except Exception as e:
        logger.error(f"Error fetching account reply statistics for {account_id}: {e}")
        return {}

def get_prompt_performance_metrics(prompt_id: int) -> Dict[str, Any]:
    """Get performance metrics for a specific prompt."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                metrics = {}
                
                # Basic metrics
                cursor.execute("SELECT COUNT(*) AS total FROM replies WHERE prompt_id = %s", (prompt_id,))
                metrics['total_generated'] = cursor.fetchone()['total']
                
                cursor.execute("SELECT COUNT(*) AS used FROM replies WHERE prompt_id = %s AND used = TRUE", (prompt_id,))
                metrics['total_used'] = cursor.fetchone()['used']
                
                metrics['usage_rate'] = (metrics['total_used'] / metrics['total_generated'] * 100) if metrics['total_generated'] > 0 else 0
                
                # Average time to use
                cursor.execute("""
                    SELECT AVG(EXTRACT(EPOCH FROM (used_time - created_time))/3600) as avg_hours_to_use
                    FROM replies 
                    WHERE prompt_id = %s AND used = TRUE AND used_time IS NOT NULL
                """, (prompt_id,))
                result = cursor.fetchone()
                metrics['avg_hours_to_use'] = float(result['avg_hours_to_use']) if result['avg_hours_to_use'] else 0
                
                # Content length statistics
                cursor.execute("""
                    SELECT 
                        AVG(LENGTH(content)) as avg_length,
                        MIN(LENGTH(content)) as min_length,
                        MAX(LENGTH(content)) as max_length
                    FROM replies 
                    WHERE prompt_id = %s
                """, (prompt_id,))
                length_stats = cursor.fetchone()
                metrics.update(dict(length_stats))
                
                return metrics
    except Exception as e:
        logger.error(f"Error fetching prompt performance metrics for {prompt_id}: {e}")
        return {}

def get_workflow_reply_efficiency(workflow_id: int) -> Dict[str, Any]:
    """Get efficiency metrics for workflow reply processing."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                metrics = {}
                
                # Processing status counts
                cursor.execute("""
                    SELECT workflow_status, COUNT(*) as count
                    FROM replies 
                    WHERE workflow_id = %s 
                    GROUP BY workflow_status
                """, (workflow_id,))
                metrics['status_breakdown'] = {row['workflow_status']: row['count'] for row in cursor.fetchall()}
                
                # Processing time metrics
                cursor.execute("""
                    SELECT 
                        AVG(EXTRACT(EPOCH FROM (workflow_processed_time - created_time))/60) as avg_processing_minutes,
                        COUNT(*) as processed_count
                    FROM replies 
                    WHERE workflow_id = %s 
                      AND workflow_processed_time IS NOT NULL 
                      AND processed_by_workflow = TRUE
                """, (workflow_id,))
                result = cursor.fetchone()
                metrics['avg_processing_minutes'] = float(result['avg_processing_minutes']) if result['avg_processing_minutes'] else 0
                metrics['processed_count'] = result['processed_count']
                
                # Success rate
                cursor.execute("SELECT COUNT(*) as total FROM replies WHERE workflow_id = %s", (workflow_id,))
                total = cursor.fetchone()['total']
                metrics['success_rate'] = (metrics['processed_count'] / total * 100) if total > 0 else 0
                
                return metrics
    except Exception as e:
        logger.error(f"Error fetching workflow efficiency metrics for {workflow_id}: {e}")
        return {}

