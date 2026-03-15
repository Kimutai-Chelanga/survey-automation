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

def get_retweets_stats_cached() -> Dict[str, Any]:
    """Get statistics about retweets from the database, cached for performance."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                try:
                    cursor.execute("SELECT COUNT(*) AS count FROM retweets")
                    stats['total_retweets'] = cursor.fetchone()['count']

                    cursor.execute("SELECT COUNT(*) AS count FROM retweets WHERE used = TRUE")
                    stats['used_retweets'] = cursor.fetchone()['count']

                    stats['unused_retweets'] = stats['total_retweets'] - stats['used_retweets']

                except Exception as e:
                    logger.error(f"Error fetching retweet stats: {e}")
                    if STREAMLIT_AVAILABLE:
                        st.error(f"❌ Error fetching retweet stats: {str(e)}")
                    return {
                        'total_retweets': 0,
                        'used_retweets': 0,
                        'unused_retweets': 0
                    }
                return stats
    except Exception as e:
        logger.error(f"Database connection error in get_retweets_stats_cached: {e}")
        return {
            'total_retweets': 0,
            'used_retweets': 0,
            'unused_retweets': 0
        }
    
def get_unused_comprehensive_messages_with_workflow_filter(account_id: int = None,
                                                           workflow_linkage: str = "All",
                                                           workflow_id: str = None,
                                                           limit: int = 100) -> list:
    """Get unused messages with workflow filtering"""
    return get_comprehensive_data_with_workflow_filter(
        'messages', account_id, workflow_linkage, workflow_id, used_only=False, limit=limit
    )


def get_unused_comprehensive_retweets_with_workflow_filter(account_id: int = None,
                                                           workflow_linkage: str = "All",
                                                           workflow_id: str = None,
                                                           limit: int = 100) -> list:
    """Get unused retweets with workflow filtering"""
    return get_comprehensive_data_with_workflow_filter(
        'retweets', account_id, workflow_linkage, workflow_id, used_only=False, limit=limit
    )
def get_comprehensive_retweets(limit: int = None, offset: int = 0, used: Optional[bool] = None) -> List[Dict[str, Any]]:
    """Fetches all retweets with associated workflow information from PostgreSQL with pagination."""
    retweets = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        r.retweets_id,
                        r.content,
                        r.used,
                        r.used_time,
                        r.created_time,
                        r.mongo_object_id,
                        r.account_id,  -- Fixed: using account_id instead of user_id
                        r.workflow_id,
                        r.workflow_status,
                        r.processed_by_workflow,
                        r.workflow_processed_time
                    FROM retweets r
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
                retweets = cursor.fetchall()
                logger.info(f"Retrieved {len(retweets)} comprehensive retweet records from PostgreSQL.")
                return retweets
    except Exception as e:
        logger.error(f"Error fetching comprehensive retweets from PostgreSQL: {e}")
        return []

def save_retweet_to_db(content: str, account_id: Optional[int] = None) -> int:
    """Inserts a single retweet into the PostgreSQL retweets table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''INSERT INTO retweets (content, used, created_time, account_id, mongo_object_id, workflow_status)
                        VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s) RETURNING retweets_id''',
                    (content, False, account_id, None, 'pending')
                )
                retweet_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"✅ Retweet saved to PostgreSQL with retweets_id: {retweet_id}")
                return retweet_id
    except Exception as e:
        logger.error(f"❌ Error saving retweet to PostgreSQL: {e}")
        raise

def save_retweets_to_db(retweets: List[str], account_id: Optional[int] = None) -> int:
    """Inserts multiple retweets into the PostgreSQL retweets table."""
    inserted_count = 0
    failed_count = 0
    
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                for i, retweet in enumerate(retweets):
                    cleaned_retweet = retweet.strip()
                    if not cleaned_retweet:
                        logger.warning(f"⚠️ Skipping empty retweet at index {i}")
                        failed_count += 1
                        continue
                        
                    if len(cleaned_retweet) < 10:
                        logger.warning(f"⚠️ Skipping too short retweet at index {i}: '{cleaned_retweet}'")
                        failed_count += 1
                        continue
                        
                    skip_patterns = ['here are', 'below are', 'list of', 'examples of', 'note:', 'please remember']
                    if any(pattern in cleaned_retweet.lower() for pattern in skip_patterns):
                        logger.warning(f"⚠️ Skipping non-content at index {i}: '{cleaned_retweet[:50]}...'")
                        failed_count += 1
                        continue
                        
                    try:
                        logger.info(f"🔍 Attempting to insert retweet {i+1}: '{cleaned_retweet[:100]}...'")
                        cursor.execute(
                            '''INSERT INTO retweets (content, used, created_time, account_id, mongo_object_id, workflow_status)
                                VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s) RETURNING retweets_id''',
                            (cleaned_retweet, False, account_id, None, 'pending')
                        )
                        
                        result = cursor.fetchone()
                        if result is not None:
                            retweet_id = result[0] if isinstance(result, (list, tuple)) else result['retweets_id']
                            logger.info(f"✅ Retweet saved to PostgreSQL with retweets_id: {retweet_id}")
                            inserted_count += 1
                        else:
                            logger.error(f"❌ INSERT returned None result for retweet {i+1}")
                            failed_count += 1
                            
                    except Exception as insert_error:
                        logger.error(f"❌ Failed to insert retweet {i+1} '{cleaned_retweet[:50]}...': {str(insert_error)}")
                        failed_count += 1
                        continue
                        
                conn.commit()
                logger.info(f"📊 Insertion summary: {inserted_count} successful, {failed_count} failed")
                
                if inserted_count > 0:
                    logger.info(f"✅ Successfully saved {inserted_count} retweets to PostgreSQL")
                if failed_count > 0:
                    logger.warning(f"⚠️ {failed_count} retweets failed to save")
                    
                if inserted_count == 0 and failed_count > 0:
                    raise Exception(f"All {failed_count} retweets failed to save to database")
                    
                return inserted_count
                
    except Exception as e:
        logger.error(f"❌ Error saving retweets to PostgreSQL: {str(e)}")
        raise

# In core/database/postgres/retweets.py
# Find the get_retweet_by_id() function and update the SQL query

def get_retweet_by_id(retweet_id):
    """Get a retweet by its ID from PostgreSQL"""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                # FIXED: Changed user_id to account_id to match the actual table schema
                cursor.execute(
                    """
                    SELECT 
                        retweets_id,
                        account_id,  -- Changed from user_id
                        mongo_object_id,
                        workflow_id,
                        prompt_id,
                        content,
                        used,
                        created_time,
                        used_time
                    FROM retweets
                    WHERE retweets_id = %s
                    """,
                    (retweet_id,)
                )
                
                result = cursor.fetchone()
                if result:
                    columns = [desc[0] for desc in cursor.description]
                    retweet_dict = dict(zip(columns, result))
                    logger.info(f"Retrieved retweet {retweet_id} from PostgreSQL.")
                    return retweet_dict
                else:
                    logger.warning(f"Retweet {retweet_id} not found in PostgreSQL.")
                    return None
                    
    except Exception as e:
        logger.error(f"Error fetching retweet {retweet_id} from PostgreSQL: {e}")
        return None
def get_retweets_with_workflows(limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches retweets that have associated workflows from PostgreSQL."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT r.retweets_id, r.content, r.used, r.used_time, r.created_time,
                           r.account_id, r.mongo_object_id, r.workflow_id, r.workflow_status,
                           r.processed_by_workflow, r.workflow_processed_time
                    FROM retweets r
                    WHERE r.mongo_object_id IS NOT NULL 
                      AND r.workflow_id IS NOT NULL
                    ORDER BY r.created_time DESC
                """
                params = []
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                retweets = cursor.fetchall()
                logger.info(f"Retrieved {len(retweets)} retweets with workflows from PostgreSQL.")
                return [dict(retweet) for retweet in retweets]
    except Exception as e:
        logger.error(f"Error fetching retweets with workflows from PostgreSQL: {e}")
        return []

def get_retweets_by_account_id(account_id: int, limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches retweets for a specific account from PostgreSQL."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT retweets_id, content, used, used_time, created_time,
                           account_id, mongo_object_id, workflow_id, workflow_status,
                           processed_by_workflow, workflow_processed_time
                    FROM retweets
                    WHERE account_id = %s
                    ORDER BY created_time DESC
                """
                params = [account_id]
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                retweets = cursor.fetchall()
                logger.info(f"Retrieved {len(retweets)} retweets for account {account_id} from PostgreSQL.")
                return [dict(retweet) for retweet in retweets]
    except Exception as e:
        logger.error(f"Error fetching retweets for account {account_id} from PostgreSQL: {e}")
        return []



def get_detailed_retweets_stats() -> Dict[str, Any]:
    """Fetches detailed statistics about retweets from PostgreSQL."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                cursor.execute("SELECT COUNT(*) AS count FROM retweets")
                stats['total_retweets'] = cursor.fetchone()['count']

                cursor.execute("SELECT COUNT(*) AS count FROM retweets WHERE used = TRUE")
                stats['used_retweets'] = cursor.fetchone()['count']

                stats['unused_retweets'] = stats['total_retweets'] - stats['used_retweets']

                # workflow_linked_retweets is the same as used_retweets
                stats['workflow_linked_retweets'] = stats['used_retweets']

                logger.info("Successfully retrieved detailed retweet statistics from PostgreSQL.")
                return stats
    except Exception as e:
        logger.error(f"Error fetching detailed retweet stats: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching retweet stats: {str(e)}")
        return {
            'total_retweets': 0,
            'used_retweets': 0,
            'unused_retweets': 0,
            'workflow_linked_retweets': 0
        }
# Alias function for compatibility with UI components
def get_detailed_stats() -> Dict[str, Any]:
    """Alias for get_detailed_retweets_stats() for UI compatibility."""
    return get_detailed_retweets_stats()

def get_unused_comprehensive_retweets(limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches ONLY unused retweets with associated workflow information from PostgreSQL."""
    retweets = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        rt.retweets_id,
                        rt.content,
                        rt.used,
                        rt.used_time,
                        rt.created_time,
                        rt.mongo_object_id,
                        rt.account_id,
                        rt.workflow_id,
                        rt.workflow_status,
                        rt.processed_by_workflow,
                        rt.workflow_processed_time
                    FROM retweets rt
                    WHERE rt.used = FALSE
                    ORDER BY rt.created_time DESC
                """
                params = []
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                retweets = cursor.fetchall()
                logger.info(f"Retrieved {len(retweets)} UNUSED comprehensive retweet records from PostgreSQL.")
                return [dict(rt) for rt in retweets]
    except Exception as e:
        logger.error(f"Error fetching unused comprehensive retweets from PostgreSQL: {e}")
        return []


def get_unused_comprehensive_retweets_by_account(account_id: int, limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches ONLY unused retweets for a specific account with workflow information."""
    retweets = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        rt.retweets_id,
                        rt.content,
                        rt.used,
                        rt.used_time,
                        rt.created_time,
                        rt.mongo_object_id,
                        rt.account_id,
                        rt.workflow_id,
                        rt.workflow_status,
                        rt.processed_by_workflow,
                        rt.workflow_processed_time
                    FROM retweets rt
                    WHERE rt.used = FALSE AND rt.account_id = %s
                    ORDER BY rt.created_time DESC
                """
                params = [account_id]
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                retweets = cursor.fetchall()
                logger.info(f"Retrieved {len(retweets)} UNUSED retweets for account {account_id} from PostgreSQL.")
                return [dict(rt) for rt in retweets]
    except Exception as e:
        logger.error(f"Error fetching unused retweets for account {account_id} from PostgreSQL: {e}")
        return []


def get_all_comprehensive_retweets(limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches ALL retweets (used and unused) with workflow information."""
    retweets = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        rt.retweets_id,
                        rt.content,
                        rt.used,
                        rt.used_time,
                        rt.created_time,
                        rt.mongo_object_id,
                        rt.account_id,
                        rt.workflow_id,
                        rt.workflow_status,
                        rt.processed_by_workflow,
                        rt.workflow_processed_time
                    FROM retweets rt
                    ORDER BY rt.created_time DESC
                """
                params = []
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                retweets = cursor.fetchall()
                logger.info(f"Retrieved {len(retweets)} total comprehensive retweet records from PostgreSQL.")
                return [dict(rt) for rt in retweets]
    except Exception as e:
        logger.error(f"Error fetching all comprehensive retweets from PostgreSQL: {e}")
        return []

def get_unused_retweets(limit: int = None, offset: int = 0) -> List[Tuple[int, str]]:
    """Fetches unused retweets from the PostgreSQL retweets table with pagination."""
    retweets = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT retweets_id, content FROM retweets WHERE used = FALSE ORDER BY created_time ASC"
                params = []
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                retweets = cursor.fetchall()
                logger.info(f"Retrieved {len(retweets)} unused retweets from PostgreSQL.")
                return retweets
    except Exception as e:
        logger.error(f"Error fetching unused retweets from PostgreSQL: {e}")
        return []





def get_comprehensive_data(limit: int = None, offset: int = 0, used: Optional[bool] = None, account_id: int = None) -> List[Dict[str, Any]]:
    """Alias for get_comprehensive_retweets() for UI compatibility with optional account filtering."""
    if account_id:
        return get_retweets_by_account_id(account_id, limit=limit, offset=offset)
    else:
        return get_comprehensive_retweets(limit=limit, offset=offset, used=used)

def mark_retweet_as_used(retweet_id: int):
    """Marks a retweet as used in the PostgreSQL retweets table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''UPDATE retweets 
                       SET used = TRUE, 
                           used_time = CURRENT_TIMESTAMP
                       WHERE retweets_id = %s''',
                    (retweet_id,)
                )
                conn.commit()
                logger.info(f"✅ Retweet {retweet_id} marked as used in PostgreSQL.")
    except Exception as e:
        logger.error(f"❌ Error marking retweet {retweet_id} as used: {e}")
        raise

def update_retweet_mongo_id(retweet_id: int, mongo_object_id: str):
    """Updates the mongo_object_id for a given retweet_id in the PostgreSQL retweets table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''UPDATE retweets SET mongo_object_id = %s WHERE retweets_id = %s''',
                    (mongo_object_id, retweet_id)
                )
                conn.commit()
                logger.info(f"✅ Retweet {retweet_id} updated with mongo_object_id: {mongo_object_id}")
    except Exception as e:
        logger.error(f"❌ Error updating mongo_object_id for retweet {retweet_id}: {e}")
        raise



def update_retweet_workflow_connection(retweet_id: int, mongo_workflow_id: str, workflow_name: str):
    """Updates the workflow connection details for a retweet in PostgreSQL."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """UPDATE retweets 
                       SET mongo_workflow_id = %s, 
                           workflow_name = %s
                       WHERE retweets_id = %s""",
                    (mongo_workflow_id, workflow_name, retweet_id)
                )
                conn.commit()
                logger.info(f"Updated retweet {retweet_id} with mongo_workflow_id: {mongo_workflow_id}")
    except Exception as e:
        logger.error(f"Error updating workflow connection for retweet {retweet_id}: {e}")
        # Remove Streamlit dependency
        raise

    
def delete_all() -> int:
    """Deletes all retweets from the PostgreSQL retweets table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM retweets")
                deleted_count = cursor.rowcount
                conn.commit()
                logger.info(f"Deleted {deleted_count} retweets from PostgreSQL.")
                return deleted_count
    except Exception as e:
        logger.error(f"❌ Error deleting retweets from PostgreSQL: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

# Add these methods to your retweets.py file

def save_retweet_with_account_prompt(content: str, account_id: int, prompt_id: Optional[int] = None, 
                                   workflow_id: Optional[int] = None) -> int:
    """Inserts a retweet with proper account and prompt relationships."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''INSERT INTO retweets (content, account_id, prompt_id, workflow_id, used, 
                                          created_time, mongo_object_id, workflow_status)
                        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s) RETURNING retweets_id''',
                    (content, account_id, prompt_id, workflow_id, False, None, 'pending')
                )
                retweet_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"Retweet saved to PostgreSQL with retweets_id: {retweet_id}, account_id: {account_id}")
                return retweet_id
    except Exception as e:
        logger.error(f"Error saving retweet with account/prompt relationship: {e}")
        raise

def save_retweets_bulk_with_relations(retweets: List[str], account_id: int, prompt_id: Optional[int] = None,
                                    workflow_id: Optional[int] = None) -> int:
    """Bulk insert retweets with account and prompt relationships."""
    inserted_count = 0
    failed_count = 0
    
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                for i, retweet in enumerate(retweets):
                    cleaned_retweet = retweet.strip()
                    if not cleaned_retweet or len(cleaned_retweet) < 10:
                        failed_count += 1
                        continue
                        
                    try:
                        cursor.execute(
                            '''INSERT INTO retweets (content, account_id, prompt_id, workflow_id, used, 
                                                  created_time, mongo_object_id, workflow_status)
                                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s) RETURNING retweets_id''',
                            (cleaned_retweet, account_id, prompt_id, workflow_id, False, None, 'pending')
                        )
                        
                        result = cursor.fetchone()
                        if result:
                            inserted_count += 1
                            logger.info(f"Retweet {i+1} saved with account_id: {account_id}")
                        else:
                            failed_count += 1
                            
                    except Exception as insert_error:
                        logger.error(f"Failed to insert retweet {i+1}: {str(insert_error)}")
                        failed_count += 1
                        continue
                        
                conn.commit()
                logger.info(f"Bulk insertion: {inserted_count} successful, {failed_count} failed")
                return inserted_count
                
    except Exception as e:
        logger.error(f"Error in bulk retweets insertion: {str(e)}")
        raise

def get_retweets_by_account(account_id: int, limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches retweets for a specific account with full relationship data."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        r.retweets_id,
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
                    FROM retweets r
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
                retweets = cursor.fetchall()
                logger.info(f"Retrieved {len(retweets)} retweets for account {account_id}")
                return [dict(retweet) for retweet in retweets]
    except Exception as e:
        logger.error(f"Error fetching retweets for account {account_id}: {e}")
        return []

def get_retweets_by_prompt(prompt_id: int, limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches all retweets generated from a specific prompt."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        r.retweets_id,
                        r.content,
                        r.used,
                        r.used_time,
                        r.created_time,
                        r.workflow_status,
                        a.username,
                        a.profile_id,
                        p.name as prompt_name,
                        p.content as prompt_content
                    FROM retweets r
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
                retweets = cursor.fetchall()
                logger.info(f"Retrieved {len(retweets)} retweets for prompt {prompt_id}")
                return [dict(retweet) for retweet in retweets]
    except Exception as e:
        logger.error(f"Error fetching retweets for prompt {prompt_id}: {e}")
        return []

def get_account_retweet_statistics(account_id: int) -> Dict[str, Any]:
    """Get comprehensive retweet statistics for a specific account."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                
                # Basic counts
                cursor.execute("SELECT COUNT(*) AS count FROM retweets WHERE account_id = %s", (account_id,))
                stats['total_retweets'] = cursor.fetchone()['count']
                
                cursor.execute("SELECT COUNT(*) AS count FROM retweets WHERE account_id = %s AND used = TRUE", (account_id,))
                stats['used_retweets'] = cursor.fetchone()['count']
                
                stats['unused_retweets'] = stats['total_retweets'] - stats['used_retweets']
                
                # Workflow status breakdown
                cursor.execute("""
                    SELECT workflow_status, COUNT(*) as count
                    FROM retweets 
                    WHERE account_id = %s 
                    GROUP BY workflow_status
                """, (account_id,))
                stats['workflow_status_breakdown'] = {row['workflow_status']: row['count'] for row in cursor.fetchall()}
                
                # Prompt breakdown
                cursor.execute("""
                    SELECT p.name, p.prompt_type, COUNT(r.retweets_id) as retweet_count
                    FROM retweets r
                    JOIN prompts p ON r.prompt_id = p.prompt_id
                    WHERE r.account_id = %s
                    GROUP BY p.prompt_id, p.name, p.prompt_type
                    ORDER BY retweet_count DESC
                """, (account_id,))
                stats['prompt_breakdown'] = [dict(row) for row in cursor.fetchall()]
                
                # Recent activity (last 7 days)
                cursor.execute("""
                    SELECT DATE(created_time) as date, COUNT(*) as count
                    FROM retweets 
                    WHERE account_id = %s 
                      AND created_time >= NOW() - INTERVAL '7 days'
                    GROUP BY DATE(created_time)
                    ORDER BY date
                """, (account_id,))
                stats['recent_activity'] = [dict(row) for row in cursor.fetchall()]
                
                return stats
    except Exception as e:
        logger.error(f"Error fetching account retweet statistics for {account_id}: {e}")
        return {}

def get_retweet_virality_metrics(account_id: int, days: int = 30) -> Dict[str, Any]:
    """Get retweet performance and virality indicators."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                metrics = {}
                
                # Time-to-use analysis (faster usage might indicate higher quality)
                cursor.execute("""
                    SELECT 
                        AVG(EXTRACT(EPOCH FROM (used_time - created_time))/3600) as avg_hours_to_use,
                        MIN(EXTRACT(EPOCH FROM (used_time - created_time))/3600) as min_hours_to_use,
                        MAX(EXTRACT(EPOCH FROM (used_time - created_time))/3600) as max_hours_to_use,
                        COUNT(*) as sample_size
                    FROM retweets 
                    WHERE account_id = %s 
                      AND used = TRUE 
                      AND used_time IS NOT NULL
                      AND created_time >= NOW() - INTERVAL '%s days'
                """, (account_id, days))
                time_metrics = cursor.fetchone()
                metrics['time_to_use'] = dict(time_metrics) if time_metrics else {}
                
                # Content length vs usage correlation
                cursor.execute("""
                    SELECT 
                        CASE 
                            WHEN LENGTH(content) <= 50 THEN 'Short (≤50)'
                            WHEN LENGTH(content) <= 100 THEN 'Medium (51-100)'
                            WHEN LENGTH(content) <= 200 THEN 'Long (101-200)'
                            ELSE 'Very Long (>200)'
                        END as length_category,
                        COUNT(*) as total,
                        COUNT(CASE WHEN used = TRUE THEN 1 END) as used,
                        ROUND(COUNT(CASE WHEN used = TRUE THEN 1 END)::numeric / 
                              NULLIF(COUNT(*), 0) * 100, 2) as usage_rate
                    FROM retweets 
                    WHERE account_id = %s
                      AND created_time >= NOW() - INTERVAL '%s days'
                    GROUP BY length_category
                    ORDER BY usage_rate DESC
                """, (account_id, days))
                metrics['length_performance'] = [dict(row) for row in cursor.fetchall()]
                
                return metrics
    except Exception as e:
        logger.error(f"Error fetching retweet virality metrics for {account_id}: {e}")
        return {}

def get_retweet_content_analysis(account_id: int) -> Dict[str, Any]:
    """Analyze retweet content patterns and effectiveness."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                analysis = {}
                
                # Most effective prompts for retweets
                cursor.execute("""
                    SELECT 
                        p.name as prompt_name,
                        p.prompt_type,
                        COUNT(r.retweets_id) as total_retweets,
                        COUNT(CASE WHEN r.used = TRUE THEN 1 END) as used_retweets,
                        ROUND(COUNT(CASE WHEN r.used = TRUE THEN 1 END)::numeric / 
                              NULLIF(COUNT(r.retweets_id), 0) * 100, 2) as effectiveness_rate,
                        ROUND(AVG(LENGTH(r.content))) as avg_content_length
                    FROM retweets r
                    JOIN prompts p ON r.prompt_id = p.prompt_id
                    WHERE r.account_id = %s
                    GROUP BY p.prompt_id, p.name, p.prompt_type
                    HAVING COUNT(r.retweets_id) >= 5  -- Only prompts with meaningful data
                    ORDER BY effectiveness_rate DESC, total_retweets DESC
                """, (account_id,))
                analysis['prompt_effectiveness'] = [dict(row) for row in cursor.fetchall()]
                
                # Workflow processing efficiency
                cursor.execute("""
                    SELECT 
                        w.name as workflow_name,
                        COUNT(CASE WHEN r.workflow_status = 'completed' THEN 1 END) as completed,
                        COUNT(CASE WHEN r.workflow_status = 'failed' THEN 1 END) as failed,
                        COUNT(CASE WHEN r.workflow_status = 'pending' THEN 1 END) as pending,
                        ROUND(COUNT(CASE WHEN r.workflow_status = 'completed' THEN 1 END)::numeric / 
                              NULLIF(COUNT(r.retweets_id), 0) * 100, 2) as success_rate
                    FROM retweets r
                    JOIN workflows w ON r.workflow_id = w.workflow_id
                    WHERE r.account_id = %s
                    GROUP BY w.workflow_id, w.name
                    ORDER BY success_rate DESC
                """, (account_id,))
                analysis['workflow_success_rates'] = [dict(row) for row in cursor.fetchall()]
                
                return analysis
    except Exception as e:
        logger.error(f"Error fetching retweet content analysis for {account_id}: {e}")
        return {}


def save_retweets_bulk_with_relations(retweets: List[str], account_id: int, prompt_id: Optional[int] = None,
                                    workflow_id: Optional[int] = None) -> int:
    """Bulk insert retweets with account and prompt relationships."""
    inserted_count = 0
    failed_count = 0
    
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                for i, retweet in enumerate(retweets):
                    cleaned_retweet = retweet.strip()
                    if not cleaned_retweet or len(cleaned_retweet) < 10:
                        failed_count += 1
                        continue
                        
                    # Skip non-content patterns
                    skip_patterns = ['here are', 'below are', 'list of', 'examples of', 'note:', 'please remember']
                    if any(pattern in cleaned_retweet.lower() for pattern in skip_patterns):
                        failed_count += 1
                        continue
                        
                    try:
                        cursor.execute(
                            '''INSERT INTO retweets (content, account_id, prompt_id, workflow_id, used, 
                                                  created_time, mongo_object_id, workflow_status)
                                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s) RETURNING retweets_id''',
                            (cleaned_retweet, account_id, prompt_id, workflow_id, False, None, 'pending')
                        )
                        
                        result = cursor.fetchone()
                        if result:
                            inserted_count += 1
                            logger.info(f"✅ Retweet {i+1} saved with account_id: {account_id}")
                        else:
                            failed_count += 1
                            
                    except Exception as insert_error:
                        logger.error(f"❌ Failed to insert retweet {i+1}: {str(insert_error)}")
                        failed_count += 1
                        continue
                        
                conn.commit()
                logger.info(f"📊 Retweets bulk insertion: {inserted_count} successful, {failed_count} failed for account {account_id}")
                return inserted_count
                
    except Exception as e:
        logger.error(f"❌ Error in bulk retweets insertion for account {account_id}: {str(e)}")
        raise

def get_retweets_by_account(account_id: int, limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches retweets for a specific account with full relationship data."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        r.retweets_id,
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
                    FROM retweets r
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
                retweets = cursor.fetchall()
                logger.info(f"Retrieved {len(retweets)} retweets for account {account_id}")
                return [dict(retweet) for retweet in retweets]
    except Exception as e:
        logger.error(f"Error fetching retweets for account {account_id}: {e}")
        return []

def get_account_retweet_statistics(account_id: int) -> Dict[str, Any]:
    """Get comprehensive retweet statistics for a specific account."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                
                # Basic counts
                cursor.execute("SELECT COUNT(*) AS count FROM retweets WHERE account_id = %s", (account_id,))
                stats['total_retweets'] = cursor.fetchone()['count']
                
                cursor.execute("SELECT COUNT(*) AS count FROM retweets WHERE account_id = %s AND used = TRUE", (account_id,))
                stats['used_retweets'] = cursor.fetchone()['count']
                
                stats['unused_retweets'] = stats['total_retweets'] - stats['used_retweets']
                
                # Workflow status breakdown
                cursor.execute("""
                    SELECT workflow_status, COUNT(*) as count
                    FROM retweets 
                    WHERE account_id = %s 
                    GROUP BY workflow_status
                """, (account_id,))
                stats['workflow_status_breakdown'] = {row['workflow_status']: row['count'] for row in cursor.fetchall()}
                
                # Prompt breakdown
                cursor.execute("""
                    SELECT p.name, p.prompt_type, COUNT(r.retweets_id) as retweet_count
                    FROM retweets r
                    JOIN prompts p ON r.prompt_id = p.prompt_id
                    WHERE r.account_id = %s
                    GROUP BY p.prompt_id, p.name, p.prompt_type
                    ORDER BY retweet_count DESC
                """, (account_id,))
                stats['prompt_breakdown'] = [dict(row) for row in cursor.fetchall()]
                
                return stats
    except Exception as e:
        logger.error(f"Error fetching account retweet statistics for {account_id}: {e}")
        return {}


