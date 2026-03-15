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

def get_messages_stats_cached() -> Dict[str, Any]:
    """Get statistics about messages from the database, cached for performance."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                try:
                    cursor.execute("SELECT COUNT(*) AS count FROM messages")
                    stats['total_messages'] = cursor.fetchone()['count']

                    cursor.execute("SELECT COUNT(*) AS count FROM messages WHERE used = TRUE")
                    stats['used_messages'] = cursor.fetchone()['count']

                    stats['unused_messages'] = stats['total_messages'] - stats['used_messages']

                except Exception as e:
                    logger.error(f"Error fetching message stats: {e}")
                    if STREAMLIT_AVAILABLE:
                        st.error(f"❌ Error fetching message stats: {str(e)}")
                    return {
                        'total_messages': 0,
                        'used_messages': 0,
                        'unused_messages': 0
                    }
                return stats
    except Exception as e:
        logger.error(f"Database connection error in get_messages_stats_cached: {e}")
        return {
            'total_messages': 0,
            'used_messages': 0,
            'unused_messages': 0
        }
def get_unused_comprehensive_messages_with_workflow_filter(account_id: int = None,
                                                           workflow_linkage: str = "All",
                                                           workflow_id: str = None,
                                                           limit: int = 100) -> list:
    """Get unused messages with workflow filtering"""
    return get_comprehensive_data_with_workflow_filter(
        'messages', account_id, workflow_linkage, workflow_id, used_only=False, limit=limit
    )
def get_detailed_messages_stats() -> Dict[str, Any]:
    """Fetches detailed statistics about messages from PostgreSQL."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                cursor.execute("SELECT COUNT(*) AS count FROM messages")
                stats['total_messages'] = cursor.fetchone()['count']

                cursor.execute("SELECT COUNT(*) AS count FROM messages WHERE used = TRUE")
                stats['used_messages'] = cursor.fetchone()['count']

                stats['unused_messages'] = stats['total_messages'] - stats['used_messages']

                # workflow_linked_messages is the same as used_messages
                stats['workflow_linked_messages'] = stats['used_messages']

                logger.info("Successfully retrieved detailed message statistics from PostgreSQL.")
                return stats
    except Exception as e:
        logger.error(f"Error fetching detailed message stats: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching message stats: {str(e)}")
        return {
            'total_messages': 0,
            'used_messages': 0,
            'unused_messages': 0,
            'workflow_linked_messages': 0
        }




# Alias function for compatibility with UI components
def get_detailed_stats() -> Dict[str, Any]:
    """Alias for get_detailed_messages_stats() for UI compatibility."""
    return get_detailed_messages_stats()

def save_message_to_db(content: str, user_id: Optional[int] = None) -> int:
    """Inserts a single message into the PostgreSQL messages table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''INSERT INTO messages (content, used, created_time, user_id, mongo_object_id, workflow_status)
                        VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s) RETURNING messages_id''',
                    (content, False, user_id, None, 'pending')
                )
                message_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"✅ Message saved to PostgreSQL with messages_id: {message_id}")
                return message_id
    except Exception as e:
        logger.error(f"❌ Error saving message to PostgreSQL: {e}")
        raise


def get_unused_messages(limit: int = None, offset: int = 0) -> List[Tuple[int, str]]:
    """Fetches unused messages from the PostgreSQL messages table with pagination."""
    messages = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT messages_id, content FROM messages WHERE used = FALSE ORDER BY created_time ASC"
                params = []
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                messages = cursor.fetchall()
                logger.info(f"Retrieved {len(messages)} unused messages from PostgreSQL.")
                return messages
    except Exception as e:
        logger.error(f"Error fetching unused messages from PostgreSQL: {e}")
        return []





def get_comprehensive_data(limit: int = None, offset: int = 0, used: Optional[bool] = None, account_id: int = None) -> List[Dict[str, Any]]:
    """Alias for get_comprehensive_messages() for UI compatibility with optional account filtering."""
    if account_id:
        return get_messages_by_account_id(account_id, limit=limit, offset=offset)
    else:
        return get_comprehensive_messages(limit=limit, offset=offset, used=used)

def mark_message_as_used(message_id: int):
    """Marks a message as used in the PostgreSQL messages table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''UPDATE messages 
                       SET used = TRUE, 
                           used_time = CURRENT_TIMESTAMP
                       WHERE messages_id = %s''',
                    (message_id,)
                )
                conn.commit()
                logger.info(f"✅ Message {message_id} marked as used in PostgreSQL.")
    except Exception as e:
        logger.error(f"❌ Error marking message {message_id} as used: {e}")
        raise

def update_message_mongo_id(message_id: int, mongo_object_id: str):
    """Updates the mongo_object_id for a given message_id in the PostgreSQL messages table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''UPDATE messages SET mongo_object_id = %s WHERE messages_id = %s''',
                    (mongo_object_id, message_id)
                )
                conn.commit()
                logger.info(f"✅ Message {message_id} updated with mongo_object_id: {mongo_object_id}")
    except Exception as e:
        logger.error(f"❌ Error updating mongo_object_id for message {message_id}: {e}")
        raise



def update_message_workflow_connection(message_id: int, mongo_workflow_id: str, workflow_name: str):
    """Updates the workflow connection details for a message in PostgreSQL."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """UPDATE messages 
                       SET mongo_workflow_id = %s, 
                           workflow_name = %s
                       WHERE messages_id = %s""",
                    (mongo_workflow_id, workflow_name, message_id)
                )
                conn.commit()
                logger.info(f"Updated message {message_id} with mongo_workflow_id: {mongo_workflow_id}")
    except Exception as e:
        logger.error(f"Error updating workflow connection for message {message_id}: {e}")
        # Remove Streamlit dependency
        raise



def delete_all() -> int:
    """Deletes all messages from the PostgreSQL messages table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM messages")
                deleted_count = cursor.rowcount
                conn.commit()
                logger.info(f"Deleted {deleted_count} messages from PostgreSQL.")
                return deleted_count
    except Exception as e:
        logger.error(f"❌ Error deleting messages from PostgreSQL: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

# Add these methods to your messages.py file

def save_message_with_account_prompt(content: str, account_id: int, prompt_id: Optional[int] = None, 
                                   workflow_id: Optional[int] = None) -> int:
    """Inserts a message with proper account and prompt relationships."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''INSERT INTO messages (content, account_id, prompt_id, workflow_id, used, 
                                          created_time, mongo_object_id, workflow_status)
                        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s) RETURNING messages_id''',
                    (content, account_id, prompt_id, workflow_id, False, None, 'pending')
                )
                message_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"Message saved to PostgreSQL with messages_id: {message_id}, account_id: {account_id}")
                return message_id
    except Exception as e:
        logger.error(f"Error saving message with account/prompt relationship: {e}")
        raise





def get_messages_by_prompt(prompt_id: int, limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches all messages generated from a specific prompt."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        m.messages_id,
                        m.content,
                        m.used,
                        m.used_time,
                        m.created_time,
                        m.workflow_status,
                        a.username,
                        a.profile_id,
                        p.name as prompt_name,
                        p.content as prompt_content
                    FROM messages m
                    JOIN accounts a ON m.account_id = a.account_id
                    JOIN prompts p ON m.prompt_id = p.prompt_id
                    WHERE m.prompt_id = %s
                    ORDER BY m.created_time DESC
                """
                params = [prompt_id]
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                messages = cursor.fetchall()
                logger.info(f"Retrieved {len(messages)} messages for prompt {prompt_id}")
                return [dict(message) for message in messages]
    except Exception as e:
        logger.error(f"Error fetching messages for prompt {prompt_id}: {e}")
        return []
# ============================================================================
# ADD THESE FUNCTIONS TO messages.py
# ============================================================================

def get_unused_comprehensive_messages(limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches ONLY unused messages with associated workflow information from PostgreSQL."""
    messages = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        m.messages_id,
                        m.content,
                        m.used,
                        m.used_time,
                        m.created_time,
                        m.mongo_object_id,
                        m.account_id,
                        m.workflow_id,
                        m.workflow_status,
                        m.processed_by_workflow,
                        m.workflow_processed_time
                    FROM messages m
                    WHERE m.used = FALSE
                    ORDER BY m.created_time DESC
                """
                params = []
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                messages = cursor.fetchall()
                logger.info(f"Retrieved {len(messages)} UNUSED comprehensive message records from PostgreSQL.")
                return [dict(msg) for msg in messages]
    except Exception as e:
        logger.error(f"Error fetching unused comprehensive messages from PostgreSQL: {e}")
        return []


def get_unused_comprehensive_messages_by_account(account_id: int, limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches ONLY unused messages for a specific account with workflow information."""
    messages = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        m.messages_id,
                        m.content,
                        m.used,
                        m.used_time,
                        m.created_time,
                        m.mongo_object_id,
                        m.account_id,
                        m.workflow_id,
                        m.workflow_status,
                        m.processed_by_workflow,
                        m.workflow_processed_time
                    FROM messages m
                    WHERE m.used = FALSE AND m.account_id = %s
                    ORDER BY m.created_time DESC
                """
                params = [account_id]
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                messages = cursor.fetchall()
                logger.info(f"Retrieved {len(messages)} UNUSED messages for account {account_id} from PostgreSQL.")
                return [dict(msg) for msg in messages]
    except Exception as e:
        logger.error(f"Error fetching unused messages for account {account_id} from PostgreSQL: {e}")
        return []


def get_all_comprehensive_messages(limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches ALL messages (used and unused) with workflow information."""
    messages = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        m.messages_id,
                        m.content,
                        m.used,
                        m.used_time,
                        m.created_time,
                        m.mongo_object_id,
                        m.account_id,
                        m.workflow_id,
                        m.workflow_status,
                        m.processed_by_workflow,
                        m.workflow_processed_time
                    FROM messages m
                    ORDER BY m.created_time DESC
                """
                params = []
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                messages = cursor.fetchall()
                logger.info(f"Retrieved {len(messages)} total comprehensive message records from PostgreSQL.")
                return [dict(msg) for msg in messages]
    except Exception as e:
        logger.error(f"Error fetching all comprehensive messages from PostgreSQL: {e}")
        return []
def get_account_message_statistics(account_id: int) -> Dict[str, Any]:
    """Get comprehensive message statistics for a specific account."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                
                # Basic counts
                cursor.execute("SELECT COUNT(*) AS count FROM messages WHERE account_id = %s", (account_id,))
                stats['total_messages'] = cursor.fetchone()['count']
                
                cursor.execute("SELECT COUNT(*) AS count FROM messages WHERE account_id = %s AND used = TRUE", (account_id,))
                stats['used_messages'] = cursor.fetchone()['count']
                
                stats['unused_messages'] = stats['total_messages'] - stats['used_messages']
                
                # Workflow status breakdown
                cursor.execute("""
                    SELECT workflow_status, COUNT(*) as count
                    FROM messages 
                    WHERE account_id = %s 
                    GROUP BY workflow_status
                """, (account_id,))
                stats['workflow_status_breakdown'] = {row['workflow_status']: row['count'] for row in cursor.fetchall()}
                
                # Prompt breakdown
                cursor.execute("""
                    SELECT p.name, p.prompt_type, COUNT(m.messages_id) as message_count
                    FROM messages m
                    JOIN prompts p ON m.prompt_id = p.prompt_id
                    WHERE m.account_id = %s
                    GROUP BY p.prompt_id, p.name, p.prompt_type
                    ORDER BY message_count DESC
                """, (account_id,))
                stats['prompt_breakdown'] = [dict(row) for row in cursor.fetchall()]
                
                # Recent activity (last 7 days)
                cursor.execute("""
                    SELECT DATE(created_time) as date, COUNT(*) as count
                    FROM messages 
                    WHERE account_id = %s 
                      AND created_time >= NOW() - INTERVAL '7 days'
                    GROUP BY DATE(created_time)
                    ORDER BY date
                """, (account_id,))
                stats['recent_activity'] = [dict(row) for row in cursor.fetchall()]
                
                return stats
    except Exception as e:
        logger.error(f"Error fetching account message statistics for {account_id}: {e}")
        return {}

def get_message_engagement_metrics(account_id: int, days: int = 30) -> Dict[str, Any]:
    """Get message engagement and performance metrics."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                metrics = {}
                
                # Daily message creation and usage rates
                cursor.execute("""
                    SELECT 
                        DATE(created_time) as date,
                        COUNT(*) as created,
                        COUNT(CASE WHEN used = TRUE THEN 1 END) as used,
                        ROUND(COUNT(CASE WHEN used = TRUE THEN 1 END)::numeric / 
                              NULLIF(COUNT(*), 0) * 100, 2) as daily_usage_rate
                    FROM messages 
                    WHERE account_id = %s 
                      AND created_time >= NOW() - INTERVAL '%s days'
                    GROUP BY DATE(created_time)
                    ORDER BY date
                """, (account_id, days))
                metrics['daily_metrics'] = [dict(row) for row in cursor.fetchall()]
                
                # Peak usage times
                cursor.execute("""
                    SELECT 
                        EXTRACT(HOUR FROM used_time) as hour,
                        COUNT(*) as usage_count
                    FROM messages 
                    WHERE account_id = %s 
                      AND used = TRUE 
                      AND used_time IS NOT NULL
                    GROUP BY EXTRACT(HOUR FROM used_time)
                    ORDER BY usage_count DESC
                    LIMIT 5
                """, (account_id,))
                metrics['peak_usage_hours'] = [dict(row) for row in cursor.fetchall()]
                
                return metrics
    except Exception as e:
        logger.error(f"Error fetching message engagement metrics for {account_id}: {e}")
        return {}



# Updated database functions for messages.py, replies.py, and retweets.py
# These functions should be added to the respective database files

# ============================================================================
# FOR messages.py - Add these functions
# ============================================================================

# ============================================================================
# UPDATED METHODS FOR messages.py - Replace these in your messages.py file
# ============================================================================

def save_message_to_db(content: str, account_id: Optional[int] = None) -> int:
    """Inserts a single message into the PostgreSQL messages table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''INSERT INTO messages (content, used, created_time, account_id, mongo_object_id, workflow_status)
                        VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s) RETURNING messages_id''',
                    (content, False, account_id, None, 'pending')
                )
                message_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"✅ Message saved to PostgreSQL with messages_id: {message_id}")
                return message_id
    except Exception as e:
        logger.error(f"❌ Error saving message to PostgreSQL: {e}")
        raise

def save_messages_to_db(messages: List[str], account_id: Optional[int] = None) -> int:
    """Inserts multiple messages into the PostgreSQL messages table."""
    inserted_count = 0
    failed_count = 0
    
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                for i, msg in enumerate(messages):
                    cleaned_msg = msg.strip()
                    if not cleaned_msg:
                        logger.warning(f"⚠️ Skipping empty message at index {i}")
                        failed_count += 1
                        continue
                        
                    if len(cleaned_msg) < 10:
                        logger.warning(f"⚠️ Skipping too short message at index {i}: '{cleaned_msg}'")
                        failed_count += 1
                        continue
                        
                    skip_patterns = ['here are', 'below are', 'list of', 'examples of', 'note:', 'please remember']
                    if any(pattern in cleaned_msg.lower() for pattern in skip_patterns):
                        logger.warning(f"⚠️ Skipping non-content at index {i}: '{cleaned_msg[:50]}...'")
                        failed_count += 1
                        continue
                        
                    try:
                        logger.info(f"🔍 Attempting to insert message {i+1}: '{cleaned_msg[:100]}...'")
                        cursor.execute(
                            '''INSERT INTO messages (content, used, created_time, account_id, mongo_object_id, workflow_status)
                                VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s) RETURNING messages_id''',
                            (cleaned_msg, False, account_id, None, 'pending')
                        )
                        
                        result = cursor.fetchone()
                        if result is not None:
                            message_id = result[0] if isinstance(result, (list, tuple)) else result['messages_id']
                            logger.info(f"✅ Message saved to PostgreSQL with messages_id: {message_id}")
                            inserted_count += 1
                        else:
                            logger.error(f"❌ INSERT returned None result for message {i+1}")
                            failed_count += 1
                            
                    except Exception as insert_error:
                        logger.error(f"❌ Failed to insert message {i+1} '{cleaned_msg[:50]}...': {str(insert_error)}")
                        failed_count += 1
                        continue
                        
                conn.commit()
                logger.info(f"📊 Insertion summary: {inserted_count} successful, {failed_count} failed")
                
                if inserted_count > 0:
                    logger.info(f"✅ Successfully saved {inserted_count} messages to PostgreSQL")
                if failed_count > 0:
                    logger.warning(f"⚠️ {failed_count} messages failed to save")
                    
                if inserted_count == 0 and failed_count > 0:
                    raise Exception(f"All {failed_count} messages failed to save to database")
                    
                return inserted_count
                
    except Exception as e:
        logger.error(f"❌ Error saving messages to PostgreSQL: {str(e)}")
        raise

def get_comprehensive_messages(limit: int = None, offset: int = 0, used: Optional[bool] = None) -> List[Dict[str, Any]]:
    """Fetches all messages with associated workflow information from PostgreSQL with pagination."""
    messages = []
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        m.messages_id,
                        m.content,
                        m.used,
                        m.used_time,
                        m.created_time,
                        m.mongo_object_id,
                        m.account_id,
                        m.workflow_id,
                        m.workflow_status,
                        m.processed_by_workflow,
                        m.workflow_processed_time
                    FROM messages m
                    WHERE 1=1
                """
                params = []
                if used is not None:
                    query += " AND m.used = %s"
                    params.append(used)
                query += " ORDER BY m.created_time DESC"
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                messages = cursor.fetchall()
                logger.info(f"Retrieved {len(messages)} comprehensive message records from PostgreSQL.")
                return messages
    except Exception as e:
        logger.error(f"Error fetching comprehensive messages from PostgreSQL: {e}")
        return []

def get_message_by_id(message_id: int) -> Optional[Dict[str, Any]]:
    """Fetches a single message by ID from the PostgreSQL messages table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT messages_id, content, used, used_time, created_time, 
                              account_id, mongo_object_id, workflow_id, workflow_status,
                              processed_by_workflow, workflow_processed_time
                       FROM messages WHERE messages_id = %s""",
                    (message_id,)
                )
                message = cursor.fetchone()
                if message:
                    logger.info(f"Retrieved message {message_id} from PostgreSQL.")
                    return dict(message)
                else:
                    logger.warning(f"Message {message_id} not found in PostgreSQL.")
                    return None
    except Exception as e:
        logger.error(f"Error fetching message {message_id} from PostgreSQL: {e}")
        return None

def get_messages_with_workflows(limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches messages that have associated workflows from PostgreSQL."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT m.messages_id, m.content, m.used, m.used_time, m.created_time,
                           m.account_id, m.mongo_object_id, m.workflow_id, m.workflow_status,
                           m.processed_by_workflow, m.workflow_processed_time
                    FROM messages m
                    WHERE m.mongo_object_id IS NOT NULL 
                      AND m.workflow_id IS NOT NULL
                    ORDER BY m.created_time DESC
                """
                params = []
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                messages = cursor.fetchall()
                logger.info(f"Retrieved {len(messages)} messages with workflows from PostgreSQL.")
                return [dict(msg) for msg in messages]
    except Exception as e:
        logger.error(f"Error fetching messages with workflows from PostgreSQL: {e}")
        return []

def get_messages_by_account_id(account_id: int, limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
    """Fetches messages for a specific account from PostgreSQL."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT messages_id, content, used, used_time, created_time,
                           account_id, mongo_object_id, workflow_id, workflow_status,
                           processed_by_workflow, workflow_processed_time
                    FROM messages
                    WHERE account_id = %s
                    ORDER BY created_time DESC
                """
                params = [account_id]
                if limit is not None:
                    query += " LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                
                cursor.execute(query, params)
                messages = cursor.fetchall()
                logger.info(f"Retrieved {len(messages)} messages for account {account_id} from PostgreSQL.")
                return [dict(msg) for msg in messages]
    except Exception as e:
        logger.error(f"Error fetching messages for account {account_id} from PostgreSQL: {e}")
        return []

def save_messages_bulk_with_relations(messages: List[str], account_id: int, prompt_id: Optional[int] = None,
                                    workflow_id: Optional[int] = None) -> int:
    """Bulk insert messages with account and prompt relationships."""
    inserted_count = 0
    failed_count = 0
    
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                for i, message in enumerate(messages):
                    cleaned_message = message.strip()
                    if not cleaned_message or len(cleaned_message) < 10:
                        failed_count += 1
                        continue
                        
                    # Skip non-content patterns
                    skip_patterns = ['here are', 'below are', 'list of', 'examples of', 'note:', 'please remember']
                    if any(pattern in cleaned_message.lower() for pattern in skip_patterns):
                        failed_count += 1
                        continue
                        
                    try:
                        cursor.execute(
                            '''INSERT INTO messages (content, account_id, prompt_id, workflow_id, used, 
                                                  created_time, mongo_object_id, workflow_status)
                                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s) RETURNING messages_id''',
                            (cleaned_message, account_id, prompt_id, workflow_id, False, None, 'pending')
                        )
                        
                        result = cursor.fetchone()
                        if result:
                            inserted_count += 1
                            logger.info(f"✅ Message {i+1} saved with account_id: {account_id}")
                        else:
                            failed_count += 1
                            
                    except Exception as insert_error:
                        logger.error(f"❌ Failed to insert message {i+1}: {str(insert_error)}")
                        failed_count += 1
                        continue
                        
                conn.commit()
                logger.info(f"📊 Messages bulk insertion: {inserted_count} successful, {failed_count} failed for account {account_id}")
                return inserted_count
                
    except Exception as e:
        logger.error(f"❌ Error in bulk messages insertion for account {account_id}: {str(e)}")
        raise

def get_cross_platform_message_analysis(account_id: int) -> Dict[str, Any]:
    """Analyze message performance across different prompt types and workflows."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                analysis = {}
                
                # Performance by prompt type
                cursor.execute("""
                    SELECT 
                        p.prompt_type,
                        COUNT(m.messages_id) as total_messages,
                        COUNT(CASE WHEN m.used = TRUE THEN 1 END) as used_messages,
                        ROUND(AVG(LENGTH(m.content))) as avg_length,
                        ROUND(COUNT(CASE WHEN m.used = TRUE THEN 1 END)::numeric / 
                              NULLIF(COUNT(m.messages_id), 0) * 100, 2) as usage_rate
                    FROM messages m
                    JOIN prompts p ON m.prompt_id = p.prompt_id
                    WHERE m.account_id = %s
                    GROUP BY p.prompt_type
                    ORDER BY usage_rate DESC
                """, (account_id,))
                analysis['by_prompt_type'] = [dict(row) for row in cursor.fetchall()]
                
                # Workflow efficiency comparison
                cursor.execute("""
                    SELECT 
                        w.name as workflow_name,
                        w.workflow_type,
                        COUNT(m.messages_id) as total_processed,
                        COUNT(CASE WHEN m.workflow_status = 'completed' THEN 1 END) as completed,
                        AVG(EXTRACT(EPOCH FROM (m.workflow_processed_time - m.created_time))/60) as avg_processing_minutes
                    FROM messages m
                    JOIN workflows w ON m.workflow_id = w.workflow_id
                    WHERE m.account_id = %s
                      AND m.workflow_processed_time IS NOT NULL
                    GROUP BY w.workflow_id, w.name, w.workflow_type
                    ORDER BY avg_processing_minutes ASC
                """, (account_id,))
                analysis['workflow_efficiency'] = [dict(row) for row in cursor.fetchall()]
                
                return analysis
    except Exception as e:
        logger.error(f"Error fetching cross-platform message analysis for {account_id}: {e}")
        return {}


def get_account_message_statistics(account_id: int) -> Dict[str, Any]:
    """Get comprehensive message statistics for a specific account."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                
                # Basic counts
                cursor.execute("SELECT COUNT(*) AS count FROM messages WHERE account_id = %s", (account_id,))
                stats['total_messages'] = cursor.fetchone()['count']
                
                cursor.execute("SELECT COUNT(*) AS count FROM messages WHERE account_id = %s AND used = TRUE", (account_id,))
                stats['used_messages'] = cursor.fetchone()['count']
                
                stats['unused_messages'] = stats['total_messages'] - stats['used_messages']
                
                # Workflow status breakdown
                cursor.execute("""
                    SELECT workflow_status, COUNT(*) as count
                    FROM messages 
                    WHERE account_id = %s 
                    GROUP BY workflow_status
                """, (account_id,))
                stats['workflow_status_breakdown'] = {row['workflow_status']: row['count'] for row in cursor.fetchall()}
                
                # Prompt breakdown
                cursor.execute("""
                    SELECT p.name, p.prompt_type, COUNT(m.messages_id) as message_count
                    FROM messages m
                    JOIN prompts p ON m.prompt_id = p.prompt_id
                    WHERE m.account_id = %s
                    GROUP BY p.prompt_id, p.name, p.prompt_type
                    ORDER BY message_count DESC
                """, (account_id,))
                stats['prompt_breakdown'] = [dict(row) for row in cursor.fetchall()]
                
                return stats
    except Exception as e:
        logger.error(f"Error fetching account message statistics for {account_id}: {e}")
        return {}

