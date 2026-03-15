import logging
from typing import List, Optional, Dict, Any
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from .connection import get_postgres_connection

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# ============================================================================
# STATS FUNCTIONS - UPDATED FOR CONTENT-ONLY ARCHITECTURE
# ============================================================================

def get_accounts_stats_cached() -> Dict[str, Any]:
    """Get statistics about accounts from the database, cached for performance."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}

                # Total accounts
                cursor.execute("SELECT COUNT(*) AS count FROM accounts")
                result = cursor.fetchone()
                stats['total_accounts'] = result['count'] if result else 0

                # Active accounts (have prompts)
                cursor.execute("""
                    SELECT COUNT(DISTINCT account_id) AS count
                    FROM prompts
                    WHERE is_active = TRUE
                """)
                result = cursor.fetchone()
                stats['active_accounts'] = result['count'] if result else 0

                # Total prompts
                cursor.execute("SELECT COUNT(*) AS count FROM prompts WHERE is_active = TRUE")
                result = cursor.fetchone()
                stats['total_prompts'] = result['count'] if result else 0

                # Total content by type
                cursor.execute("""
                    SELECT
                        COUNT(*) as total_content,
                        COUNT(CASE WHEN used = TRUE THEN 1 END) as used_content,
                        COUNT(CASE WHEN used = FALSE THEN 1 END) as unused_content
                    FROM content
                """)
                result = cursor.fetchone()
                stats['total_content'] = result['total_content'] if result else 0
                stats['used_content'] = result['used_content'] if result else 0
                stats['unused_content'] = result['unused_content'] if result else 0

                # Content by custom types
                cursor.execute("""
                    SELECT content_type, COUNT(*) as count
                    FROM content
                    GROUP BY content_type
                """)
                content_by_type = cursor.fetchall()
                stats['content_by_type'] = {row['content_type']: row['count'] for row in content_by_type}

                logger.info("Successfully retrieved account statistics from PostgreSQL.")
                return stats
    except Exception as e:
        logger.error(f"Error fetching account stats: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching account stats: {str(e)}")
        return {
            'total_accounts': 0,
            'active_accounts': 0,
            'total_prompts': 0,
            'total_content': 0,
            'used_content': 0,
            'unused_content': 0,
            'content_by_type': {}
        }


def get_detailed_accounts_stats(
    active_filter: Optional[bool] = None,
    has_prompts: Optional[bool] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
) -> Dict[str, Any]:
    """Fetches detailed statistics about accounts with filtering."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                base_conditions = ["1=1"]
                base_params = []

                if active_filter is not None:
                    if active_filter:
                        base_conditions.append("""
                            EXISTS (
                                SELECT 1 FROM prompts
                                WHERE prompts.account_id = accounts.account_id
                                AND prompts.is_active = TRUE
                            )
                        """)
                    else:
                        base_conditions.append("""
                            NOT EXISTS (
                                SELECT 1 FROM prompts
                                WHERE prompts.account_id = accounts.account_id
                                AND prompts.is_active = TRUE
                            )
                        """)

                if has_prompts is not None:
                    if has_prompts:
                        base_conditions.append("""
                            EXISTS (
                                SELECT 1 FROM prompts
                                WHERE prompts.account_id = accounts.account_id
                            )
                        """)
                    else:
                        base_conditions.append("""
                            NOT EXISTS (
                                SELECT 1 FROM prompts
                                WHERE prompts.account_id = accounts.account_id
                            )
                        """)

                if start_time:
                    base_conditions.append("created_time >= %s")
                    base_params.append(start_time)

                if end_time:
                    base_conditions.append("created_time <= %s")
                    base_params.append(end_time)

                base_where = " AND ".join(base_conditions)

                # Total accounts with filters
                cursor.execute(f"SELECT COUNT(*) AS count FROM accounts WHERE {base_where}", base_params)
                result = cursor.fetchone()
                stats['total_accounts'] = result['count'] if result else 0

                # Active accounts (with prompts)
                active_conditions = base_conditions + ["""
                    EXISTS (
                        SELECT 1 FROM prompts
                        WHERE prompts.account_id = accounts.account_id
                        AND prompts.is_active = TRUE
                    )
                """]
                active_where = " AND ".join(active_conditions)
                cursor.execute(f"SELECT COUNT(*) AS count FROM accounts WHERE {active_where}", base_params)
                result = cursor.fetchone()
                stats['active_accounts'] = result['count'] if result else 0

                # Total content
                cursor.execute(f"""
                    SELECT
                        COUNT(DISTINCT c.content_id) as total_content,
                        COUNT(DISTINCT c.content_id) FILTER (WHERE c.used = FALSE) as unused_content,
                        COUNT(DISTINCT c.content_id) FILTER (WHERE c.used = TRUE) as used_content
                    FROM content c
                    JOIN accounts a ON c.account_id = a.account_id
                    WHERE {base_where}
                """, base_params)
                result = cursor.fetchone()
                stats['total_content'] = result['total_content'] if result and result['total_content'] else 0
                stats['unused_content'] = result['unused_content'] if result and result['unused_content'] else 0
                stats['used_content'] = result['used_content'] if result and result['used_content'] else 0

                # Accounts with prompts
                cursor.execute(f"""
                    SELECT COUNT(DISTINCT prompts.account_id) as count
                    FROM prompts
                    JOIN accounts ON prompts.account_id = accounts.account_id
                    WHERE {base_where}
                """, base_params)
                result = cursor.fetchone()
                stats['accounts_with_prompts'] = result['count'] if result else 0

                logger.info("Successfully retrieved detailed account statistics.")
                return stats
    except Exception as e:
        logger.error(f"Error fetching detailed account stats: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching detailed account stats: {str(e)}")
        return {
            'total_accounts': 0,
            'active_accounts': 0,
            'total_content': 0,
            'unused_content': 0,
            'used_content': 0,
            'accounts_with_prompts': 0
        }


def get_system_overview_stats() -> Dict[str, Any]:
    """Get system-wide overview statistics."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}

                # Total accounts
                cursor.execute("SELECT COUNT(*) AS count FROM accounts")
                result = cursor.fetchone()
                stats['total_accounts'] = result['count'] if result else 0

                # Active prompts
                cursor.execute("SELECT COUNT(*) AS count FROM prompts WHERE is_active = TRUE")
                result = cursor.fetchone()
                stats['active_prompts'] = result['count'] if result else 0

                # Content statistics
                cursor.execute("""
                    SELECT
                        COUNT(*) AS total,
                        COUNT(CASE WHEN used = TRUE THEN 1 END) AS used,
                        COUNT(CASE WHEN used = FALSE THEN 1 END) AS unused
                    FROM content
                """)
                result = cursor.fetchone()
                stats['total_content'] = result['total'] if result and result['total'] else 0
                stats['used_content'] = result['used'] if result and result['used'] else 0
                stats['unused_content'] = result['unused'] if result and result['unused'] else 0

                # Calculate usage percentage
                if stats['total_content'] > 0:
                    stats['usage_percentage'] = round(
                        (stats['used_content'] / stats['total_content']) * 100,
                        2
                    )
                else:
                    stats['usage_percentage'] = 0.0

                # Unique content types
                cursor.execute("""
                    SELECT COUNT(DISTINCT content_type) as count
                    FROM content
                """)
                result = cursor.fetchone()
                stats['unique_content_types'] = result['count'] if result else 0

                logger.info("Successfully retrieved system overview statistics.")
                return stats
    except Exception as e:
        logger.error(f"Error fetching system overview stats: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching system stats: {str(e)}")
        return {}


# ============================================================================
# ACCOUNT CRUD OPERATIONS - UPDATED
# ============================================================================

def create_account(username: str, profile_id: str, profile_type: str = 'hyperbrowser') -> Optional[int]:
    """Creates a new account in the PostgreSQL accounts table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''
                    INSERT INTO accounts (username, profile_id, profile_type, created_time, updated_time)
                    VALUES (%s, %s, %s, NOW(), NOW())
                    RETURNING account_id
                    ''',
                    (username, profile_id, profile_type)
                )
                account_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"Account {username} created with ID: {account_id}")
                return account_id
    except Exception as e:
        logger.error(f"Error creating account {username}: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error creating account {username}: {str(e)}")
        return None


def update_account(account_id: int, username: str = None, profile_id: str = None,
                   profile_type: str = None) -> bool:
    """Updates an account in the PostgreSQL accounts table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                updates = []
                params = []

                if username:
                    updates.append("username = %s")
                    params.append(username)
                if profile_id:
                    updates.append("profile_id = %s")
                    params.append(profile_id)
                if profile_type:
                    updates.append("profile_type = %s")
                    params.append(profile_type)

                if not updates:
                    return False

                updates.append("updated_time = NOW()")
                params.append(account_id)

                query = f"UPDATE accounts SET {', '.join(updates)} WHERE account_id = %s"
                cursor.execute(query, params)
                conn.commit()
                logger.info(f"Account {account_id} updated successfully.")
                return True
    except Exception as e:
        logger.error(f"Error updating account {account_id}: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error updating account {account_id}: {str(e)}")
        return False


def delete_account(account_id: int) -> bool:
    """Deletes an account from the PostgreSQL accounts table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'DELETE FROM accounts WHERE account_id = %s',
                    (account_id,)
                )
                conn.commit()
                logger.info(f"Account {account_id} deleted successfully.")
                return True
    except Exception as e:
        logger.error(f"Error deleting account {account_id}: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error deleting account {account_id}: {str(e)}")
        return False


def update_account_stats(account_id: int, content_processed: int = 0) -> bool:
    """Updates account statistics - SIMPLIFIED for content-only architecture."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''
                    UPDATE accounts
                    SET
                        total_content_processed = total_content_processed + %s,
                        updated_time = NOW()
                    WHERE account_id = %s
                    ''',
                    (content_processed, account_id)
                )
                conn.commit()
                logger.info(f"Account {account_id} statistics updated successfully.")
                return True
    except Exception as e:
        logger.error(f"Error updating account {account_id} statistics: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error updating account {account_id} statistics: {str(e)}")
        return False


# ============================================================================
# ACCOUNT RETRIEVAL - UPDATED
# ============================================================================

def get_account_by_id(account_id: int) -> Optional[Dict[str, Any]]:
    """Fetches a single account by its ID."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        account_id,
                        username,
                        profile_id,
                        profile_type,
                        mongo_object_id,
                        created_time,
                        updated_time,
                        total_content_processed,
                        has_cookies,
                        cookies_last_updated
                    FROM accounts
                    WHERE account_id = %s
                """, (account_id,))
                account = cursor.fetchone()
                if account:
                    logger.info(f"Retrieved account {account_id} from PostgreSQL.")
                    return dict(account)
                else:
                    logger.warning(f"Account {account_id} not found.")
                    return None
    except Exception as e:
        logger.error(f"Error fetching account {account_id}: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching account: {str(e)}")
        return None


def get_all_accounts() -> List[Dict[str, Any]]:
    """Fetches all accounts with basic information."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        account_id,
                        username,
                        profile_id,
                        profile_type,
                        mongo_object_id,
                        created_time,
                        updated_time,
                        has_cookies
                    FROM accounts
                    ORDER BY username
                """)
                accounts = cursor.fetchall()
                logger.info(f"Retrieved {len(accounts)} accounts from PostgreSQL.")
                return [dict(account) for account in accounts]
    except Exception as e:
        logger.error(f"Error fetching all accounts: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching accounts: {str(e)}")
        return []


def get_comprehensive_accounts(
    limit: int = None,
    active: Optional[bool] = None,
    has_prompts: Optional[bool] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
) -> List[Dict[str, Any]]:
    """Fetches all accounts with comprehensive filtering - UPDATED."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT
                        a.account_id,
                        a.username,
                        a.profile_id,
                        a.profile_type,
                        a.mongo_object_id,
                        a.created_time,
                        a.updated_time,
                        a.total_content_processed,
                        a.has_cookies,
                        a.cookies_last_updated,
                        COUNT(DISTINCT p.prompt_id) as prompt_count,
                        COUNT(DISTINCT c.content_id) as content_count,
                        COUNT(DISTINCT c.content_id) FILTER (WHERE c.used = FALSE) as unused_content_count
                    FROM accounts a
                    LEFT JOIN prompts p ON a.account_id = p.account_id
                    LEFT JOIN content c ON a.account_id = c.account_id
                    WHERE 1=1
                """
                params = []

                if active is not None:
                    if active:
                        query += " AND EXISTS (SELECT 1 FROM prompts WHERE prompts.account_id = a.account_id AND prompts.is_active = TRUE)"
                    else:
                        query += " AND NOT EXISTS (SELECT 1 FROM prompts WHERE prompts.account_id = a.account_id AND prompts.is_active = TRUE)"

                if has_prompts is not None:
                    if has_prompts:
                        query += " AND EXISTS (SELECT 1 FROM prompts WHERE prompts.account_id = a.account_id)"
                    else:
                        query += " AND NOT EXISTS (SELECT 1 FROM prompts WHERE prompts.account_id = a.account_id)"

                if start_time:
                    query += " AND a.created_time >= %s"
                    params.append(start_time)

                if end_time:
                    query += " AND a.created_time <= %s"
                    params.append(end_time)

                query += " GROUP BY a.account_id ORDER BY a.created_time DESC"

                if limit is not None:
                    query += " LIMIT %s"
                    params.append(limit)

                cursor.execute(query, params)
                accounts = cursor.fetchall()
                logger.info(f"Retrieved {len(accounts)} comprehensive account records.")
                return [dict(account) for account in accounts]
    except Exception as e:
        logger.error(f"Error fetching comprehensive accounts: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error fetching comprehensive accounts: {str(e)}")
        return []


def get_all_active_accounts() -> List[Dict[str, Any]]:
    """Get all active accounts with their basic information."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        account_id,
                        username,
                        profile_id,
                        profile_type,
                        mongo_object_id,
                        created_time,
                        updated_time,
                        total_content_processed,
                        has_cookies
                    FROM accounts
                    ORDER BY account_id
                """)
                accounts = cursor.fetchall()
                logger.info(f"Retrieved {len(accounts)} active accounts")
                return [dict(account) for account in accounts]
    except Exception as e:
        logger.error(f"Error fetching active accounts: {e}")
        return []


# ============================================================================
# MONGODB INTEGRATION - UPDATED
# ============================================================================

def get_account_with_mongo_id(account_id: int) -> Optional[Dict[str, Any]]:
    """Get account including MongoDB object ID for cross-database operations."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        account_id,
                        username,
                        profile_id,
                        profile_type,
                        mongo_object_id,
                        created_time,
                        updated_time
                    FROM accounts
                    WHERE account_id = %s
                """, (account_id,))
                account = cursor.fetchone()
                if account:
                    logger.info(f"Retrieved account {account_id} with MongoDB ID")
                    return dict(account)
                return None
    except Exception as e:
        logger.error(f"Error fetching account with MongoDB ID: {e}")
        return None


def update_account_mongo_id(account_id: int, mongo_object_id: str) -> bool:
    """Update the MongoDB object ID for an account."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE accounts
                    SET mongo_object_id = %s, updated_time = NOW()
                    WHERE account_id = %s
                """, (mongo_object_id, account_id))
                conn.commit()
                logger.info(f"Updated MongoDB ID for account {account_id}")
                return True
    except Exception as e:
        logger.error(f"Error updating MongoDB ID for account {account_id}: {e}")
        return False


def get_account_by_mongo_id(mongo_object_id: str) -> Optional[Dict[str, Any]]:
    """Get account by MongoDB object ID."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        account_id,
                        username,
                        profile_id,
                        profile_type,
                        mongo_object_id,
                        created_time,
                        updated_time
                    FROM accounts
                    WHERE mongo_object_id = %s
                """, (mongo_object_id,))
                account = cursor.fetchone()
                if account:
                    logger.info(f"Retrieved account by MongoDB ID: {mongo_object_id}")
                    return dict(account)
                return None
    except Exception as e:
        logger.error(f"Error fetching account by MongoDB ID: {e}")
        return None


# ============================================================================
# CONTENT & PROMPT ANALYSIS - NEW
# ============================================================================

def get_account_content_summary(account_id: int) -> Dict[str, Any]:
    """Get a summary of all content for a specific account with dynamic content types."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                summary = {}

                # Get account info
                cursor.execute("SELECT username, profile_id, has_cookies FROM accounts WHERE account_id = %s", (account_id,))
                account_info = cursor.fetchone()
                if account_info:
                    summary['account'] = dict(account_info)

                # Get content counts by type
                cursor.execute("""
                    SELECT
                        content_type,
                        COUNT(*) as total,
                        COUNT(CASE WHEN used = TRUE THEN 1 END) as used,
                        COUNT(CASE WHEN used = FALSE THEN 1 END) as unused
                    FROM content
                    WHERE account_id = %s
                    GROUP BY content_type
                """, (account_id,))

                content_counts = cursor.fetchall()
                summary['content_counts'] = {row['content_type']: dict(row) for row in content_counts}

                # Get prompt counts
                cursor.execute("""
                    SELECT
                        COUNT(*) as total_prompts,
                        COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active_prompts,
                        COUNT(DISTINCT prompt_type) as unique_types
                    FROM prompts
                    WHERE account_id = %s
                """, (account_id,))

                prompt_counts = cursor.fetchone()
                summary['prompt_counts'] = dict(prompt_counts) if prompt_counts else {
                    'total_prompts': 0,
                    'active_prompts': 0,
                    'unique_types': 0
                }

                # Get list of prompt types
                cursor.execute("""
                    SELECT DISTINCT prompt_type
                    FROM prompts
                    WHERE account_id = %s AND is_active = TRUE
                """, (account_id,))

                prompt_types = cursor.fetchall()
                summary['prompt_types'] = [row['prompt_type'] for row in prompt_types]

                logger.info(f"Retrieved content summary for account {account_id}")
                return summary
    except Exception as e:
        logger.error(f"Error fetching content summary for account {account_id}: {e}")
        return {}


def get_prompt_effectiveness_analysis() -> List[Dict[str, Any]]:
    """Get analysis of most effective prompts with dynamic prompt types."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        p.name as prompt_name,
                        p.content as prompt_text,
                        p.prompt_type,
                        a.username,
                        COUNT(c.content_id) as total_usage,
                        COUNT(CASE WHEN c.used = TRUE THEN 1 END) as used_count,
                        COUNT(CASE WHEN c.used = FALSE THEN 1 END) as unused_count,
                        p.created_time,
                        p.is_active
                    FROM prompts p
                    LEFT JOIN accounts a ON p.account_id = a.account_id
                    LEFT JOIN content c ON p.prompt_id = c.prompt_id
                    GROUP BY p.prompt_id, p.name, p.content, p.prompt_type, a.username, p.created_time, p.is_active
                    ORDER BY total_usage DESC, p.created_time DESC
                    LIMIT 50
                """)
                prompt_analysis = cursor.fetchall()
                logger.info(f"Retrieved effectiveness analysis for {len(prompt_analysis)} prompts")
                return [dict(row) for row in prompt_analysis]
    except Exception as e:
        logger.error(f"Error fetching prompt effectiveness analysis: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching prompt analysis: {str(e)}")
        return []


def get_account_prompts(account_id: int, content_type: Optional[str] = None,
                       active_only: bool = True) -> List[Dict[str, Any]]:
    """Get prompts for a specific account."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT
                        prompt_id,
                        name,
                        content,
                        prompt_type,
                        created_time,
                        updated_time,
                        is_active
                    FROM prompts
                    WHERE account_id = %s
                """
                params = [account_id]

                if content_type:
                    query += " AND prompt_type = %s"
                    params.append(content_type)

                if active_only:
                    query += " AND is_active = TRUE"

                query += " ORDER BY updated_time DESC"

                cursor.execute(query, params)
                prompts = cursor.fetchall()
                logger.info(f"Retrieved {len(prompts)} prompts for account {account_id}")
                return [dict(prompt) for prompt in prompts]
    except Exception as e:
        logger.error(f"Error fetching prompts for account {account_id}: {e}")
        return []


def get_account_performance_comparison() -> List[Dict[str, Any]]:
    """Get performance comparison data for all accounts."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        a.username,
                        a.profile_id,
                        a.profile_type,
                        a.total_content_processed,
                        COUNT(DISTINCT p.prompt_id) as prompt_count,
                        COUNT(DISTINCT c.content_id) as total_content,
                        COUNT(DISTINCT c.content_id) FILTER (WHERE c.used = FALSE) as unused_content
                    FROM accounts a
                    LEFT JOIN prompts p ON a.account_id = p.account_id
                    LEFT JOIN content c ON a.account_id = c.account_id
                    GROUP BY a.account_id
                    ORDER BY a.total_content_processed DESC
                    LIMIT 20
                """)
                comparison_data = cursor.fetchall()
                logger.info(f"Retrieved performance comparison for {len(comparison_data)} accounts.")
                return [dict(row) for row in comparison_data]
    except Exception as e:
        logger.error(f"Error fetching account performance comparison: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching performance comparison: {str(e)}")
        return []


# ============================================================================
# DEPRECATED FUNCTIONS (REMOVED)
# ============================================================================

def create_default_prompts_for_account(account_id: int) -> bool:
    """
    DEPRECATED: This function has been removed to support custom prompt types.
    Users should create prompts with their own custom types through the UI.
    """
    logger.warning(f"create_default_prompts_for_account called for account {account_id} but this function is deprecated")
    logger.info("Users should create prompts with custom types through the Prompts page")
    return False
