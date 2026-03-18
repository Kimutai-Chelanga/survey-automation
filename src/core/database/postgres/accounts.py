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
# STATS FUNCTIONS - SURVEY AUTOMATION SCHEMA
# ============================================================================

def get_accounts_stats_cached() -> Dict[str, Any]:
    """Get statistics about accounts from the database."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}

                # Total accounts
                cursor.execute("SELECT COUNT(*) AS count FROM accounts")
                result = cursor.fetchone()
                stats['total_accounts'] = result['count'] if result else 0

                # Active accounts (is_active = TRUE)
                cursor.execute("SELECT COUNT(*) AS count FROM accounts WHERE is_active = TRUE")
                result = cursor.fetchone()
                stats['active_accounts'] = result['count'] if result else 0

                # Accounts with prompts
                cursor.execute("""
                    SELECT COUNT(DISTINCT account_id) AS count
                    FROM prompts
                    WHERE is_active = TRUE
                """)
                result = cursor.fetchone()
                stats['accounts_with_prompts'] = result['count'] if result else 0

                # Total active prompts
                cursor.execute("SELECT COUNT(*) AS count FROM prompts WHERE is_active = TRUE")
                result = cursor.fetchone()
                stats['total_prompts'] = result['count'] if result else 0

                # Questions stats
                cursor.execute("""
                    SELECT
                        COUNT(*) as total_questions,
                        COUNT(CASE WHEN is_active THEN 1 END) as active_questions,
                        COUNT(DISTINCT survey_site_id) as sites_covered,
                        COUNT(DISTINCT account_id) as accounts_with_questions
                    FROM questions
                """)
                result = cursor.fetchone()
                stats['total_questions'] = result['total_questions'] if result else 0
                stats['active_questions'] = result['active_questions'] if result else 0
                stats['sites_covered'] = result['sites_covered'] if result else 0
                stats['accounts_with_questions'] = result['accounts_with_questions'] if result else 0

                # Answers stats
                cursor.execute("""
                    SELECT
                        COUNT(*) as total_answers,
                        COUNT(DISTINCT account_id) as accounts_with_answers,
                        COUNT(DISTINCT submission_batch_id) as total_batches,
                        MAX(submitted_at) as last_answer_at
                    FROM answers
                """)
                result = cursor.fetchone()
                stats['total_answers'] = result['total_answers'] if result else 0
                stats['accounts_with_answers'] = result['accounts_with_answers'] if result else 0
                stats['total_batches'] = result['total_batches'] if result else 0
                stats['last_answer_at'] = result['last_answer_at'] if result else None

                # Questions by type breakdown
                cursor.execute("""
                    SELECT question_type, COUNT(*) as count
                    FROM questions
                    WHERE is_active = TRUE
                    GROUP BY question_type
                """)
                rows = cursor.fetchall()
                stats['questions_by_type'] = {row['question_type']: row['count'] for row in rows}

                logger.info("Successfully retrieved account statistics from PostgreSQL.")
                return stats
    except Exception as e:
        logger.error(f"Error fetching account stats: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching account stats: {str(e)}")
        return {
            'total_accounts': 0,
            'active_accounts': 0,
            'accounts_with_prompts': 0,
            'total_prompts': 0,
            'total_questions': 0,
            'active_questions': 0,
            'sites_covered': 0,
            'accounts_with_questions': 0,
            'total_answers': 0,
            'accounts_with_answers': 0,
            'total_batches': 0,
            'last_answer_at': None,
            'questions_by_type': {}
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
                    base_conditions.append("a.is_active = %s")
                    base_params.append(active_filter)

                if has_prompts is not None:
                    if has_prompts:
                        base_conditions.append("""
                            EXISTS (
                                SELECT 1 FROM prompts
                                WHERE prompts.account_id = a.account_id
                            )
                        """)
                    else:
                        base_conditions.append("""
                            NOT EXISTS (
                                SELECT 1 FROM prompts
                                WHERE prompts.account_id = a.account_id
                            )
                        """)

                if start_time:
                    base_conditions.append("a.created_time >= %s")
                    base_params.append(start_time)

                if end_time:
                    base_conditions.append("a.created_time <= %s")
                    base_params.append(end_time)

                base_where = " AND ".join(base_conditions)

                cursor.execute(f"SELECT COUNT(*) AS count FROM accounts a WHERE {base_where}", base_params)
                result = cursor.fetchone()
                stats['total_accounts'] = result['count'] if result else 0

                cursor.execute(f"""
                    SELECT COUNT(*) AS count FROM accounts a
                    WHERE {base_where} AND a.is_active = TRUE
                """, base_params)
                result = cursor.fetchone()
                stats['active_accounts'] = result['count'] if result else 0

                cursor.execute(f"""
                    SELECT
                        COUNT(DISTINCT q.question_id) as total_questions,
                        COUNT(DISTINCT ans.answer_id) as total_answers
                    FROM accounts a
                    LEFT JOIN questions q ON a.account_id = q.account_id
                    LEFT JOIN answers ans ON a.account_id = ans.account_id
                    WHERE {base_where}
                """, base_params)
                result = cursor.fetchone()
                stats['total_questions'] = result['total_questions'] if result else 0
                stats['total_answers'] = result['total_answers'] if result else 0

                cursor.execute(f"""
                    SELECT COUNT(DISTINCT p.account_id) as count
                    FROM prompts p
                    JOIN accounts a ON p.account_id = a.account_id
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
            'total_questions': 0,
            'total_answers': 0,
            'accounts_with_prompts': 0
        }


def get_system_overview_stats() -> Dict[str, Any]:
    """Get system-wide overview statistics."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}

                cursor.execute("SELECT COUNT(*) AS count FROM accounts")
                result = cursor.fetchone()
                stats['total_accounts'] = result['count'] if result else 0

                cursor.execute("SELECT COUNT(*) AS count FROM accounts WHERE is_active = TRUE")
                result = cursor.fetchone()
                stats['active_accounts'] = result['count'] if result else 0

                cursor.execute("SELECT COUNT(*) AS count FROM prompts WHERE is_active = TRUE")
                result = cursor.fetchone()
                stats['active_prompts'] = result['count'] if result else 0

                cursor.execute("""
                    SELECT
                        COUNT(*) AS total_questions,
                        COUNT(CASE WHEN is_active THEN 1 END) AS active_questions,
                        COUNT(DISTINCT question_type) AS unique_question_types,
                        COUNT(DISTINCT survey_site_id) AS sites_with_questions
                    FROM questions
                """)
                result = cursor.fetchone()
                stats['total_questions'] = result['total_questions'] if result else 0
                stats['active_questions'] = result['active_questions'] if result else 0
                stats['unique_question_types'] = result['unique_question_types'] if result else 0
                stats['sites_with_questions'] = result['sites_with_questions'] if result else 0

                cursor.execute("""
                    SELECT
                        COUNT(*) AS total_answers,
                        COUNT(DISTINCT account_id) AS answering_accounts,
                        COUNT(DISTINCT submission_batch_id) AS total_batches
                    FROM answers
                """)
                result = cursor.fetchone()
                stats['total_answers'] = result['total_answers'] if result else 0
                stats['answering_accounts'] = result['answering_accounts'] if result else 0
                stats['total_batches'] = result['total_batches'] if result else 0

                # Response rate: answers / questions
                if stats['total_questions'] > 0:
                    stats['response_rate'] = round(
                        (stats['total_answers'] / stats['total_questions']) * 100, 2
                    )
                else:
                    stats['response_rate'] = 0.0

                cursor.execute("SELECT COUNT(*) AS count FROM survey_sites WHERE is_active = TRUE")
                result = cursor.fetchone()
                stats['active_survey_sites'] = result['count'] if result else 0

                logger.info("Successfully retrieved system overview statistics.")
                return stats
    except Exception as e:
        logger.error(f"Error fetching system overview stats: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching system stats: {str(e)}")
        return {}


# ============================================================================
# ACCOUNT CRUD OPERATIONS
# ============================================================================

def create_account(username: str, profile_id: str = None, profile_type: str = 'local_chrome',
                   country: str = None) -> Optional[int]:
    """Creates a new account in the PostgreSQL accounts table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''
                    INSERT INTO accounts (username, profile_id, profile_type, country, created_time, updated_time)
                    VALUES (%s, %s, %s, %s, NOW(), NOW())
                    RETURNING account_id
                    ''',
                    (username, profile_id, profile_type, country)
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
                   profile_type: str = None, country: str = None,
                   is_active: bool = None) -> bool:
    """Updates an account in the PostgreSQL accounts table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                updates = []
                params = []

                if username is not None:
                    updates.append("username = %s")
                    params.append(username)
                if profile_id is not None:
                    updates.append("profile_id = %s")
                    params.append(profile_id)
                if profile_type is not None:
                    updates.append("profile_type = %s")
                    params.append(profile_type)
                if country is not None:
                    updates.append("country = %s")
                    params.append(country)
                if is_active is not None:
                    updates.append("is_active = %s")
                    params.append(is_active)

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
                cursor.execute('DELETE FROM accounts WHERE account_id = %s', (account_id,))
                conn.commit()
                logger.info(f"Account {account_id} deleted successfully.")
                return True
    except Exception as e:
        logger.error(f"Error deleting account {account_id}: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error deleting account {account_id}: {str(e)}")
        return False


def update_account_stats(account_id: int, surveys_processed: int = 0) -> bool:
    """Updates account survey statistics."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''
                    UPDATE accounts
                    SET
                        total_surveys_processed = total_surveys_processed + %s,
                        updated_time = NOW()
                    WHERE account_id = %s
                    ''',
                    (surveys_processed, account_id)
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
# ACCOUNT RETRIEVAL
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
                        country,
                        profile_id,
                        profile_type,
                        mongo_object_id,
                        created_time,
                        updated_time,
                        total_surveys_processed,
                        has_cookies,
                        cookies_last_updated,
                        is_active
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
                        country,
                        profile_id,
                        profile_type,
                        mongo_object_id,
                        created_time,
                        updated_time,
                        has_cookies,
                        is_active
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


def get_all_active_accounts() -> List[Dict[str, Any]]:
    """Get all active accounts with their basic information."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        account_id,
                        username,
                        country,
                        profile_id,
                        profile_type,
                        mongo_object_id,
                        created_time,
                        updated_time,
                        total_surveys_processed,
                        has_cookies,
                        is_active
                    FROM accounts
                    WHERE is_active = TRUE
                    ORDER BY account_id
                """)
                accounts = cursor.fetchall()
                logger.info(f"Retrieved {len(accounts)} active accounts")
                return [dict(account) for account in accounts]
    except Exception as e:
        logger.error(f"Error fetching active accounts: {e}")
        return []


def get_comprehensive_accounts(
    limit: int = None,
    active: Optional[bool] = None,
    has_prompts: Optional[bool] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
) -> List[Dict[str, Any]]:
    """Fetches all accounts with comprehensive filtering and survey stats."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT
                        a.account_id,
                        a.username,
                        a.country,
                        a.profile_id,
                        a.profile_type,
                        a.mongo_object_id,
                        a.created_time,
                        a.updated_time,
                        a.total_surveys_processed,
                        a.has_cookies,
                        a.cookies_last_updated,
                        a.is_active,
                        COUNT(DISTINCT p.prompt_id) as prompt_count,
                        COUNT(DISTINCT q.question_id) as question_count,
                        COUNT(DISTINCT ans.answer_id) as answer_count,
                        COUNT(DISTINCT q.survey_site_id) as sites_participated,
                        MAX(ans.submitted_at) as last_answer_at
                    FROM accounts a
                    LEFT JOIN prompts p ON a.account_id = p.account_id
                    LEFT JOIN questions q ON a.account_id = q.account_id
                    LEFT JOIN answers ans ON a.account_id = ans.account_id
                    WHERE 1=1
                """
                params = []

                if active is not None:
                    query += " AND a.is_active = %s"
                    params.append(active)

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

                query += """
                    GROUP BY a.account_id, a.username, a.country, a.profile_id, a.profile_type,
                             a.mongo_object_id, a.created_time, a.updated_time,
                             a.total_surveys_processed, a.has_cookies, a.cookies_last_updated, a.is_active
                    ORDER BY a.created_time DESC
                """

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


# ============================================================================
# MONGODB INTEGRATION
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
                        country,
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
                        country,
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
# SURVEY-SPECIFIC ANALYSIS
# ============================================================================

def get_account_survey_summary(account_id: int) -> Dict[str, Any]:
    """Get a full survey summary for a specific account."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                summary = {}

                # Account info
                cursor.execute("""
                    SELECT username, country, profile_id, has_cookies, is_active,
                           total_surveys_processed
                    FROM accounts WHERE account_id = %s
                """, (account_id,))
                account_info = cursor.fetchone()
                if account_info:
                    summary['account'] = dict(account_info)

                # Prompt info (one per account)
                cursor.execute("""
                    SELECT prompt_id, name, content, prompt_type, is_active, created_time
                    FROM prompts
                    WHERE account_id = %s
                """, (account_id,))
                prompt = cursor.fetchone()
                summary['prompt'] = dict(prompt) if prompt else None

                # Questions extracted by this account
                cursor.execute("""
                    SELECT
                        COUNT(*) as total_questions,
                        COUNT(CASE WHEN is_active THEN 1 END) as active_questions,
                        COUNT(DISTINCT survey_site_id) as sites_covered,
                        COUNT(DISTINCT question_type) as question_types
                    FROM questions
                    WHERE account_id = %s
                """, (account_id,))
                q_stats = cursor.fetchone()
                summary['question_stats'] = dict(q_stats) if q_stats else {}

                # Questions by type
                cursor.execute("""
                    SELECT question_type, COUNT(*) as count
                    FROM questions
                    WHERE account_id = %s AND is_active = TRUE
                    GROUP BY question_type
                """, (account_id,))
                summary['questions_by_type'] = {
                    row['question_type']: row['count'] for row in cursor.fetchall()
                }

                # Answers submitted by this account
                cursor.execute("""
                    SELECT
                        COUNT(*) as total_answers,
                        COUNT(DISTINCT submission_batch_id) as total_batches,
                        MIN(submitted_at) as first_answer,
                        MAX(submitted_at) as last_answer
                    FROM answers
                    WHERE account_id = %s
                """, (account_id,))
                a_stats = cursor.fetchone()
                summary['answer_stats'] = dict(a_stats) if a_stats else {}

                # Extraction state per site
                cursor.execute("""
                    SELECT
                        ss.country,
                        es.last_extraction_time,
                        es.questions_found_last_run,
                        es.last_extraction_batch_id
                    FROM extraction_state es
                    JOIN survey_sites ss ON es.site_id = ss.site_id
                    WHERE es.account_id = %s
                    ORDER BY es.last_extraction_time DESC
                """, (account_id,))
                summary['extraction_history'] = [dict(row) for row in cursor.fetchall()]

                logger.info(f"Retrieved survey summary for account {account_id}")
                return summary
    except Exception as e:
        logger.error(f"Error fetching survey summary for account {account_id}: {e}")
        return {}


def get_account_performance_comparison() -> List[Dict[str, Any]]:
    """Get performance comparison data for all accounts."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        a.username,
                        a.country,
                        a.profile_type,
                        a.total_surveys_processed,
                        COUNT(DISTINCT p.prompt_id) as prompt_count,
                        COUNT(DISTINCT q.question_id) as questions_extracted,
                        COUNT(DISTINCT ans.answer_id) as answers_submitted,
                        COUNT(DISTINCT q.survey_site_id) as sites_participated,
                        MAX(ans.submitted_at) as last_active
                    FROM accounts a
                    LEFT JOIN prompts p ON a.account_id = p.account_id
                    LEFT JOIN questions q ON a.account_id = q.account_id
                    LEFT JOIN answers ans ON a.account_id = ans.account_id
                    GROUP BY a.account_id, a.username, a.country, a.profile_type, a.total_surveys_processed
                    ORDER BY answers_submitted DESC
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


def get_account_prompts(account_id: int, active_only: bool = True) -> Optional[Dict[str, Any]]:
    """
    Get prompt for a specific account.
    Returns a single prompt dict or None (one prompt per account by schema design).
    """
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

                if active_only:
                    query += " AND is_active = TRUE"

                cursor.execute(query, params)
                prompt = cursor.fetchone()
                if prompt:
                    logger.info(f"Retrieved prompt for account {account_id}")
                    return dict(prompt)
                return None
    except Exception as e:
        logger.error(f"Error fetching prompt for account {account_id}: {e}")
        return None