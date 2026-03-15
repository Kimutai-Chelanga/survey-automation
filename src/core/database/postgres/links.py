# File: src/core/database/postgres/links.py
# ENHANCED: Added extracted_urls table support with connection tracking
# FIXED: Stats functions now properly count all records
# UPDATED: Added delete_link with full cleanup
# UPDATED: Added has_chat_link / has_user_id filters to get_comprehensive_links_with_filters

import logging
from typing import List, Optional, Dict, Any, Tuple, Union
from psycopg2.extras import RealDictCursor
from datetime import datetime, date

from .connection import get_postgres_connection

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# =============================================================================
# DELETE LINK
# =============================================================================

def delete_link(link_id: int, cleanup_mongo: bool = True) -> Dict[str, Any]:
    """
    Delete a single link row with full referential cleanup.

    Cleanup order (to respect FK constraints):
      1. link_content_mappings  — junction table rows for this link
      2. link_content_connections — connection-tracking rows for this link
      3. content table — nullify connected_link_id back-references
         (sets connected_link_id = NULL, link_connection_status = 'orphaned')
      4. MongoDB workflow_metadata — optionally clear has_link / link_url fields
      5. links table — DELETE the row itself

    Args:
        link_id: Primary key of the row to delete in the links table.
        cleanup_mongo: If True, attempt to reset MongoDB workflow metadata
                       that references this link (best-effort, never blocks delete).

    Returns:
        Dict with keys:
          deleted            bool
          link_id            int
          mappings_deleted   int
          connections_deleted int
          content_nullified  int   (content rows whose back-reference was cleared)
          mongo_reset        int   (MongoDB docs updated)
          error              str | None
    """
    result: Dict[str, Any] = {
        "deleted": False,
        "link_id": link_id,
        "mappings_deleted": 0,
        "connections_deleted": 0,
        "content_nullified": 0,
        "mongo_reset": 0,
        "error": None,
    }

    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # 0. Verify link exists and grab workflow name for Mongo lookup
                cursor.execute(
                    "SELECT links_id, connected_via_workflow FROM links WHERE links_id = %s",
                    (link_id,)
                )
                row = cursor.fetchone()
                if not row:
                    result["error"] = f"links_id={link_id} not found"
                    return result

                workflow_name = row.get("connected_via_workflow")

                # 1. Delete link_content_mappings
                cursor.execute(
                    "DELETE FROM link_content_mappings WHERE link_id = %s",
                    (link_id,)
                )
                result["mappings_deleted"] = cursor.rowcount

                # 2. Delete link_content_connections
                cursor.execute(
                    "DELETE FROM link_content_connections WHERE link_id = %s",
                    (link_id,)
                )
                result["connections_deleted"] = cursor.rowcount

                # 3. Nullify content back-references
                cursor.execute(
                    """
                    UPDATE content
                    SET connected_link_id       = NULL,
                        link_connection_status  = 'orphaned',
                        link_connection_time    = NULL,
                        connected_via_workflow  = NULL
                    WHERE connected_link_id = %s
                    """,
                    (link_id,)
                )
                result["content_nullified"] = cursor.rowcount

                # 4. Delete the link row itself
                cursor.execute(
                    "DELETE FROM links WHERE links_id = %s",
                    (link_id,)
                )
                deleted_rows = cursor.rowcount
                conn.commit()

        if deleted_rows == 1:
            result["deleted"] = True
            logger.info(
                f"✓ Deleted links_id={link_id} "
                f"(mappings={result['mappings_deleted']}, "
                f"connections={result['connections_deleted']}, "
                f"content_nullified={result['content_nullified']})"
            )
        else:
            result["error"] = "DELETE affected 0 rows — already deleted?"
            return result

        # 5. MongoDB cleanup (best-effort, outside the PG transaction)
        if cleanup_mongo and workflow_name:
            try:
                from src.core.database.mongodb.connection import get_mongo_collection
                mongo_collection = get_mongo_collection('workflow_metadata', db_name='messages_db')
                if mongo_collection:
                    mongo_result = mongo_collection.update_many(
                        {"connected_via_workflow": workflow_name,
                         "linked_link_id": link_id},
                        {"$set": {
                            "has_link": False,
                            "link_url": None,
                            "linked_link_id": None,
                            "updated_at": datetime.utcnow()
                        }}
                    )
                    result["mongo_reset"] = mongo_result.modified_count
            except Exception as mongo_err:
                logger.warning(f"MongoDB cleanup for links_id={link_id} failed (non-fatal): {mongo_err}")

    except Exception as e:
        logger.error(f"Error deleting links_id={link_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        result["error"] = str(e)

    return result


# =============================================================================
# EXTRACTED URLS FUNCTIONS
# =============================================================================

def get_extracted_urls_with_filters(
    limit: int = None,
    is_reply: Optional[bool] = None,
    parent_extracted: Optional[bool] = None,
    parent_extraction_attempted: Optional[bool] = None,
    linked_to_links_table: Optional[bool] = None,
    extraction_batch_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetches extracted URLs from PostgreSQL with filtering.
    Shows the staging area before URLs are moved to links table.
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT
                        eu.extracted_url_id,
                        eu.account_id,
                        a.username as account_username,
                        eu.url,
                        eu.tweet_id,
                        eu.tweet_text,
                        eu.is_reply,
                        eu.extracted_at,
                        eu.parent_extracted,
                        eu.parent_extraction_attempted,
                        eu.parent_extraction_time,
                        eu.parent_url_id,
                        eu.parent_tweet_id,
                        eu.parent_tweet_url,
                        eu.linked_to_links_table,
                        eu.links_table_id,
                        eu.source_page,
                        eu.extraction_batch_id,
                        parent_eu.url as parent_url_full,
                        parent_eu.tweet_id as parent_tweet_id_full
                    FROM extracted_urls eu
                    LEFT JOIN accounts a ON eu.account_id = a.account_id
                    LEFT JOIN extracted_urls parent_eu ON eu.parent_url_id = parent_eu.extracted_url_id
                    WHERE 1=1
                """
                params = []

                if is_reply is not None:
                    query += " AND eu.is_reply = %s"
                    params.append(is_reply)

                if parent_extracted is not None:
                    query += " AND eu.parent_extracted = %s"
                    params.append(parent_extracted)

                if parent_extraction_attempted is not None:
                    query += " AND eu.parent_extraction_attempted = %s"
                    params.append(parent_extraction_attempted)

                if linked_to_links_table is not None:
                    query += " AND eu.linked_to_links_table = %s"
                    params.append(linked_to_links_table)

                if extraction_batch_id is not None:
                    query += " AND eu.extraction_batch_id = %s"
                    params.append(extraction_batch_id)

                query += " ORDER BY eu.extracted_at DESC"

                if limit is not None:
                    query += " LIMIT %s"
                    params.append(limit)

                cursor.execute(query, params)
                urls = cursor.fetchall()
                logger.info(f"✅ Retrieved {len(urls)} extracted URLs from PostgreSQL.")
                return [dict(row) for row in urls]

    except Exception as e:
        logger.error(f"Error fetching extracted URLs: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error fetching extracted URLs: {str(e)}")
        return []


def get_extracted_urls_stats(
    is_reply_filter: Optional[bool] = None,
    parent_extraction_attempted_filter: Optional[bool] = None,
    linked_to_links_table_filter: Optional[bool] = None
) -> Dict[str, Any]:
    """Fetches statistics about extracted URLs."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}

                cursor.execute("SELECT COUNT(*) AS count FROM extracted_urls")
                stats['total_extracted'] = (cursor.fetchone() or {}).get('count', 0)

                cursor.execute("SELECT COUNT(*) AS count FROM extracted_urls WHERE is_reply = TRUE")
                stats['total_replies'] = (cursor.fetchone() or {}).get('count', 0)

                cursor.execute("SELECT COUNT(*) AS count FROM extracted_urls WHERE is_reply = FALSE")
                stats['total_regular'] = (cursor.fetchone() or {}).get('count', 0)

                cursor.execute("""
                    SELECT COUNT(*) AS count FROM extracted_urls
                    WHERE is_reply = TRUE AND parent_extraction_attempted = FALSE
                """)
                stats['pending_parent_extraction'] = (cursor.fetchone() or {}).get('count', 0)

                cursor.execute("SELECT COUNT(*) AS count FROM extracted_urls WHERE parent_extracted = TRUE")
                stats['parents_found'] = (cursor.fetchone() or {}).get('count', 0)

                cursor.execute("""
                    SELECT COUNT(*) AS count FROM extracted_urls
                    WHERE is_reply = TRUE AND parent_extraction_attempted = TRUE AND parent_extracted = FALSE
                """)
                stats['parents_not_found'] = (cursor.fetchone() or {}).get('count', 0)

                cursor.execute("SELECT COUNT(*) AS count FROM extracted_urls WHERE linked_to_links_table = TRUE")
                stats['moved_to_links'] = (cursor.fetchone() or {}).get('count', 0)

                cursor.execute("SELECT COUNT(*) AS count FROM extracted_urls WHERE linked_to_links_table = FALSE")
                stats['pending_move_to_links'] = (cursor.fetchone() or {}).get('count', 0)

                cursor.execute("""
                    SELECT DISTINCT extraction_batch_id,
                           COUNT(*) as count,
                           MAX(extracted_at) as latest_extraction
                    FROM extracted_urls
                    WHERE extraction_batch_id IS NOT NULL
                    GROUP BY extraction_batch_id
                    ORDER BY latest_extraction DESC
                    LIMIT 10
                """)
                stats['recent_batches'] = [dict(row) for row in cursor.fetchall()]

                return stats

    except Exception as e:
        logger.error(f"Error fetching extracted URLs stats: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error fetching extracted URLs stats: {str(e)}")
        return {
            'total_extracted': 0, 'total_replies': 0, 'total_regular': 0,
            'pending_parent_extraction': 0, 'parents_found': 0, 'parents_not_found': 0,
            'moved_to_links': 0, 'pending_move_to_links': 0, 'recent_batches': []
        }


def get_extracted_url_links_connection() -> List[Dict[str, Any]]:
    """Get the connection mapping between extracted_urls and links table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT
                        eu.extracted_url_id,
                        eu.url as extracted_url,
                        eu.tweet_id as extracted_tweet_id,
                        eu.is_reply,
                        eu.extracted_at,
                        eu.linked_to_links_table,
                        eu.links_table_id,
                        l.links_id,
                        l.link as links_url,
                        l.tweet_id as links_tweet_id,
                        l.is_parent_tweet,
                        l.child_tweet_id,
                        l.scraped_time,
                        l.used,
                        l.filtered,
                        l.executed,
                        a.username
                    FROM extracted_urls eu
                    LEFT JOIN links l ON eu.links_table_id = l.links_id
                    LEFT JOIN accounts a ON eu.account_id = a.account_id
                    WHERE eu.linked_to_links_table = TRUE
                    ORDER BY eu.extracted_at DESC
                    LIMIT 100
                """
                cursor.execute(query)
                connections = cursor.fetchall()
                logger.info(f"✅ Retrieved {len(connections)} extracted_url → links connections")
                return [dict(row) for row in connections]

    except Exception as e:
        logger.error(f"Error fetching extracted_url links connections: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error fetching connections: {str(e)}")
        return []


# =============================================================================
# LINKS TABLE FUNCTIONS
# =============================================================================

def get_comprehensive_links_with_filters(
    limit: int = None,
    within_limit: Optional[bool] = None,
    used: Optional[bool] = None,
    filtered: Optional[bool] = None,
    executed: Optional[bool] = None,
    success: Optional[bool] = None,
    failure: Optional[bool] = None,
    tweeted_date: Optional[date] = None,
    is_parent_tweet: Optional[bool] = None,
    has_chat_link: Optional[bool] = None,
    has_user_id: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    """
    Fetches links from PostgreSQL with filtering.
    Shows ALL content items connected via junction table.

    Filter args:
        has_chat_link: True  → only rows where chat_link IS NOT NULL
                       False → only rows where chat_link IS NULL
        has_user_id:   True  → only rows where tweet_author_user_id IS NOT NULL
                       False → only rows where tweet_author_user_id IS NULL
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT
                        l.links_id,
                        l.account_id,
                        a.username as link_account_username,
                        l.link,
                        l.tweet_id,
                        l.tweeted_date,
                        l.tweeted_time,
                        l.within_limit,
                        l.used,
                        l.filtered,
                        l.executed,
                        l.success,
                        l.failure,
                        l.tweet_author_user_id,
                        l.chat_link,
                        l.extracted_url_id,
                        l.is_parent_tweet,
                        l.child_tweet_id,
                        eu.url as extracted_url,
                        eu.is_reply as was_reply_in_extraction,
                        eu.parent_extracted,
                        child_link.link as child_link_url,
                        child_link.tweet_id as child_tweet_id_value,
                        l.connected_content_id,
                        l.connected_via_workflow,
                        l.content_connection_time,
                        l.connection_status,
                        c_primary.content_name as primary_content_name,
                        c_primary.content_type as primary_content_type,
                        c_primary.used as primary_content_used,
                        ca_primary.username as primary_content_account_username,
                        array_agg(DISTINCT lcm.content_id) FILTER (WHERE lcm.content_id IS NOT NULL) as all_content_ids,
                        array_agg(DISTINCT c_all.content_name) FILTER (WHERE c_all.content_name IS NOT NULL) as all_content_names,
                        array_agg(DISTINCT c_all.content_type) FILTER (WHERE c_all.content_type IS NOT NULL) as all_content_types,
                        COUNT(DISTINCT lcm.content_id) FILTER (WHERE lcm.content_id IS NOT NULL) as total_content_count
                    FROM links l
                    LEFT JOIN accounts a ON l.account_id = a.account_id
                    LEFT JOIN extracted_urls eu ON l.extracted_url_id = eu.extracted_url_id
                    LEFT JOIN links child_link ON l.child_tweet_id = child_link.links_id
                    LEFT JOIN content c_primary ON l.connected_content_id = c_primary.content_id
                    LEFT JOIN accounts ca_primary ON c_primary.account_id = ca_primary.account_id
                    LEFT JOIN link_content_mappings lcm ON l.links_id = lcm.link_id
                    LEFT JOIN content c_all ON lcm.content_id = c_all.content_id
                    WHERE 1=1
                """
                params = []

                if within_limit is not None:
                    query += " AND l.within_limit = %s"
                    params.append(within_limit)
                if used is not None:
                    query += " AND l.used = %s"
                    params.append(used)
                if filtered is not None:
                    query += " AND l.filtered = %s"
                    params.append(filtered)
                if executed is not None:
                    query += " AND l.executed = %s"
                    params.append(executed)
                if success is not None:
                    query += " AND l.success = %s"
                    params.append(success)
                if failure is not None:
                    query += " AND l.failure = %s"
                    params.append(failure)
                if tweeted_date is not None:
                    query += " AND l.tweeted_date = %s"
                    params.append(tweeted_date)
                if is_parent_tweet is not None:
                    if is_parent_tweet:
                        query += " AND (l.is_parent_tweet = TRUE OR l.is_parent_tweet IS NULL)"
                    else:
                        query += " AND l.is_parent_tweet = FALSE"
                # ── New filters ───────────────────────────────────────────
                if has_chat_link is True:
                    query += " AND l.chat_link IS NOT NULL"
                elif has_chat_link is False:
                    query += " AND l.chat_link IS NULL"

                if has_user_id is True:
                    query += " AND l.tweet_author_user_id IS NOT NULL"
                elif has_user_id is False:
                    query += " AND l.tweet_author_user_id IS NULL"

                query += """
                    GROUP BY
                        l.links_id, l.account_id, a.username, l.link, l.tweet_id,
                        l.tweeted_date, l.tweeted_time, l.within_limit, l.used,
                        l.filtered, l.executed, l.success, l.failure,
                        l.tweet_author_user_id, l.chat_link,
                        l.extracted_url_id, l.is_parent_tweet, l.child_tweet_id,
                        eu.url, eu.is_reply, eu.parent_extracted,
                        child_link.link, child_link.tweet_id,
                        l.connected_content_id, l.connected_via_workflow,
                        l.content_connection_time, l.connection_status,
                        c_primary.content_name, c_primary.content_type,
                        c_primary.used, ca_primary.username
                    ORDER BY l.tweeted_time DESC NULLS LAST, l.links_id DESC
                """

                if limit is not None:
                    query += " LIMIT %s"
                    params.append(limit)

                cursor.execute(query, params)
                links = cursor.fetchall()
                logger.info(f"✅ Retrieved {len(links)} links from PostgreSQL")
                return [dict(row) for row in links]

    except Exception as e:
        logger.error(f"Error fetching links with filters: {e}")
        import traceback
        logger.error(traceback.format_exc())
        if STREAMLIT_AVAILABLE:
            st.error(f"Error fetching links: {str(e)}")
        return []


def get_detailed_links_stats(
    within_limit_filter: Optional[bool] = None,
    used_filter: Optional[bool] = None,
    filtered_filter: Optional[bool] = None,
    executed_filter: Optional[bool] = None,
    success_filter: Optional[bool] = None,
    failure_filter: Optional[bool] = None,
    tweeted_date: Optional[date] = None
) -> Dict[str, Any]:
    """
    Fetches detailed statistics about links including success/failure and
    tweet_author_user_id / chat_link coverage.
    Handles missing link_content_mappings table gracefully.
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}

                def _count(q, p=()):
                    cursor.execute(q, p)
                    r = cursor.fetchone()
                    return r['count'] if r else 0

                stats['total_links']            = _count("SELECT COUNT(*) AS count FROM links")
                stats['used_links']             = _count("SELECT COUNT(*) AS count FROM links WHERE used = TRUE")
                stats['unused_links']           = _count("SELECT COUNT(*) AS count FROM links WHERE used = FALSE")
                stats['filtered_links']         = _count("SELECT COUNT(*) AS count FROM links WHERE filtered = TRUE")
                stats['executed_links']         = _count("SELECT COUNT(*) AS count FROM links WHERE executed = TRUE")
                stats['within_limit_count']     = _count("SELECT COUNT(*) AS count FROM links WHERE within_limit = TRUE")
                stats['parent_tweets_count']    = _count("SELECT COUNT(*) AS count FROM links WHERE is_parent_tweet = TRUE")
                stats['with_extracted_url']     = _count("SELECT COUNT(*) AS count FROM links WHERE extracted_url_id IS NOT NULL")
                stats['with_content_connection']= _count("SELECT COUNT(*) AS count FROM links WHERE connected_content_id IS NOT NULL")
                stats['active_connections']     = _count("SELECT COUNT(*) AS count FROM links WHERE connection_status = 'active'")
                stats['success_count']          = _count("SELECT COUNT(*) AS count FROM links WHERE success = TRUE")
                stats['failure_count']          = _count("SELECT COUNT(*) AS count FROM links WHERE failure = TRUE")
                stats['both_true_count']        = _count("SELECT COUNT(*) AS count FROM links WHERE success = TRUE AND failure = TRUE")
                stats['not_executed_count']     = _count("SELECT COUNT(*) AS count FROM links WHERE success = FALSE AND failure = FALSE")

                # Author identity & chat link coverage
                stats['with_user_id']   = _count("SELECT COUNT(*) AS count FROM links WHERE tweet_author_user_id IS NOT NULL")
                stats['with_chat_link'] = _count("SELECT COUNT(*) AS count FROM links WHERE chat_link IS NOT NULL")
                stats['chat_coverage_pct'] = (
                    round((stats['with_chat_link'] / stats['total_links']) * 100, 1)
                    if stats['total_links'] > 0 else 0
                )

                stats['success_rate'] = (
                    round((stats['success_count'] / stats['executed_links']) * 100, 1)
                    if stats['executed_links'] > 0 else 0
                )

                # Junction table stats (graceful fallback)
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = 'link_content_mappings'
                    ) AS table_exists
                """)
                table_exists = (cursor.fetchone() or {}).get('table_exists', False)

                if table_exists:
                    try:
                        cursor.execute("""
                            SELECT
                                COUNT(DISTINCT link_id)    as links_with_mappings,
                                COUNT(DISTINCT content_id) as content_with_mappings,
                                COUNT(*)                   as total_mappings
                            FROM link_content_mappings
                        """)
                        jr = cursor.fetchone() or {}
                        stats['links_with_content_mappings']  = jr.get('links_with_mappings', 0) or 0
                        stats['content_with_link_mappings']   = jr.get('content_with_mappings', 0) or 0
                        stats['total_link_content_mappings']  = jr.get('total_mappings', 0) or 0

                        cursor.execute("""
                            SELECT COALESCE(AVG(content_count), 0)::NUMERIC(10,2) as avg_content
                            FROM (SELECT link_id, COUNT(*) as content_count FROM link_content_mappings GROUP BY link_id) s
                        """)
                        stats['avg_content_per_link'] = float((cursor.fetchone() or {}).get('avg_content', 0) or 0)

                        cursor.execute("""
                            SELECT COUNT(*) as count
                            FROM (SELECT link_id FROM link_content_mappings GROUP BY link_id HAVING COUNT(*) > 1) m
                        """)
                        stats['links_with_multiple_content'] = (cursor.fetchone() or {}).get('count', 0)

                    except Exception as e:
                        logger.warning(f"Could not fetch junction table stats: {e}")
                        stats.update({
                            'links_with_content_mappings': 0, 'content_with_link_mappings': 0,
                            'total_link_content_mappings': 0, 'avg_content_per_link': 0,
                            'links_with_multiple_content': 0
                        })
                else:
                    stats.update({
                        'links_with_content_mappings': 0, 'content_with_link_mappings': 0,
                        'total_link_content_mappings': 0, 'avg_content_per_link': 0,
                        'links_with_multiple_content': 0
                    })

                return stats

    except Exception as e:
        logger.error(f"Error fetching detailed link stats: {e}")
        import traceback
        logger.error(traceback.format_exc())
        if STREAMLIT_AVAILABLE:
            st.error(f"Error fetching link stats: {str(e)}")
        return {
            'total_links': 0, 'used_links': 0, 'unused_links': 0,
            'filtered_links': 0, 'executed_links': 0, 'within_limit_count': 0,
            'parent_tweets_count': 0, 'with_extracted_url': 0,
            'with_content_connection': 0, 'active_connections': 0,
            'success_count': 0, 'failure_count': 0, 'both_true_count': 0,
            'not_executed_count': 0, 'success_rate': 0,
            'with_user_id': 0, 'with_chat_link': 0, 'chat_coverage_pct': 0,
            'links_with_content_mappings': 0, 'content_with_link_mappings': 0,
            'total_link_content_mappings': 0, 'avg_content_per_link': 0,
            'links_with_multiple_content': 0
        }


# =============================================================================
# BACKWARD COMPATIBILITY - Legacy functions
# =============================================================================

def get_available_accounts() -> List[Dict[str, Any]]:
    """Get list of available accounts for filtering."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT account_id, username, profile_id, created_time
                    FROM accounts ORDER BY username
                """)
                return [dict(r) for r in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Error fetching available accounts: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching available accounts: {str(e)}")
        return []


def get_links_stats_cached() -> Dict[str, Any]:
    """Get statistics about links from the database."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                cursor.execute("SELECT COUNT(*) AS count FROM links")
                stats['total_links'] = (cursor.fetchone() or {}).get('count', 0)
                cursor.execute("SELECT COUNT(*) AS count FROM links WHERE used = TRUE")
                stats['used_links'] = (cursor.fetchone() or {}).get('count', 0)
                cursor.execute("SELECT COUNT(*) AS count FROM links WHERE filtered = TRUE")
                stats['filtered_links'] = (cursor.fetchone() or {}).get('count', 0)
                stats['unused_links'] = stats['total_links'] - stats['used_links']
                return stats
    except Exception as e:
        logger.error(f"Error fetching link stats: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching link stats: {str(e)}")
        return {'total_links': 0, 'used_links': 0, 'unused_links': 0, 'filtered_links': 0}


def reverse_filter_links_dag(account_id: Optional[int] = 1, days: Optional[int] = None) -> Dict[str, int]:
    """Reverse all operations done by filter_links DAG."""
    try:
        from src.core.database.mongodb.connection import get_mongo_collection
        from datetime import timezone

        reset_counts = {
            'links_reset': 0,
            'link_content_mappings_deleted': 0,
            'workflow_metadata_reset': 0,
            'content_workflow_links_deleted': 0
        }

        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                date_filter = ""
                params = [account_id]

                if days:
                    date_filter = " AND filtered_time >= CURRENT_DATE - INTERVAL '%s days'"
                    params.append(days)

                cursor.execute(f"""
                    SELECT DISTINCT links_id FROM links
                    WHERE account_id = %s AND (filtered = TRUE OR used = TRUE)
                    {date_filter}
                """, params)

                link_ids = [row[0] for row in cursor.fetchall()]

                if not link_ids:
                    logger.info(f"ℹ️ No links to reset for account_id={account_id}")
                    return reset_counts

                try:
                    cursor.execute("DELETE FROM link_content_mappings WHERE link_id = ANY(%s)", (link_ids,))
                    reset_counts['link_content_mappings_deleted'] = cursor.rowcount
                except Exception as e:
                    logger.warning(f"Could not delete from link_content_mappings: {e}")

                cursor.execute(f"""
                    UPDATE links SET
                        filtered = FALSE, filtered_time = NULL,
                        within_limit = FALSE,
                        used = FALSE, used_time = NULL,
                        processed_by_workflow = FALSE,
                        workflow_processed_time = NULL,
                        workflow_status = 'pending',
                        workflow_type = NULL,
                        connected_content_id = NULL,
                        connected_via_workflow = NULL,
                        content_connection_time = NULL,
                        connection_status = 'pending'
                    WHERE account_id = %s
                    {date_filter.replace('filtered_time', 'COALESCE(filtered_time, used_time, CURRENT_TIMESTAMP)')}
                """, params)

                reset_counts['links_reset'] = cursor.rowcount
                conn.commit()

                try:
                    mongo_collection = get_mongo_collection('workflow_metadata', db_name='messages_db')
                    if mongo_collection:
                        result = mongo_collection.update_many(
                            {'linked_link_id': {'$in': link_ids}, 'account_id': account_id},
                            {'$set': {
                                'has_link': False, 'link_url': None,
                                'execute': False, 'status': 'ready_for_links',
                                'updated_at': datetime.now(timezone.utc)
                            }}
                        )
                        reset_counts['workflow_metadata_reset'] = result.modified_count
                except Exception as mongo_error:
                    logger.warning(f"MongoDB workflow_metadata reset optional - error: {mongo_error}")

                try:
                    links_collection = get_mongo_collection('content_workflow_links', db_name='messages_db')
                    if links_collection:
                        result = links_collection.delete_many({
                            'postgres_content_id': {'$in': link_ids},
                            'account_id': account_id
                        })
                        reset_counts['content_workflow_links_deleted'] = result.deleted_count
                except Exception as mongo_error:
                    logger.warning(f"MongoDB content_workflow_links deletion optional - error: {mongo_error}")

                return reset_counts

    except Exception as e:
        logger.error(f"❌ Error reversing filter_links DAG: {e}")
        import traceback
        logger.error(traceback.format_exc())
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error reversing filter_links DAG: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        return reset_counts


def save_links_to_db(links: List[str], account_id: Optional[int] = None) -> int:
    """Inserts multiple links into the PostgreSQL links table."""
    inserted_count = 0
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                for link in links:
                    if link.strip():
                        try:
                            cursor.execute(
                                '''INSERT INTO links (account_id, link, tweet_id, tweeted_time, scraped_time, used)
                                   VALUES (%s, %s, %s, %s, %s, %s) RETURNING links_id''',
                                (account_id, link.strip(), extract_tweet_id(link.strip()),
                                 None, datetime.now(), False)
                            )
                            cursor.fetchone()
                            inserted_count += 1
                        except Exception as e:
                            if 'unique constraint' in str(e).lower():
                                continue
                            raise e
            conn.commit()
        return inserted_count
    except Exception as e:
        logger.error(f"Error saving links to PostgreSQL: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error saving links to PostgreSQL: {str(e)}")
        raise


def get_unused_links(
    limit: int = None,
    account_id: Optional[int] = None
) -> List[Tuple[int, str, Optional[str], Optional[int]]]:
    """Fetches unused links from the PostgreSQL links table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT links_id, link, mongo_object_id, account_id FROM links WHERE used = FALSE"
                params = []
                if account_id is not None:
                    query += " AND account_id = %s"
                    params.append(account_id)
                query += " ORDER BY scraped_time ASC"
                if limit is not None:
                    query += " LIMIT %s"
                    params.append(limit)
                cursor.execute(query, params)
                return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error fetching unused links from PostgreSQL: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error fetching unused links from PostgreSQL: {str(e)}")
        return []


def mark_link_as_used(link_id: int, mongo_object_id: Optional[str] = None):
    """Marks a link as used in the PostgreSQL links table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                query = 'UPDATE links SET used = TRUE, used_time = CURRENT_TIMESTAMP'
                params = []
                if mongo_object_id:
                    query += ", mongo_object_id = %s"
                    params.append(mongo_object_id)
                query += " WHERE links_id = %s"
                params.append(link_id)
                cursor.execute(query, params)
                conn.commit()
    except Exception as e:
        logger.error(f"Error marking link {link_id} as used: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error marking link {link_id} as used: {str(e)}")
        raise


def mark_link_as_filtered(link_id: int):
    """Marks a link as filtered in the PostgreSQL links table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'UPDATE links SET filtered = TRUE, filtered_time = CURRENT_TIMESTAMP WHERE links_id = %s',
                    (link_id,)
                )
                conn.commit()
    except Exception as e:
        logger.error(f"Error marking link {link_id} as filtered: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error marking link {link_id} as filtered: {str(e)}")
        raise


def update_link_workflow_info(link_id: int, workflow_id: Optional[int] = None,
                              mongo_workflow_id: Optional[str] = None,
                              workflow_status: str = 'pending'):
    """Updates workflow information for a link."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    UPDATE links SET
                        workflow_id = %s, mongo_workflow_id = %s,
                        workflow_status = %s,
                        workflow_processed_time = CURRENT_TIMESTAMP,
                        processed_by_workflow = TRUE
                    WHERE links_id = %s
                ''', (workflow_id, mongo_workflow_id, workflow_status, link_id))
                conn.commit()
    except Exception as e:
        logger.error(f"Error updating workflow info for link {link_id}: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error updating workflow info for link {link_id}: {str(e)}")
        raise


def update_link_mongo_id(link_id: int, mongo_object_id: str):
    """Updates the mongo_object_id for a given link_id."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'UPDATE links SET mongo_object_id = %s WHERE links_id = %s',
                    (mongo_object_id, link_id)
                )
                conn.commit()
    except Exception as e:
        logger.error(f"Error updating mongo_object_id for link {link_id}: {e}")
        raise


def extract_tweet_id(link: str) -> Optional[str]:
    """Extract tweet ID from a link."""
    import re
    match = re.search(r'status/(\d+)', link)
    return match.group(1) if match else None


def unfilter_all() -> int:
    """Resets filtering-related fields in the links table."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE links SET
                        tweeted_time = NULL, tweeted_date = NULL,
                        within_limit = FALSE,
                        used = FALSE, used_time = NULL,
                        workflow_id = NULL, mongo_workflow_id = NULL,
                        processed_by_workflow = FALSE,
                        workflow_processed_time = NULL,
                        workflow_status = 'pending',
                        execution_mode = 'execution',
                        filtered = FALSE, filtered_time = NULL
                """)
                updated_count = cursor.rowcount
                conn.commit()
                return updated_count
    except Exception as e:
        logger.error(f"❌ Error resetting filtering for links: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error resetting filtering for links: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        return 0


def delete_all(account_id: Optional[int] = 1) -> Dict[str, int]:
    """COMPLETE DELETION: Deletes all pipeline data for an account."""
    return delete_all_comprehensive(account_id)


def delete_all_comprehensive(account_id: Optional[int] = 1) -> Dict[str, int]:
    """COMPLETE DELETION: Reverses all operations from extract_links_weekly DAG."""
    try:
        from src.core.database.mongodb.connection import get_mongo_collection

        deletion_counts = {
            'links': 0, 'extracted_urls': 0, 'link_content_mappings': 0,
            'link_content_connections': 0, 'mongodb_workflows': 0,
            'content_nullified': 0  # Add this to track content updates
        }

        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                # Get all link IDs for this account
                cursor.execute("SELECT links_id FROM links WHERE account_id = %s", (account_id,))
                link_ids = [row[0] for row in cursor.fetchall()]

                if link_ids:
                    # CRITICAL FIX: First nullify content back-references
                    cursor.execute("""
                        UPDATE content
                        SET connected_link_id = NULL,
                            link_connection_status = 'orphaned',
                            link_connection_time = NULL,
                            connected_via_workflow = NULL
                        WHERE connected_link_id = ANY(%s)
                    """, (link_ids,))
                    deletion_counts['content_nullified'] = cursor.rowcount
                    
                    # Then delete junction table entries
                    cursor.execute("DELETE FROM link_content_mappings WHERE link_id = ANY(%s)", (link_ids,))
                    deletion_counts['link_content_mappings'] = cursor.rowcount
                    
                    # Then delete connection tracking entries
                    cursor.execute("DELETE FROM link_content_connections WHERE link_id = ANY(%s)", (link_ids,))
                    deletion_counts['link_content_connections'] = cursor.rowcount

                # Delete extracted URLs
                cursor.execute("DELETE FROM extracted_urls WHERE account_id = %s", (account_id,))
                deletion_counts['extracted_urls'] = cursor.rowcount
                
                # Finally delete the links themselves
                cursor.execute("DELETE FROM links WHERE account_id = %s", (account_id,))
                deletion_counts['links'] = cursor.rowcount
                
                conn.commit()

                # MongoDB cleanup (best-effort)
                if link_ids:
                    try:
                        mongo_collection = get_mongo_collection('workflow_metadata', db_name='messages_db')
                        if mongo_collection:
                            result = mongo_collection.delete_many({'linked_link_id': {'$in': link_ids}})
                            deletion_counts['mongodb_workflows'] = result.deleted_count
                            
                        executions_collection = get_mongo_collection('workflow_executions', db_name='messages_db')
                        if executions_collection:
                            executions_collection.delete_many({'postgres_link_id': {'$in': link_ids}})
                    except Exception as mongo_error:
                        logger.warning(f"MongoDB cleanup optional - error: {mongo_error}")

                return deletion_counts

    except Exception as e:
        logger.error(f"❌ Complete deletion failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Complete deletion failed: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        return deletion_counts
