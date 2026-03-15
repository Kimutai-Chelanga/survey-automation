# File: src/core/database/postgres/content_handler.py
# UNIFIED CONTENT HANDLER - Single table, prompt-based types only
# UPDATED: save_bulk returns (inserted_count, failed_count) tuple for DAG feedback loop
#          Removed double-filtering — DAG is the single authoritative filter
#          ADDED: delete_content and delete_content_by_name methods

import logging
from typing import List, Optional, Dict, Any, Tuple
from psycopg2.extras import RealDictCursor
from .connection import get_postgres_connection

logger = logging.getLogger(__name__)


class ContentHandler:
    """
    Unified handler for ALL content - uses ONLY the 'content' table.
    No hardcoded types. Everything is based on custom prompt types.

    UPDATED: save_bulk now returns (inserted_count, failed_count) so the caller
    can implement a deterministic retry loop. Content filtering is the DAG's
    responsibility — save_bulk only rejects items that are structurally invalid
    (empty, too short) and does NOT duplicate the DAG's filler phrase filtering.
    """

    def __init__(self, content_type: str):
        self.content_type = content_type
        logger.info(f"ContentHandler initialized for custom type: '{content_type}'")

    # ============================================================================
    # STATISTICS
    # ============================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for this content type."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(
                        "SELECT COUNT(*) AS count FROM content WHERE content_type = %s",
                        (self.content_type,)
                    )
                    total = cursor.fetchone()['count']

                    cursor.execute(
                        "SELECT COUNT(*) AS count FROM content WHERE content_type = %s AND used = TRUE",
                        (self.content_type,)
                    )
                    used = cursor.fetchone()['count']

                    return {
                        f'total_{self.content_type}': total,
                        f'used_{self.content_type}': used,
                        f'unused_{self.content_type}': total - used
                    }
        except Exception as e:
            logger.error(f"Error getting stats for {self.content_type}: {e}")
            return {
                f'total_{self.content_type}': 0,
                f'used_{self.content_type}': 0,
                f'unused_{self.content_type}': 0
            }

    # ============================================================================
    # CONTENT NAMES
    # ============================================================================

    def get_all_content_names(self, account_id: Optional[int] = None) -> List[str]:
        """Get all unique content names for this content type."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT DISTINCT content_name
                        FROM content
                        WHERE content_type = %s AND content_name IS NOT NULL AND content_name != ''
                    """
                    params = [self.content_type]

                    if account_id:
                        query += " AND account_id = %s"
                        params.append(account_id)

                    query += " ORDER BY content_name"

                    cursor.execute(query, params)
                    results = cursor.fetchall()

                    content_names = []
                    for row in results:
                        if row['content_name'] and row['content_name'].strip():
                            content_names.append(row['content_name'].strip())

                    logger.info(f"Found {len(content_names)} unique content names for type: {self.content_type}")

                    if not content_names:
                        cursor.execute(
                            "SELECT COUNT(*) FROM content WHERE content_type = %s",
                            (self.content_type,)
                        )
                        count = cursor.fetchone()[0]
                        if count > 0:
                            logger.warning(
                                f"Content type '{self.content_type}' has {count} records "
                                f"but no valid content names"
                            )

                    return content_names

        except Exception as e:
            logger.error(f"Error getting content names for {self.content_type}: {e}")
            return []

    # ============================================================================
    # BULK SAVE — returns (inserted_count, failed_count) for DAG feedback loop
    # ============================================================================

    def save_bulk(
        self,
        content_items: List[str],
        content_names: List[str],
        account_id: int,
        prompt_id: Optional[int] = None
    ) -> Tuple[int, int]:
        """
        Bulk save pre-filtered content items.

        The DAG is the single authoritative filter. This method only rejects items
        that are structurally invalid for DB insertion (empty or under 10 chars).
        It does NOT re-apply filler phrase filtering — that would cause unpredictable
        double losses and break the deterministic count guarantee.

        Args:
            content_items: List of pre-filtered content texts (from DAG filter layer)
            content_names: List of content names (same length as content_items)
            account_id: Account ID
            prompt_id: Optional prompt ID

        Returns:
            Tuple of (inserted_count, failed_count)
        """
        if not content_items:
            logger.warning("No content items to save")
            return 0, 0

        if len(content_names) != len(content_items):
            raise ValueError(
                f"content_names length ({len(content_names)}) must match "
                f"content_items length ({len(content_items)})"
            )

        inserted_count = 0
        failed_count = 0

        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                for i, (content_item, content_name) in enumerate(zip(content_items, content_names), 1):
                    cleaned_content = content_item.strip()
                    cleaned_name = content_name.strip()

                    # Structural validation only — content quality was handled by DAG filter
                    if not cleaned_content or len(cleaned_content) < 10:
                        logger.debug(f"Skipping item {i}: content empty or too short ({len(cleaned_content)} chars)")
                        failed_count += 1
                        continue

                    if not cleaned_name:
                        logger.debug(f"Skipping item {i}: content name empty")
                        failed_count += 1
                        continue

                    try:
                        cursor.execute(
                            """
                            INSERT INTO content (
                                content,
                                content_name,
                                content_type,
                                account_id,
                                prompt_id,
                                used,
                                created_time,
                                workflow_status
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
                            RETURNING content_id
                            """,
                            (
                                cleaned_content,
                                cleaned_name,
                                self.content_type,
                                account_id,
                                prompt_id,
                                False,
                                'pending'
                            )
                        )

                        result = cursor.fetchone()
                        content_id = result.get("content_id") if result else None

                        if content_id:
                            inserted_count += 1
                            logger.debug(
                                f"✓ Inserted {self.content_type} item {i} "
                                f"(name: {cleaned_name}, content_id={content_id})"
                            )
                        else:
                            failed_count += 1
                            logger.error(f"No content_id returned for {self.content_type} item {i}")

                    except Exception as e:
                        failed_count += 1
                        logger.exception(f"DB insert failed for {self.content_type} item {i}: {e}")

        logger.info(
            f"Bulk save '{self.content_type}': "
            f"{inserted_count} inserted, {failed_count} failed"
        )

        if inserted_count == 0 and len(content_items) > 0:
            raise RuntimeError(
                f"All {len(content_items)} inserts failed for content_type='{self.content_type}'"
            )

        return inserted_count, failed_count

    # ============================================================================
    # DELETE CONTENT
    # ============================================================================

    def delete_content(self, content_id: int, cleanup_links: bool = True) -> Dict[str, Any]:
        """
        Delete a single content item by content_id.

        If cleanup_links=True, also clears the back-reference on any connected
        link row (sets connected_content_id = NULL, connection_status = 'orphaned')
        and removes the link_content_connections record.

        Args:
            content_id: ID of the content row to delete
            cleanup_links: Whether to clean up link table references first

        Returns:
            Dict with keys: deleted (bool), content_id, link_ids_cleaned (list), error (str|None)
        """
        result: Dict[str, Any] = {
            "deleted": False,
            "content_id": content_id,
            "link_ids_cleaned": [],
            "error": None,
        }
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # 1. Verify this content_id belongs to this handler's type
                    cursor.execute(
                        "SELECT content_id, content_type FROM content WHERE content_id = %s",
                        (content_id,)
                    )
                    row = cursor.fetchone()
                    if not row:
                        result["error"] = f"content_id={content_id} not found"
                        return result
                    if row["content_type"] != self.content_type:
                        result["error"] = (
                            f"content_id={content_id} has type '{row['content_type']}', "
                            f"expected '{self.content_type}'"
                        )
                        return result

                    if cleanup_links:
                        # 2a. Find all links pointing to this content
                        cursor.execute(
                            "SELECT links_id FROM links WHERE connected_content_id = %s",
                            (content_id,)
                        )
                        link_rows = cursor.fetchall()
                        link_ids = [r["links_id"] for r in link_rows]

                        if link_ids:
                            # Nullify back-reference on links table
                            cursor.execute(
                                """
                                UPDATE links
                                SET connected_content_id = NULL,
                                    connection_status = 'orphaned',
                                    content_connection_time = NULL
                                WHERE connected_content_id = %s
                                """,
                                (content_id,)
                            )
                            # Remove connection records
                            cursor.execute(
                                "DELETE FROM link_content_connections WHERE content_id = %s",
                                (content_id,)
                            )
                            result["link_ids_cleaned"] = link_ids
                            logger.info(
                                f"Cleaned link references for content_id={content_id}: "
                                f"link_ids={link_ids}"
                            )

                    # 3. Delete the content row
                    cursor.execute(
                        "DELETE FROM content WHERE content_id = %s AND content_type = %s",
                        (content_id, self.content_type)
                    )
                    deleted_rows = cursor.rowcount
                    conn.commit()

                    if deleted_rows == 1:
                        result["deleted"] = True
                        logger.info(
                            f"✓ Deleted content_id={content_id} "
                            f"(type={self.content_type}, "
                            f"links_cleaned={result['link_ids_cleaned']})"
                        )
                    else:
                        result["error"] = "DELETE affected 0 rows — already deleted?"

        except Exception as e:
            logger.error(f"Error deleting content_id={content_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            result["error"] = str(e)

        return result

    def delete_content_by_name(
        self,
        content_name: str,
        account_id: Optional[int] = None,
        cleanup_links: bool = True,
    ) -> Dict[str, Any]:
        """
        Bulk-delete ALL content items that share a given content_name.

        Optionally scoped to a specific account_id.
        If cleanup_links=True, link table back-references are cleared first.

        Args:
            content_name: The content_name value to match
            account_id: Optional — restrict deletion to one account
            cleanup_links: Whether to clean up link table references

        Returns:
            Dict with keys: deleted_count, content_ids, link_ids_cleaned, error
        """
        result: Dict[str, Any] = {
            "deleted_count": 0,
            "content_ids": [],
            "link_ids_cleaned": [],
            "error": None,
        }
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # 1. Find all matching content_ids
                    query = """
                        SELECT content_id FROM content
                        WHERE content_type = %s AND content_name = %s
                    """
                    params: list = [self.content_type, content_name]
                    if account_id is not None:
                        query += " AND account_id = %s"
                        params.append(account_id)

                    cursor.execute(query, params)
                    rows = cursor.fetchall()
                    content_ids = [r["content_id"] for r in rows]

                    if not content_ids:
                        result["error"] = (
                            f"No content found for name='{content_name}', "
                            f"type='{self.content_type}'"
                            + (f", account_id={account_id}" if account_id else "")
                        )
                        return result

                    result["content_ids"] = content_ids

                    if cleanup_links:
                        # 2a. Find links pointing to any of these content items
                        cursor.execute(
                            "SELECT links_id FROM links WHERE connected_content_id = ANY(%s)",
                            (content_ids,)
                        )
                        link_rows = cursor.fetchall()
                        link_ids = [r["links_id"] for r in link_rows]

                        if link_ids:
                            cursor.execute(
                                """
                                UPDATE links
                                SET connected_content_id = NULL,
                                    connection_status = 'orphaned',
                                    content_connection_time = NULL
                                WHERE connected_content_id = ANY(%s)
                                """,
                                (content_ids,)
                            )
                            cursor.execute(
                                "DELETE FROM link_content_connections WHERE content_id = ANY(%s)",
                                (content_ids,)
                            )
                            result["link_ids_cleaned"] = link_ids
                            logger.info(
                                f"Cleaned {len(link_ids)} link references for "
                                f"content_name='{content_name}'"
                            )

                    # 3. Bulk delete
                    delete_query = """
                        DELETE FROM content
                        WHERE content_type = %s AND content_name = %s
                    """
                    delete_params: list = [self.content_type, content_name]
                    if account_id is not None:
                        delete_query += " AND account_id = %s"
                        delete_params.append(account_id)

                    cursor.execute(delete_query, delete_params)
                    result["deleted_count"] = cursor.rowcount
                    conn.commit()

                    logger.info(
                        f"✓ Bulk deleted {result['deleted_count']} rows for "
                        f"content_name='{content_name}', type='{self.content_type}' "
                        f"(links_cleaned={len(result['link_ids_cleaned'])})"
                    )

        except Exception as e:
            logger.error(
                f"Error bulk-deleting content_name='{content_name}' "
                f"for type '{self.content_type}': {e}"
            )
            import traceback
            logger.error(traceback.format_exc())
            result["error"] = str(e)

        return result

    # ============================================================================
    # COMPREHENSIVE DATA WITH LINK CONNECTIONS
    # ============================================================================

    def get_comprehensive_data(
        self,
        account_id: Optional[int] = None,
        used: Optional[bool] = None,
        limit: int = None,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get comprehensive data with optional filtering.
        Retrieves ALL link connections for content items that share workflows.

        Architecture:
        - 1 Workflow → Multiple Content Items (via workflow_metadata.content_items)
        - 1 Link → 1 Workflow → Multiple Content Items
        """
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT
                            c.content_id,
                            c.content,
                            c.content_name,
                            c.content_type,
                            c.used,
                            c.used_time,
                            c.created_time,
                            c.account_id,
                            c.prompt_id,
                            c.workflow_status,
                            c.automa_workflow_id,
                            c.workflow_name,
                            c.has_content,
                            c.workflow_generated_time,
                            a.username,
                            a.profile_id,
                            p.name as prompt_name,
                            p.prompt_type,
                            l_primary.links_id as primary_link_id,
                            l_primary.link as primary_link_url,
                            l_primary.connected_via_workflow as primary_link_workflow,
                            l_primary.content_connection_time as primary_link_connection_time,
                            l_primary.connection_status as primary_link_status,
                            l_primary.used as primary_link_used,
                            l_primary.tweet_id as primary_tweet_id,
                            l_primary.tweeted_time as primary_tweeted_time,
                            la_primary.username as primary_link_account_username
                        FROM content c
                        LEFT JOIN accounts a ON c.account_id = a.account_id
                        LEFT JOIN prompts p ON c.prompt_id = p.prompt_id
                        LEFT JOIN links l_primary ON c.content_id = l_primary.connected_content_id
                        LEFT JOIN accounts la_primary ON l_primary.account_id = la_primary.account_id
                        WHERE c.content_type = %s
                    """
                    params = [self.content_type]

                    if account_id is not None:
                        query += " AND c.account_id = %s"
                        params.append(account_id)

                    if used is not None:
                        query += " AND c.used = %s"
                        params.append(used)

                    query += " ORDER BY c.content_name, c.created_time DESC"

                    if limit is not None:
                        query += " LIMIT %s OFFSET %s"
                        params.extend([limit, offset])

                    cursor.execute(query, params)
                    results = cursor.fetchall()

                    logger.info(f"Retrieved {len(results)} {self.content_type} records from PostgreSQL")

                    content_data = [dict(row) for row in results]
                    content_data = self._enrich_with_all_link_connections(content_data, cursor)

                    for item in content_data:
                        item['has_link'] = (
                            item.get('primary_link_id') is not None or
                            bool(item.get('shared_link_ids'))
                        )

                        if item.get('primary_link_id'):
                            item['connected_link_id']           = item['primary_link_id']
                            item['link_url']                    = item['primary_link_url']
                            item['link_connected_via_workflow'] = item['primary_link_workflow']
                            item['link_connection_time']        = item['primary_link_connection_time']
                            item['link_connection_status']      = item['primary_link_status']
                            item['link_account_username']       = item['primary_link_account_username']
                            item['tweet_id']                    = item['primary_tweet_id']
                            item['tweeted_time']                = item['primary_tweeted_time']
                        elif item.get('shared_link_ids'):
                            item['connected_link_id']           = item['shared_link_ids'][0]
                            item['link_url']                    = item['shared_link_urls'][0] if item.get('shared_link_urls') else None
                            item['link_connected_via_workflow'] = item.get('automa_workflow_id')
                            item['link_connection_time']        = None
                            item['link_connection_status']      = 'active'
                            item['link_account_username']       = item['shared_link_accounts'][0] if item.get('shared_link_accounts') else None
                            item['tweet_id']                    = item['shared_tweet_ids'][0] if item.get('shared_tweet_ids') else None
                            item['tweeted_time']                = None
                        else:
                            item['connected_link_id']           = None
                            item['link_url']                    = None
                            item['link_connected_via_workflow'] = None
                            item['link_connection_time']        = None
                            item['link_connection_status']      = None
                            item['link_account_username']       = None
                            item['tweet_id']                    = None
                            item['tweeted_time']                = None

                    links_count = sum(1 for item in content_data if item['has_link'])
                    logger.info(
                        f"Found {links_count} content items with link connections "
                        f"out of {len(content_data)}"
                    )

                    return content_data

        except Exception as e:
            logger.error(f"Error getting comprehensive data for {self.content_type}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    def _enrich_with_all_link_connections(
        self,
        content_data: List[Dict[str, Any]],
        cursor
    ) -> List[Dict[str, Any]]:
        """
        Enrich content data with ALL link connections via shared workflows.

        Multiple content items can share the same workflow_id. When a link is
        assigned to that workflow, ALL content items in the workflow should
        reflect that connection.
        """
        try:
            workflow_map = {}
            for item in content_data:
                workflow_id = item.get('automa_workflow_id')
                if workflow_id:
                    workflow_map.setdefault(workflow_id, []).append(item)

            if not workflow_map:
                logger.debug("No workflows found in content data")
                for item in content_data:
                    item['shared_link_ids']    = []
                    item['shared_link_urls']   = []
                    item['shared_link_accounts'] = []
                    item['shared_tweet_ids']   = []
                return content_data

            logger.info(f"Found {len(workflow_map)} unique workflows across {len(content_data)} content items")

            workflow_links = {}

            for workflow_id, content_items in workflow_map.items():
                content_ids  = [item['content_id'] for item in content_items]
                workflow_name = content_items[0].get('workflow_name')

                cursor.execute("""
                    SELECT DISTINCT
                        l.links_id,
                        l.link,
                        l.tweet_id,
                        l.tweeted_time,
                        l.connected_content_id,
                        l.connected_via_workflow,
                        a.username as account_username
                    FROM links l
                    LEFT JOIN accounts a ON l.account_id = a.account_id
                    WHERE l.connected_content_id = ANY(%s)
                    OR l.connected_via_workflow = %s
                    ORDER BY l.tweeted_time DESC NULLS LAST
                """, (content_ids, workflow_name))

                links = cursor.fetchall()
                if links:
                    workflow_links[workflow_id] = [dict(link) for link in links]
                    logger.debug(
                        f"Workflow {workflow_id[:12]}... has {len(links)} link(s) "
                        f"connected to {len(content_ids)} content item(s)"
                    )

            for workflow_id, content_items in workflow_map.items():
                links = workflow_links.get(workflow_id, [])
                for item in content_items:
                    if links:
                        item['shared_link_ids']      = [l['links_id'] for l in links]
                        item['shared_link_urls']     = [l['link'] for l in links if l.get('link')]
                        item['shared_link_accounts'] = [l['account_username'] for l in links if l.get('account_username')]
                        item['shared_tweet_ids']     = [l['tweet_id'] for l in links if l.get('tweet_id')]
                    else:
                        item['shared_link_ids']      = []
                        item['shared_link_urls']     = []
                        item['shared_link_accounts'] = []
                        item['shared_tweet_ids']     = []

            # Handle content items without workflows
            for item in content_data:
                if not item.get('automa_workflow_id'):
                    item['shared_link_ids']      = []
                    item['shared_link_urls']     = []
                    item['shared_link_accounts'] = []
                    item['shared_tweet_ids']     = []

            total_with_shared = sum(
                1 for item in content_data if item.get('shared_link_ids')
            )
            logger.info(
                f"Enriched {len(content_data)} content items: "
                f"{total_with_shared} have shared link connections"
            )

            return content_data

        except Exception as e:
            logger.error(f"Error enriching with link connections: {e}")
            import traceback
            logger.error(traceback.format_exc())
            for item in content_data:
                if 'shared_link_ids' not in item:
                    item['shared_link_ids']      = []
                    item['shared_link_urls']     = []
                    item['shared_link_accounts'] = []
                    item['shared_tweet_ids']     = []
            return content_data

    # ============================================================================
    # ACCOUNT-SPECIFIC STATISTICS
    # ============================================================================

    def get_account_statistics(self, account_id: int) -> Dict[str, Any]:
        """Get comprehensive statistics for a specific account."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    stats = {}

                    cursor.execute(
                        "SELECT COUNT(*) AS count FROM content WHERE content_type = %s AND account_id = %s",
                        (self.content_type, account_id)
                    )
                    stats[f'total_{self.content_type}'] = cursor.fetchone()['count']

                    cursor.execute(
                        "SELECT COUNT(*) AS count FROM content WHERE content_type = %s AND account_id = %s AND used = TRUE",
                        (self.content_type, account_id)
                    )
                    stats[f'used_{self.content_type}'] = cursor.fetchone()['count']

                    stats[f'unused_{self.content_type}'] = (
                        stats[f'total_{self.content_type}'] - stats[f'used_{self.content_type}']
                    )

                    cursor.execute("""
                        SELECT workflow_status, COUNT(*) as count
                        FROM content
                        WHERE content_type = %s AND account_id = %s
                        GROUP BY workflow_status
                    """, (self.content_type, account_id))

                    stats['workflow_status_breakdown'] = {
                        row['workflow_status']: row['count'] for row in cursor.fetchall()
                    }

                    cursor.execute("""
                        SELECT p.name, p.prompt_type, COUNT(c.content_id) as count
                        FROM content c
                        JOIN prompts p ON c.prompt_id = p.prompt_id
                        WHERE c.content_type = %s AND c.account_id = %s
                        GROUP BY p.prompt_id, p.name, p.prompt_type
                        ORDER BY count DESC
                    """, (self.content_type, account_id))

                    stats['prompt_breakdown'] = [dict(row) for row in cursor.fetchall()]

                    cursor.execute("""
                        SELECT DATE(created_time) as date, COUNT(*) as count
                        FROM content
                        WHERE content_type = %s AND account_id = %s
                          AND created_time >= NOW() - INTERVAL '7 days'
                        GROUP BY DATE(created_time)
                        ORDER BY date
                    """, (self.content_type, account_id))

                    stats['recent_activity'] = [dict(row) for row in cursor.fetchall()]

                    return stats

        except Exception as e:
            logger.error(f"Error getting account statistics for {self.content_type}: {e}")
            return {}

    # ============================================================================
    # CONTENT NAME STATISTICS
    # ============================================================================

    def get_content_name_statistics(self, account_id: Optional[int] = None) -> Dict[str, Dict[str, int]]:
        """Get statistics for each content name (total, used, unused counts)."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT
                            content_name,
                            COUNT(*) as total,
                            COUNT(CASE WHEN used = TRUE THEN 1 END) as used,
                            COUNT(CASE WHEN used = FALSE THEN 1 END) as unused
                        FROM content
                        WHERE content_type = %s
                        AND content_name IS NOT NULL
                        AND content_name != ''
                    """
                    params = [self.content_type]

                    if account_id:
                        query += " AND account_id = %s"
                        params.append(account_id)

                    query += " GROUP BY content_name ORDER BY content_name"

                    cursor.execute(query, params)
                    results = cursor.fetchall()

                    stats_dict = {}
                    for row in results:
                        stats_dict[row['content_name']] = {
                            'total':  row['total'],
                            'used':   row['used'],
                            'unused': row['unused']
                        }

                    logger.info(
                        f"Retrieved statistics for {len(stats_dict)} content names "
                        f"for type: {self.content_type}"
                    )
                    return stats_dict

        except Exception as e:
            logger.error(f"Error getting content name statistics for {self.content_type}: {e}")
            return {}

    # ============================================================================
    # MARK AS USED
    # ============================================================================

    def mark_as_used(self, content_id: int):
        """Mark content as used."""
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    UPDATE content
                    SET used = TRUE, used_time = CURRENT_TIMESTAMP
                    WHERE content_id = %s AND content_type = %s
                    """,
                    (content_id, self.content_type)
                )

                if cursor.rowcount == 0:
                    raise RuntimeError(
                        f"No rows updated for content_id={content_id} "
                        f"and type={self.content_type}"
                    )

                logger.info(f"Marked {self.content_type} content_id {content_id} as used")

    # ============================================================================
    # GET CONTENT BY NAME
    # ============================================================================

    def get_content_by_name(
        self,
        content_name: str,
        account_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get all content items with a specific content name."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT
                            c.content_id,
                            c.content,
                            c.content_name,
                            c.content_type,
                            c.used,
                            c.used_time,
                            c.created_time,
                            c.account_id,
                            c.prompt_id,
                            c.workflow_status,
                            a.username
                        FROM content c
                        LEFT JOIN accounts a ON c.account_id = a.account_id
                        WHERE c.content_type = %s AND c.content_name = %s
                    """
                    params = [self.content_type, content_name]

                    if account_id:
                        query += " AND c.account_id = %s"
                        params.append(account_id)

                    query += " ORDER BY c.created_time DESC"

                    cursor.execute(query, params)
                    results = cursor.fetchall()
                    return [dict(row) for row in results]

        except Exception as e:
            logger.error(f"Error getting content by name '{content_name}' for {self.content_type}: {e}")
            return []

    # ============================================================================
    # GET UNUSED CONTENT NAMES
    # ============================================================================

    def get_unused_content_names(self, account_id: Optional[int] = None) -> List[str]:
        """Get all unique unused content names for this content type."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT DISTINCT content_name
                        FROM content
                        WHERE content_type = %s AND used = FALSE
                          AND content_name IS NOT NULL AND content_name != ''
                    """
                    params = [self.content_type]

                    if account_id:
                        query += " AND account_id = %s"
                        params.append(account_id)

                    query += " ORDER BY content_name"

                    cursor.execute(query, params)
                    results = cursor.fetchall()
                    return [row['content_name'] for row in results if row['content_name']]

        except Exception as e:
            logger.error(f"Error getting unused content names for {self.content_type}: {e}")
            return []

    # ============================================================================
    # VALIDATE CONTENT TYPE
    # ============================================================================

    def validate_content_type(self) -> bool:
        """Validate that this content type exists in the prompts table."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM prompts
                            WHERE prompt_type = %s AND is_active = TRUE
                        ) as exists
                        """,
                        (self.content_type,)
                    )
                    result = cursor.fetchone()
                    exists = result['exists'] if result else False

                    if not exists:
                        logger.warning(
                            f"Content type '{self.content_type}' does not exist "
                            f"or is not active in prompts table"
                        )
                    return exists

        except Exception as e:
            logger.error(f"Error validating content type '{self.content_type}': {e}")
            return False


# ============================================================================
# FACTORY FUNCTION
# ============================================================================

def get_content_handler(content_type: str) -> ContentHandler:
    """Factory function to get a content handler for any custom type."""
    handler = ContentHandler(content_type)
    logger.info(f"Factory created handler for content type: '{content_type}'")

    try:
        if not handler.validate_content_type():
            logger.warning(f"Content type '{content_type}' may not exist in prompts table")
    except Exception as e:
        logger.error(f"Error validating content type '{content_type}': {e}")

    return handler


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_all_content_types() -> List[str]:
    """Get all unique content types currently in the database."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT DISTINCT prompt_type
                    FROM prompts
                    WHERE prompt_type IS NOT NULL AND is_active = TRUE
                    ORDER BY prompt_type
                """)
                results = cursor.fetchall()
                types = [row['prompt_type'] for row in results]
                logger.info(f"Found {len(types)} unique content types")
                return types
    except Exception as e:
        logger.error(f"Error fetching content types: {e}")
        return []


def get_content_type_summary() -> List[Dict[str, Any]]:
    """Get summary statistics for all content types."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        c.content_type,
                        COUNT(*) as total_content,
                        COUNT(CASE WHEN c.used = FALSE THEN 1 END) as unused_content,
                        COUNT(CASE WHEN c.used = TRUE THEN 1 END) as used_content,
                        COUNT(DISTINCT c.account_id) as accounts_with_content,
                        COUNT(DISTINCT c.prompt_id) as unique_prompts,
                        MAX(c.created_time) as latest_created
                    FROM content c
                    GROUP BY c.content_type
                    ORDER BY c.content_type
                """)
                results = cursor.fetchall()
                return [dict(row) for row in results]
    except Exception as e:
        logger.error(f"Error getting content type summary: {e}")
        return []


def get_content_by_type_and_name(
    content_type: str,
    content_name: str,
    account_id: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Utility function to get content by type and name."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT
                        content_id,
                        content,
                        content_name,
                        content_type,
                        used,
                        used_time,
                        created_time,
                        account_id,
                        prompt_id,
                        workflow_status
                    FROM content
                    WHERE content_type = %s AND content_name = %s
                """
                params = [content_type, content_name]

                if account_id:
                    query += " AND account_id = %s"
                    params.append(account_id)

                query += " ORDER BY created_time DESC"

                cursor.execute(query, params)
                results = cursor.fetchall()
                return [dict(row) for row in results]

    except Exception as e:
        logger.error(
            f"Error getting content by type '{content_type}' "
            f"and name '{content_name}': {e}"
        )
        return []


# ============================================================================
# DEBUG/DIAGNOSTIC FUNCTIONS
# ============================================================================

def diagnose_content_type(content_type: str) -> Dict[str, Any]:
    """Diagnostic function to check what's available for a content type."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT prompt_id, name FROM prompts WHERE prompt_type = %s AND is_active = TRUE",
                    (content_type,)
                )
                prompts = cursor.fetchall()

                cursor.execute(
                    "SELECT COUNT(*) as count, COUNT(DISTINCT content_name) as unique_names "
                    "FROM content WHERE content_type = %s",
                    (content_type,)
                )
                content_stats = cursor.fetchone()

                cursor.execute(
                    "SELECT content_name FROM content WHERE content_type = %s "
                    "AND content_name IS NOT NULL LIMIT 5",
                    (content_type,)
                )
                sample_names = [row['content_name'] for row in cursor.fetchall() if row['content_name']]

                return {
                    'content_type':       content_type,
                    'prompts_found':      len(prompts),
                    'prompts':            [dict(p) for p in prompts],
                    'total_content':      content_stats['count'] if content_stats else 0,
                    'unique_content_names': content_stats['unique_names'] if content_stats else 0,
                    'sample_content_names': sample_names,
                    'has_content':        content_stats['count'] > 0 if content_stats else False,
                    'has_valid_names':    any(sample_names)
                }

    except Exception as e:
        logger.error(f"Error diagnosing content type '{content_type}': {e}")
        return {'content_type': content_type, 'error': str(e)}
