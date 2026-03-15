# File: src/core/database/postgres/link_content_sync.py
# LINK-CONTENT CONNECTION SYNCHRONIZATION
# Ensures bidirectional sync between links and content tables

import logging
from typing import Optional, Dict, Any
from psycopg2.extras import RealDictCursor
from .connection import get_postgres_connection

logger = logging.getLogger(__name__)


def sync_link_to_content_connection(
    link_id: int,
    content_id: int,
    workflow_name: str,
    automa_workflow_id: Optional[str] = None,
    account_id: Optional[int] = None,
    connection_status: str = 'active'
) -> bool:
    """
    Create bidirectional link between link and content.
    Updates BOTH links and content tables.

    Args:
        link_id: Link ID from links table
        content_id: Content ID from content table
        workflow_name: Name of workflow that created connection
        automa_workflow_id: MongoDB workflow ID
        account_id: Account ID
        connection_status: Status ('active', 'broken', 'pending')

    Returns:
        True if successful, False otherwise
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # 1. Update LINKS table
                cursor.execute("""
                    UPDATE links SET
                        connected_content_id = %s,
                        connected_via_workflow = %s,
                        content_connection_time = CURRENT_TIMESTAMP,
                        connection_status = %s
                    WHERE links_id = %s
                """, (content_id, workflow_name, connection_status, link_id))

                links_updated = cursor.rowcount

                # 2. Update CONTENT table
                cursor.execute("""
                    UPDATE content SET
                        connected_link_id = %s,
                        connected_via_workflow = %s,
                        link_connection_time = CURRENT_TIMESTAMP,
                        link_connection_status = %s
                    WHERE content_id = %s
                """, (link_id, workflow_name, connection_status, content_id))

                content_updated = cursor.rowcount

                # 3. Insert into link_content_connections table
                cursor.execute("""
                    INSERT INTO link_content_connections (
                        link_id,
                        content_id,
                        workflow_name,
                        automa_workflow_id,
                        account_id,
                        connection_type,
                        status,
                        connected_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT DO NOTHING
                """, (
                    link_id,
                    content_id,
                    workflow_name,
                    automa_workflow_id,
                    account_id,
                    'workflow_based',
                    connection_status
                ))

                conn.commit()

                logger.info(
                    f"✓ Synced link-content connection: "
                    f"link_id={link_id} ↔ content_id={content_id} "
                    f"via workflow '{workflow_name}' "
                    f"(links_updated={links_updated}, content_updated={content_updated})"
                )

                return True

    except Exception as e:
        logger.error(f"Error syncing link-content connection: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def sync_all_workflow_connections() -> Dict[str, int]:
    """
    Sync ALL link-content connections based on shared automa_workflow_id.
    This finds content and links that share the same workflow but aren't connected.

    Returns:
        Dictionary with sync statistics
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Find content and links that share workflows but aren't connected
                cursor.execute("""
                    SELECT
                        c.content_id,
                        c.automa_workflow_id,
                        c.workflow_name,
                        c.account_id as content_account_id,
                        l.links_id,
                        l.account_id as link_account_id
                    FROM content c
                    INNER JOIN links l ON c.automa_workflow_id = l.mongo_workflow_id
                    WHERE c.automa_workflow_id IS NOT NULL
                      AND l.mongo_workflow_id IS NOT NULL
                      AND c.connected_link_id IS NULL
                      AND l.connected_content_id IS NULL
                """)

                matches = cursor.fetchall()

                if not matches:
                    logger.info("No unconnected workflow matches found")
                    return {'total_found': 0, 'synced': 0, 'failed': 0}

                logger.info(f"Found {len(matches)} workflow matches to sync")

                synced_count = 0
                failed_count = 0

                for match in matches:
                    try:
                        # Update both tables
                        cursor.execute("""
                            UPDATE links SET
                                connected_content_id = %s,
                                connected_via_workflow = %s,
                                content_connection_time = CURRENT_TIMESTAMP,
                                connection_status = 'active'
                            WHERE links_id = %s
                        """, (
                            match['content_id'],
                            match['workflow_name'],
                            match['links_id']
                        ))

                        cursor.execute("""
                            UPDATE content SET
                                connected_link_id = %s,
                                connected_via_workflow = %s,
                                link_connection_time = CURRENT_TIMESTAMP,
                                link_connection_status = 'active'
                            WHERE content_id = %s
                        """, (
                            match['links_id'],
                            match['workflow_name'],
                            match['content_id']
                        ))

                        # Insert connection record
                        cursor.execute("""
                            INSERT INTO link_content_connections (
                                link_id,
                                content_id,
                                workflow_name,
                                automa_workflow_id,
                                account_id,
                                connection_type,
                                status
                            ) VALUES (%s, %s, %s, %s, %s, 'workflow_based', 'active')
                            ON CONFLICT DO NOTHING
                        """, (
                            match['links_id'],
                            match['content_id'],
                            match['workflow_name'],
                            match['automa_workflow_id'],
                            match['content_account_id']
                        ))

                        synced_count += 1
                        logger.debug(
                            f"Synced: link_id={match['links_id']} ↔ "
                            f"content_id={match['content_id']} "
                            f"via workflow {match['workflow_name']}"
                        )

                    except Exception as e:
                        failed_count += 1
                        logger.error(
                            f"Failed to sync link_id={match['links_id']} "
                            f"and content_id={match['content_id']}: {e}"
                        )

                conn.commit()

                logger.info(
                    f"Sync complete: {synced_count} synced, "
                    f"{failed_count} failed out of {len(matches)} total"
                )

                return {
                    'total_found': len(matches),
                    'synced': synced_count,
                    'failed': failed_count
                }

    except Exception as e:
        logger.error(f"Error in sync_all_workflow_connections: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {'total_found': 0, 'synced': 0, 'failed': 0, 'error': str(e)}


def verify_link_content_connections() -> Dict[str, Any]:
    """
    Verify the integrity of link-content connections.
    Returns statistics about connection status.
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}

                # Links with content connections
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM links
                    WHERE connected_content_id IS NOT NULL
                """)
                stats['links_with_content'] = cursor.fetchone()['count']

                # Content with link connections
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM content
                    WHERE connected_link_id IS NOT NULL
                """)
                stats['content_with_links'] = cursor.fetchone()['count']

                # Verify bidirectional integrity
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM links l
                    INNER JOIN content c ON l.connected_content_id = c.content_id
                    WHERE c.connected_link_id = l.links_id
                """)
                stats['bidirectional_valid'] = cursor.fetchone()['count']

                # Broken links (link points to content but content doesn't point back)
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM links l
                    LEFT JOIN content c ON l.connected_content_id = c.content_id
                    WHERE l.connected_content_id IS NOT NULL
                      AND (c.content_id IS NULL OR c.connected_link_id != l.links_id)
                """)
                stats['broken_from_links'] = cursor.fetchone()['count']

                # Broken content (content points to link but link doesn't point back)
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM content c
                    LEFT JOIN links l ON c.connected_link_id = l.links_id
                    WHERE c.connected_link_id IS NOT NULL
                      AND (l.links_id IS NULL OR l.connected_content_id != c.content_id)
                """)
                stats['broken_from_content'] = cursor.fetchone()['count']

                # Connection records
                cursor.execute("SELECT COUNT(*) as count FROM link_content_connections")
                stats['connection_records'] = cursor.fetchone()['count']

                # Active connections
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM link_content_connections
                    WHERE status = 'active'
                """)
                stats['active_connections'] = cursor.fetchone()['count']

                logger.info(f"Connection verification: {stats}")
                return stats

    except Exception as e:
        logger.error(f"Error verifying connections: {e}")
        return {'error': str(e)}


def get_content_link_details(content_id: int) -> Optional[Dict[str, Any]]:
    """Get detailed link information for a specific content item."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        c.content_id,
                        c.content_name,
                        c.connected_link_id,
                        c.connected_via_workflow,
                        c.link_connection_time,
                        c.link_connection_status,
                        l.link as link_url,
                        l.tweet_id,
                        l.tweeted_time,
                        l.used as link_used,
                        la.username as link_account_username,
                        lcc.connection_type,
                        lcc.connected_at as connection_record_time
                    FROM content c
                    LEFT JOIN links l ON c.connected_link_id = l.links_id
                    LEFT JOIN accounts la ON l.account_id = la.account_id
                    LEFT JOIN link_content_connections lcc ON
                        lcc.content_id = c.content_id AND lcc.link_id = l.links_id
                    WHERE c.content_id = %s
                """, (content_id,))

                result = cursor.fetchone()
                return dict(result) if result else None

    except Exception as e:
        logger.error(f"Error getting content link details: {e}")
        return None


def repair_broken_connections() -> Dict[str, int]:
    """
    Repair broken bidirectional connections.
    Ensures if link points to content, content points back to link.
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                repaired = 0

                # Repair: link has content_id but content doesn't have link_id
                cursor.execute("""
                    UPDATE content c
                    SET connected_link_id = l.links_id,
                        connected_via_workflow = l.connected_via_workflow,
                        link_connection_time = l.content_connection_time,
                        link_connection_status = 'active'
                    FROM links l
                    WHERE l.connected_content_id = c.content_id
                      AND c.connected_link_id IS NULL
                """)
                repaired += cursor.rowcount

                # Repair: content has link_id but link doesn't have content_id
                cursor.execute("""
                    UPDATE links l
                    SET connected_content_id = c.content_id,
                        connected_via_workflow = c.connected_via_workflow,
                        content_connection_time = c.link_connection_time,
                        connection_status = 'active'
                    FROM content c
                    WHERE c.connected_link_id = l.links_id
                      AND l.connected_content_id IS NULL
                """)
                repaired += cursor.rowcount

                conn.commit()

                logger.info(f"Repaired {repaired} broken connections")
                return {'repaired': repaired}

    except Exception as e:
        logger.error(f"Error repairing connections: {e}")
        return {'repaired': 0, 'error': str(e)}
