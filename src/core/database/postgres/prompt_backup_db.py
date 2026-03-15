"""
Prompt Backups Database Functions
Handles database operations for prompt version history and backups
"""

import logging
from typing import List, Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from .connection import get_postgres_connection

logger = logging.getLogger(__name__)


class PromptBackupDB:
    """Database operations for prompt backups"""

    @staticmethod
    def create_backup(
        prompt_id: int,
        account_id: int,
        username: str,
        prompt_name: str,
        prompt_type: str,
        prompt_content: str,
        backup_type: str = 'manual',
        backup_reason: str = None,
        metadata: dict = None
    ) -> Optional[int]:
        """Create a new prompt backup"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Get next version number
                    cursor.execute(
                        "SELECT COALESCE(MAX(version_number), 0) + 1 as next_version "
                        "FROM prompt_backups WHERE prompt_id = %s",
                        (prompt_id,)
                    )
                    result = cursor.fetchone()
                    version_number = result['next_version']

                    # Insert backup
                    cursor.execute("""
                        INSERT INTO prompt_backups (
                            prompt_id, account_id, username, prompt_name,
                            prompt_type, prompt_content, version_number,
                            backup_type, backup_reason, metadata
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING backup_id
                    """, (
                        prompt_id, account_id, username, prompt_name,
                        prompt_type, prompt_content, version_number,
                        backup_type, backup_reason,
                        psycopg2.extras.Json(metadata) if metadata else None
                    ))

                    backup_id = cursor.fetchone()['backup_id']
                    logger.info(f"Created backup {backup_id} for prompt {prompt_id}, version {version_number}")
                    return backup_id

        except Exception as e:
            logger.error(f"Error creating prompt backup: {e}")
            return None

    @staticmethod
    def get_all_backups(
        prompt_id: Optional[int] = None,
        account_id: Optional[int] = None,
        username: Optional[str] = None,
        prompt_type: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get all backups with optional filtering"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT
                            pb.backup_id,
                            pb.prompt_id,
                            pb.account_id,
                            pb.username,
                            pb.prompt_name,
                            pb.prompt_type,
                            pb.version_number,
                            pb.backed_up_at,
                            pb.backup_type,
                            pb.backup_reason,
                            pb.is_restorable,
                            pb.restored,
                            pb.restored_at,
                            LENGTH(pb.prompt_content) as content_length,
                            p.name as current_prompt_name,
                            p.is_active as prompt_is_active
                        FROM prompt_backups pb
                        LEFT JOIN prompts p ON pb.prompt_id = p.prompt_id
                        WHERE 1=1
                    """
                    params = []

                    if prompt_id:
                        query += " AND pb.prompt_id = %s"
                        params.append(prompt_id)

                    if account_id:
                        query += " AND pb.account_id = %s"
                        params.append(account_id)

                    if username:
                        query += " AND pb.username = %s"
                        params.append(username)

                    if prompt_type:
                        query += " AND pb.prompt_type = %s"
                        params.append(prompt_type)

                    query += " ORDER BY pb.backed_up_at DESC LIMIT %s"
                    params.append(limit)

                    cursor.execute(query, params)
                    backups = cursor.fetchall()

                    logger.info(f"Retrieved {len(backups)} backups")
                    return [dict(row) for row in backups]

        except Exception as e:
            logger.error(f"Error retrieving backups: {e}")
            return []

    @staticmethod
    def get_backup_by_id(backup_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific backup by ID including full content"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            pb.*,
                            p.name as current_prompt_name,
                            p.content as current_prompt_content,
                            p.is_active as prompt_is_active
                        FROM prompt_backups pb
                        LEFT JOIN prompts p ON pb.prompt_id = p.prompt_id
                        WHERE pb.backup_id = %s
                    """, (backup_id,))

                    result = cursor.fetchone()
                    return dict(result) if result else None

        except Exception as e:
            logger.error(f"Error retrieving backup {backup_id}: {e}")
            return None

    @staticmethod
    def get_backup_versions(prompt_id: int) -> List[Dict[str, Any]]:
        """Get all versions for a specific prompt"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            backup_id,
                            version_number,
                            backed_up_at,
                            backup_type,
                            backup_reason,
                            is_restorable,
                            restored,
                            LENGTH(prompt_content) as content_length
                        FROM prompt_backups
                        WHERE prompt_id = %s
                        ORDER BY version_number DESC
                    """, (prompt_id,))

                    versions = cursor.fetchall()
                    return [dict(row) for row in versions]

        except Exception as e:
            logger.error(f"Error retrieving versions for prompt {prompt_id}: {e}")
            return []

    @staticmethod
    def restore_backup(backup_id: int, create_new_prompt: bool = False) -> Optional[int]:
        """Restore a backup to either update existing prompt or create new one"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Get backup data
                    cursor.execute(
                        "SELECT * FROM prompt_backups WHERE backup_id = %s AND is_restorable = TRUE",
                        (backup_id,)
                    )
                    backup = cursor.fetchone()

                    if not backup:
                        logger.error(f"Backup {backup_id} not found or not restorable")
                        return None

                    if create_new_prompt:
                        # Create new prompt from backup
                        cursor.execute("""
                            INSERT INTO prompts (
                                account_id, name, content, prompt_type, is_active
                            ) VALUES (%s, %s, %s, %s, %s)
                            RETURNING prompt_id
                        """, (
                            backup['account_id'],
                            f"{backup['prompt_name']} (Restored)",
                            backup['prompt_content'],
                            backup['prompt_type'],
                            True
                        ))
                        new_prompt_id = cursor.fetchone()['prompt_id']

                        # Mark backup as restored
                        cursor.execute("""
                            UPDATE prompt_backups
                            SET restored = TRUE,
                                restored_at = CURRENT_TIMESTAMP,
                                restored_to_prompt_id = %s
                            WHERE backup_id = %s
                        """, (new_prompt_id, backup_id))

                        logger.info(f"Restored backup {backup_id} to new prompt {new_prompt_id}")
                        return new_prompt_id
                    else:
                        # Update existing prompt
                        cursor.execute("""
                            UPDATE prompts
                            SET content = %s,
                                name = %s,
                                updated_time = CURRENT_TIMESTAMP
                            WHERE prompt_id = %s
                        """, (
                            backup['prompt_content'],
                            backup['prompt_name'],
                            backup['prompt_id']
                        ))

                        # Mark backup as restored
                        cursor.execute("""
                            UPDATE prompt_backups
                            SET restored = TRUE,
                                restored_at = CURRENT_TIMESTAMP,
                                restored_to_prompt_id = %s
                            WHERE backup_id = %s
                        """, (backup['prompt_id'], backup_id))

                        logger.info(f"Restored backup {backup_id} to existing prompt {backup['prompt_id']}")
                        return backup['prompt_id']

        except Exception as e:
            logger.error(f"Error restoring backup {backup_id}: {e}")
            return None

    @staticmethod
    def get_backup_statistics() -> Dict[str, Any]:
        """Get overall backup statistics"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    stats = {}

                    # Total backups
                    cursor.execute("SELECT COUNT(*) as count FROM prompt_backups")
                    stats['total_backups'] = cursor.fetchone()['count']

                    # Total prompts with backups
                    cursor.execute("""
                        SELECT COUNT(DISTINCT prompt_id) as count FROM prompt_backups
                    """)
                    stats['prompts_with_backups'] = cursor.fetchone()['count']

                    # Backups by type
                    cursor.execute("""
                        SELECT backup_type, COUNT(*) as count
                        FROM prompt_backups
                        GROUP BY backup_type
                    """)
                    stats['by_type'] = {row['backup_type']: row['count']
                                       for row in cursor.fetchall()}

                    # Backups by prompt type
                    cursor.execute("""
                        SELECT prompt_type, COUNT(*) as count
                        FROM prompt_backups
                        GROUP BY prompt_type
                        ORDER BY count DESC
                        LIMIT 10
                    """)
                    stats['by_prompt_type'] = {row['prompt_type']: row['count']
                                              for row in cursor.fetchall()}

                    # Recent backup activity
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM prompt_backups
                        WHERE backed_up_at >= NOW() - INTERVAL '7 days'
                    """)
                    stats['last_7_days'] = cursor.fetchone()['count']

                    # Restored backups
                    cursor.execute("""
                        SELECT COUNT(*) as count FROM prompt_backups WHERE restored = TRUE
                    """)
                    stats['restored_count'] = cursor.fetchone()['count']

                    return stats

        except Exception as e:
            logger.error(f"Error getting backup statistics: {e}")
            return {}

    @staticmethod
    def delete_old_backups(prompt_id: int, keep_versions: int = 10) -> int:
        """Delete old backups, keeping only the most recent versions"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        DELETE FROM prompt_backups
                        WHERE backup_id IN (
                            SELECT backup_id
                            FROM prompt_backups
                            WHERE prompt_id = %s
                            ORDER BY version_number DESC
                            OFFSET %s
                        )
                    """, (prompt_id, keep_versions))

                    deleted_count = cursor.rowcount
                    logger.info(f"Deleted {deleted_count} old backups for prompt {prompt_id}")
                    return deleted_count

        except Exception as e:
            logger.error(f"Error deleting old backups: {e}")
            return 0
