"""
Updated Prompts Database Functions with Integrated Backup Support
"""

import logging
from typing import List, Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from .connection import get_postgres_connection

logger = logging.getLogger(__name__)

# Import backup manager
from .prompt_backup_db import PromptBackupDB

backup_db = PromptBackupDB()


def create_prompt(account_id: int, name: str, content: str, prompt_type: str, is_active: bool = True) -> Optional[int]:
    """
    Creates a new prompt in the PostgreSQL prompts table.

    FIX: The initial backup is now created in a SEPARATE connection after the
    prompt INSERT has been committed. Previously the backup INSERT ran inside
    the same transaction before the COMMIT, causing:
        "insert or update on table 'prompt_backups' violates foreign key
         constraint 'prompt_backups_prompt_id_fkey'
         DETAIL: Key (prompt_id)=(N) is not present in table 'prompts'."
    """
    logger.info(f"create_prompt called with: account_id={account_id}, name='{name}', prompt_type='{prompt_type}'")

    # ── STEP 1: Insert the prompt and commit ─────────────────────────────────
    prompt_id  = None
    username   = None
    created_at = None

    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:

                # Verify account exists
                cursor.execute(
                    "SELECT account_id, username FROM accounts WHERE account_id = %s",
                    (account_id,)
                )
                account_result = cursor.fetchone()

                if not account_result:
                    logger.error(f"Account with ID {account_id} does not exist")
                    return None

                username = account_result['username']
                logger.info(f"✓ Account verified: {username}")

                insert_query = '''
                    INSERT INTO prompts (account_id, name, content, prompt_type, is_active)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING prompt_id, name, prompt_type, created_time
                '''
                cursor.execute(insert_query, (account_id, name, content, prompt_type, is_active))
                result = cursor.fetchone()

                if result is None:
                    logger.error("INSERT query returned None")
                    conn.rollback()
                    return None

                prompt_id  = result['prompt_id']
                created_at = result['created_time']
                logger.info(f"✓ Prompt created with ID: {prompt_id}")

        # Connection closed here → INSERT is committed at this point.

    except Exception as e:
        logger.error(f"Error creating prompt: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

    # ── STEP 2: Create initial backup in a NEW connection ────────────────────
    # The prompt row now exists in the DB, so the FK constraint is satisfied.
    try:
        backup_id = backup_db.create_backup(
            prompt_id=prompt_id,
            account_id=account_id,
            username=username,
            prompt_name=name,
            prompt_type=prompt_type,
            prompt_content=content,
            backup_type='auto',
            backup_reason='Initial creation backup',
            metadata={
                'is_active': is_active,
                'created_time': created_at.isoformat() if created_at else None
            }
        )

        if backup_id:
            logger.info(f"✓ Initial backup created with ID: {backup_id}")
        else:
            # Non-fatal — log and continue; the prompt was created successfully.
            logger.warning("Initial backup creation failed (prompt was still created)")

    except Exception as e:
        # Non-fatal — the prompt exists; we just couldn't back it up right now.
        logger.warning(f"Initial backup failed (non-fatal): {e}")

    return prompt_id


def get_all_prompt_types() -> List[str]:
    """Get all unique prompt types currently in the database."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT DISTINCT prompt_type
                    FROM prompts
                    WHERE prompt_type IS NOT NULL
                    ORDER BY prompt_type
                """)
                types = cursor.fetchall()
                prompt_types = [row['prompt_type'] for row in types]
                logger.info(f"Retrieved {len(prompt_types)} unique prompt types")
                return prompt_types
    except Exception as e:
        logger.error(f"Error fetching prompt types: {e}")
        return []


def validate_prompt_type_length(prompt_type: str) -> bool:
    """Validate that prompt type doesn't exceed database field length."""
    MAX_LENGTH = 50
    if len(prompt_type) > MAX_LENGTH:
        logger.warning(f"Prompt type '{prompt_type}' exceeds maximum length of {MAX_LENGTH}")
        return False
    return True


def get_prompts_stats_cached() -> Dict[str, Any]:
    """Get statistics about prompts from the database."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}

                cursor.execute("SELECT COUNT(*) AS count FROM prompts")
                result = cursor.fetchone()
                stats['total_prompts'] = result['count'] if result else 0

                cursor.execute("SELECT COUNT(*) AS count FROM prompts WHERE is_active = TRUE")
                result = cursor.fetchone()
                stats['active_prompts'] = result['count'] if result else 0

                cursor.execute("SELECT COUNT(DISTINCT prompt_type) AS count FROM prompts")
                result = cursor.fetchone()
                stats['prompt_types'] = result['count'] if result else 0

                cursor.execute("SELECT COUNT(DISTINCT account_id) AS count FROM prompts")
                result = cursor.fetchone()
                stats['accounts_with_prompts'] = result['count'] if result else 0

                cursor.execute("""
                    SELECT prompt_type, COUNT(*) as count
                    FROM prompts
                    GROUP BY prompt_type
                """)
                type_counts = cursor.fetchall()
                stats['type_counts'] = {row['prompt_type']: row['count'] for row in type_counts}

                logger.info("Successfully retrieved prompt statistics")
                return stats
    except Exception as e:
        logger.error(f"Error fetching prompt stats: {e}")
        return {
            'total_prompts': 0,
            'active_prompts': 0,
            'prompt_types': 0,
            'accounts_with_prompts': 0,
            'type_counts': {}
        }


def get_comprehensive_prompts(
    limit: int = None,
    active: Optional[bool] = None,
    prompt_type: Optional[str] = None,
    account_id: Optional[int] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
) -> List[Dict[str, Any]]:
    """Fetches all prompts with comprehensive filtering."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT
                        p.prompt_id,
                        p.account_id,
                        COALESCE(a.username, 'Unknown') as username,
                        p.name as prompt_name,
                        p.content,
                        p.prompt_type,
                        p.created_time,
                        p.updated_time,
                        p.is_active,
                        p.mongo_object_id,
                        COUNT(DISTINCT c.content_id) as content_count,
                        COUNT(DISTINCT pb.backup_id) as backup_count,
                        COUNT(DISTINCT pv.variation_id) as variation_count
                    FROM prompts p
                    LEFT JOIN accounts a ON p.account_id = a.account_id
                    LEFT JOIN content c ON p.prompt_id = c.prompt_id
                    LEFT JOIN prompt_backups pb ON p.prompt_id = pb.prompt_id
                    LEFT JOIN prompt_variations pv ON p.prompt_id = pv.parent_prompt_id AND pv.is_active = TRUE
                    WHERE 1=1
                """
                params = []
                group_by = """ GROUP BY p.prompt_id, p.account_id, p.name, p.content, p.prompt_type,
                              p.created_time, p.updated_time, p.is_active, p.mongo_object_id, a.username"""

                if active is not None:
                    query += " AND p.is_active = %s"
                    params.append(active)

                if prompt_type:
                    query += " AND p.prompt_type = %s"
                    params.append(prompt_type)

                if account_id:
                    query += " AND p.account_id = %s"
                    params.append(account_id)

                if start_time:
                    query += " AND p.created_time >= %s"
                    params.append(start_time)

                if end_time:
                    query += " AND p.created_time <= %s"
                    params.append(end_time)

                query += group_by + " ORDER BY p.updated_time DESC"

                if limit is not None:
                    query += " LIMIT %s"
                    params.append(limit)

                cursor.execute(query, params)
                prompts = cursor.fetchall()

                logger.info(f"Retrieved {len(prompts)} comprehensive prompt records")
                return [dict(row) for row in prompts]
    except Exception as e:
        logger.error(f"Error fetching comprehensive prompts: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []


def update_prompt(prompt_id: int, name: str = None, content: str = None,
                 prompt_type: str = None, is_active: bool = None) -> bool:
    """Updates a prompt in the PostgreSQL prompts table. Backup is automatic via trigger."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                updates = []
                params = []

                if name is not None:
                    updates.append("name = %s")
                    params.append(name)

                if content is not None:
                    updates.append("content = %s")
                    params.append(content)

                if prompt_type is not None:
                    updates.append("prompt_type = %s")
                    params.append(prompt_type)

                if is_active is not None:
                    updates.append("is_active = %s")
                    params.append(is_active)

                if not updates:
                    return False

                updates.append("updated_time = NOW()")
                query = f"UPDATE prompts SET {', '.join(updates)} WHERE prompt_id = %s"
                params.append(prompt_id)

                cursor.execute(query, params)

                if cursor.rowcount == 0:
                    logger.warning(f"No prompt found with ID {prompt_id}")
                    return False

                logger.info(f"Prompt {prompt_id} updated successfully (backup created automatically)")
                return True
    except Exception as e:
        logger.error(f"Error updating prompt {prompt_id}: {e}")
        return False


def delete_prompt(prompt_id: int) -> bool:
    """Deletes a prompt from the PostgreSQL prompts table. Backup is automatic via trigger."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('DELETE FROM prompts WHERE prompt_id = %s', (prompt_id,))

                if cursor.rowcount == 0:
                    logger.warning(f"No prompt found with ID {prompt_id}")
                    return False

                logger.info(f"Prompt {prompt_id} deleted successfully (backup created automatically)")
                return True
    except Exception as e:
        logger.error(f"Error deleting prompt {prompt_id}: {e}")
        return False


def get_prompts_by_account(account_id: int, active_only: bool = True) -> List[Dict[str, Any]]:
    """Gets all prompts for a specific account."""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT
                        p.prompt_id,
                        p.name as prompt_name,
                        p.content,
                        p.prompt_type,
                        p.created_time,
                        p.updated_time,
                        p.is_active,
                        COUNT(DISTINCT c.content_id) as content_count,
                        COUNT(DISTINCT pb.backup_id) as backup_count,
                        COUNT(DISTINCT pv.variation_id) as variation_count
                    FROM prompts p
                    LEFT JOIN content c ON p.prompt_id = c.prompt_id
                    LEFT JOIN prompt_backups pb ON p.prompt_id = pb.prompt_id
                    LEFT JOIN prompt_variations pv ON p.prompt_id = pv.parent_prompt_id AND pv.is_active = TRUE
                    WHERE p.account_id = %s
                """
                params = [account_id]

                if active_only:
                    query += " AND p.is_active = TRUE"

                query += " GROUP BY p.prompt_id ORDER BY p.updated_time DESC"

                cursor.execute(query, params)
                prompts = cursor.fetchall()
                logger.info(f"Retrieved {len(prompts)} prompts for account {account_id}")
                return [dict(row) for row in prompts]
    except Exception as e:
        logger.error(f"Error fetching prompts for account {account_id}: {e}")
        return []
