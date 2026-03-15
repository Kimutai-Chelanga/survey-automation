"""
Prompt Variations Database Functions
Handles database operations for AI-generated prompt variations
"""

import logging
from typing import List, Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import uuid
from .connection import get_postgres_connection

logger = logging.getLogger(__name__)


class PromptVariationsDB:
    """Database operations for prompt variations"""

    @staticmethod
    def create_variation_batch(
        parent_prompt_id: int,
        account_id: int,
        username: str,
        prompt_type: str,
        prompt_name: str,
        variations: List[str],
        metadata: Optional[dict] = None
    ) -> Optional[str]:
        """
        Create a batch of prompt variations

        Args:
            parent_prompt_id: ID of the original prompt
            account_id: Account ID
            username: Username
            prompt_type: Type of prompt
            prompt_name: Name of prompt
            variations: List of variation text strings
            metadata: Optional metadata dict

        Returns:
            generation_batch_id if successful, None otherwise
        """
        try:
            batch_id = str(uuid.uuid4())

            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Insert all variations
                    for idx, variation_text in enumerate(variations, start=1):
                        cursor.execute("""
                            INSERT INTO prompt_variations (
                                parent_prompt_id, account_id, username,
                                prompt_type, prompt_name, variation_content,
                                variation_number, generation_batch_id, metadata
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            parent_prompt_id, account_id, username,
                            prompt_type, prompt_name, variation_text,
                            idx, batch_id,
                            psycopg2.extras.Json(metadata) if metadata else None
                        ))

                    logger.info(f"Created {len(variations)} variations for prompt {parent_prompt_id}, batch {batch_id}")
                    return batch_id

        except Exception as e:
            logger.error(f"Error creating variation batch: {e}")
            return None

    @staticmethod
    def get_variations(
        parent_prompt_id: Optional[int] = None,
        account_id: Optional[int] = None,
        username: Optional[str] = None,
        prompt_type: Optional[str] = None,
        batch_id: Optional[str] = None,
        unused_only: bool = False,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get variations with optional filtering"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT
                            pv.variation_id,
                            pv.parent_prompt_id,
                            pv.account_id,
                            pv.username,
                            pv.prompt_type,
                            pv.prompt_name,
                            pv.variation_content,
                            pv.variation_number,
                            pv.generation_batch_id,
                            pv.created_at,
                            pv.used,
                            pv.used_at,
                            pv.copied_count,
                            pv.last_copied_at,
                            pv.quality_score,
                            p.name as parent_prompt_name,
                            p.is_active as parent_is_active
                        FROM prompt_variations pv
                        LEFT JOIN prompts p ON pv.parent_prompt_id = p.prompt_id
                        WHERE pv.is_active = TRUE
                    """
                    params = []

                    if parent_prompt_id:
                        query += " AND pv.parent_prompt_id = %s"
                        params.append(parent_prompt_id)

                    if account_id:
                        query += " AND pv.account_id = %s"
                        params.append(account_id)

                    if username:
                        query += " AND pv.username = %s"
                        params.append(username)

                    if prompt_type:
                        query += " AND pv.prompt_type = %s"
                        params.append(prompt_type)

                    if batch_id:
                        query += " AND pv.generation_batch_id = %s"
                        params.append(batch_id)

                    if unused_only:
                        query += " AND pv.used = FALSE"

                    query += " ORDER BY pv.created_at DESC, pv.variation_number LIMIT %s"
                    params.append(limit)

                    cursor.execute(query, params)
                    variations = cursor.fetchall()

                    logger.info(f"Retrieved {len(variations)} variations")
                    return [dict(row) for row in variations]

        except Exception as e:
            logger.error(f"Error retrieving variations: {e}")
            return []

    @staticmethod
    def get_variation_by_id(variation_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific variation by ID"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            pv.*,
                            p.name as parent_prompt_name,
                            p.content as parent_prompt_content,
                            p.is_active as parent_is_active
                        FROM prompt_variations pv
                        LEFT JOIN prompts p ON pv.parent_prompt_id = p.prompt_id
                        WHERE pv.variation_id = %s
                    """, (variation_id,))

                    result = cursor.fetchone()
                    return dict(result) if result else None

        except Exception as e:
            logger.error(f"Error retrieving variation {variation_id}: {e}")
            return None

    @staticmethod
    def mark_variation_used(variation_id: int) -> bool:
        """Mark a variation as used"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE prompt_variations
                        SET used = TRUE,
                            used_at = CURRENT_TIMESTAMP
                        WHERE variation_id = %s
                    """, (variation_id,))

                    success = cursor.rowcount > 0
                    if success:
                        logger.info(f"Marked variation {variation_id} as used")
                    return success

        except Exception as e:
            logger.error(f"Error marking variation {variation_id} as used: {e}")
            return False

    @staticmethod
    def increment_copy_count(variation_id: int) -> bool:
        """Increment the copy count for a variation"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE prompt_variations
                        SET copied_count = copied_count + 1,
                            last_copied_at = CURRENT_TIMESTAMP
                        WHERE variation_id = %s
                    """, (variation_id,))

                    success = cursor.rowcount > 0
                    if success:
                        logger.info(f"Incremented copy count for variation {variation_id}")
                    return success

        except Exception as e:
            logger.error(f"Error incrementing copy count for variation {variation_id}: {e}")
            return False

    @staticmethod
    def get_variation_statistics(
        account_id: Optional[int] = None,
        prompt_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get variation statistics"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    stats = {}

                    base_where = "WHERE is_active = TRUE"
                    params = []

                    if account_id:
                        base_where += " AND account_id = %s"
                        params.append(account_id)

                    if prompt_type:
                        base_where += " AND prompt_type = %s"
                        params.append(prompt_type)

                    # Total variations
                    cursor.execute(f"SELECT COUNT(*) as count FROM prompt_variations {base_where}", params)
                    stats['total_variations'] = cursor.fetchone()['count']

                    # Unused variations
                    cursor.execute(
                        f"SELECT COUNT(*) as count FROM prompt_variations {base_where} AND used = FALSE",
                        params
                    )
                    stats['unused_variations'] = cursor.fetchone()['count']

                    # Used variations
                    cursor.execute(
                        f"SELECT COUNT(*) as count FROM prompt_variations {base_where} AND used = TRUE",
                        params
                    )
                    stats['used_variations'] = cursor.fetchone()['count']

                    # Total copies
                    cursor.execute(
                        f"SELECT SUM(copied_count) as total FROM prompt_variations {base_where}",
                        params
                    )
                    result = cursor.fetchone()
                    stats['total_copies'] = int(result['total']) if result['total'] else 0

                    # Variations by prompt type
                    cursor.execute(f"""
                        SELECT prompt_type, COUNT(*) as count
                        FROM prompt_variations
                        {base_where}
                        GROUP BY prompt_type
                        ORDER BY count DESC
                    """, params)
                    stats['by_type'] = {row['prompt_type']: row['count']
                                       for row in cursor.fetchall()}

                    # Recent generation activity
                    where_with_date = f"{base_where} AND created_at >= NOW() - INTERVAL '7 days'"
                    cursor.execute(
                        f"SELECT COUNT(*) as count FROM prompt_variations {where_with_date}",
                        params
                    )
                    stats['last_7_days'] = cursor.fetchone()['count']

                    # Average variations per prompt
                    cursor.execute(f"""
                        SELECT AVG(variation_count) as avg_count
                        FROM (
                            SELECT parent_prompt_id, COUNT(*) as variation_count
                            FROM prompt_variations
                            {base_where}
                            GROUP BY parent_prompt_id
                        ) as counts
                    """, params)
                    result = cursor.fetchone()
                    stats['avg_per_prompt'] = float(result['avg_count']) if result['avg_count'] else 0

                    return stats

        except Exception as e:
            logger.error(f"Error getting variation statistics: {e}")
            return {}

    @staticmethod
    def get_batch_info(batch_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a generation batch"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            generation_batch_id,
                            parent_prompt_id,
                            account_id,
                            username,
                            prompt_type,
                            prompt_name,
                            COUNT(*) as variation_count,
                            MIN(created_at) as created_at,
                            COUNT(CASE WHEN used = TRUE THEN 1 END) as used_count,
                            SUM(copied_count) as total_copies
                        FROM prompt_variations
                        WHERE generation_batch_id = %s
                        GROUP BY generation_batch_id, parent_prompt_id, account_id,
                                username, prompt_type, prompt_name
                    """, (batch_id,))

                    result = cursor.fetchone()
                    return dict(result) if result else None

        except Exception as e:
            logger.error(f"Error getting batch info for {batch_id}: {e}")
            return None

    @staticmethod
    def get_all_batches(
        account_id: Optional[int] = None,
        prompt_type: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get all generation batches"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT
                            generation_batch_id,
                            parent_prompt_id,
                            account_id,
                            username,
                            prompt_type,
                            prompt_name,
                            COUNT(*) as variation_count,
                            MIN(created_at) as created_at,
                            COUNT(CASE WHEN used = TRUE THEN 1 END) as used_count,
                            COUNT(CASE WHEN used = FALSE THEN 1 END) as unused_count,
                            SUM(copied_count) as total_copies
                        FROM prompt_variations
                        WHERE is_active = TRUE
                    """
                    params = []

                    if account_id:
                        query += " AND account_id = %s"
                        params.append(account_id)

                    if prompt_type:
                        query += " AND prompt_type = %s"
                        params.append(prompt_type)

                    query += """
                        GROUP BY generation_batch_id, parent_prompt_id, account_id,
                                username, prompt_type, prompt_name
                        ORDER BY MIN(created_at) DESC
                        LIMIT %s
                    """
                    params.append(limit)

                    cursor.execute(query, params)
                    batches = cursor.fetchall()

                    return [dict(row) for row in batches]

        except Exception as e:
            logger.error(f"Error getting all batches: {e}")
            return []

    @staticmethod
    def delete_batch(batch_id: str) -> bool:
        """Delete all variations in a batch (soft delete)"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE prompt_variations
                        SET is_active = FALSE
                        WHERE generation_batch_id = %s
                    """, (batch_id,))

                    deleted_count = cursor.rowcount
                    logger.info(f"Soft deleted {deleted_count} variations from batch {batch_id}")
                    return deleted_count > 0

        except Exception as e:
            logger.error(f"Error deleting batch {batch_id}: {e}")
            return False

    @staticmethod
    def delete_old_used_variations(days: int = 30) -> int:
        """Delete old used variations (soft delete)"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE prompt_variations
                        SET is_active = FALSE
                        WHERE used = TRUE
                          AND used_at < NOW() - INTERVAL '%s days'
                          AND is_active = TRUE
                    """, (days,))

                    deleted_count = cursor.rowcount
                    logger.info(f"Soft deleted {deleted_count} old used variations")
                    return deleted_count

        except Exception as e:
            logger.error(f"Error deleting old variations: {e}")
            return 0
