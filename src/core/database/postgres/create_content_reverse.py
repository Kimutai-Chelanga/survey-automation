"""
PostgreSQL reversal operations for Create Content DAG.
Reverses content creation by DELETING replies, messages, and retweets.
Location: src/core/database/postgres/create_content_reverse.py
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
from .connection import get_postgres_connection

logger = logging.getLogger(__name__)


def get_reversal_preview(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Get a preview of what will be reversed for Create Content workflows.
    Shows replies, messages, and retweets that will be DELETED.

    Args:
        workflow_type: Type of workflow (None = all content types)
        account_id: Filter by account ID
        date_from: Start date for filtering
        date_to: End date for filtering

    Returns:
        Dictionary containing preview information
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                # Build WHERE clause
                params = []

                if account_id:
                    account_condition = "account_id = %s"
                    params.append(account_id)
                else:
                    account_condition = "TRUE"

                date_conditions = []
                if date_from:
                    date_conditions.append("created_time >= %s")
                    params.append(date_from)
                if date_to:
                    date_conditions.append("created_time <= %s")
                    params.append(date_to)

                date_clause = " AND ".join(date_conditions) if date_conditions else "TRUE"

                # Count replies
                cursor.execute(
                    f"SELECT COUNT(*) FROM replies WHERE {account_condition} AND {date_clause}",
                    params
                )
                replies_count = cursor.fetchone()[0]

                # Count messages
                cursor.execute(
                    f"SELECT COUNT(*) FROM messages WHERE {account_condition} AND {date_clause}",
                    params
                )
                messages_count = cursor.fetchone()[0]

                # Count retweets
                cursor.execute(
                    f"SELECT COUNT(*) FROM retweets WHERE {account_condition} AND {date_clause}",
                    params
                )
                retweets_count = cursor.fetchone()[0]

                total_count = replies_count + messages_count + retweets_count

                # Get sample content from each type
                sample_content = []

                # Sample replies
                cursor.execute(
                    f"""SELECT replies_id, content, account_id, created_time, workflow_status
                        FROM replies
                        WHERE {account_condition} AND {date_clause}
                        ORDER BY created_time DESC
                        LIMIT 3""",
                    params
                )
                for row in cursor.fetchall():
                    sample_content.append({
                        'type': 'reply',
                        'id': row[0],
                        'content_preview': row[1][:100] + '...' if len(row[1]) > 100 else row[1],
                        'account_id': row[2],
                        'created_time': row[3].isoformat() if row[3] else None,
                        'workflow_status': row[4]
                    })

                # Sample messages
                cursor.execute(
                    f"""SELECT messages_id, content, account_id, created_time, workflow_status
                        FROM messages
                        WHERE {account_condition} AND {date_clause}
                        ORDER BY created_time DESC
                        LIMIT 3""",
                    params
                )
                for row in cursor.fetchall():
                    sample_content.append({
                        'type': 'message',
                        'id': row[0],
                        'content_preview': row[1][:100] + '...' if len(row[1]) > 100 else row[1],
                        'account_id': row[2],
                        'created_time': row[3].isoformat() if row[3] else None,
                        'workflow_status': row[4]
                    })

                # Sample retweets
                cursor.execute(
                    f"""SELECT retweets_id, content, account_id, created_time, workflow_status
                        FROM retweets
                        WHERE {account_condition} AND {date_clause}
                        ORDER BY created_time DESC
                        LIMIT 3""",
                    params
                )
                for row in cursor.fetchall():
                    sample_content.append({
                        'type': 'retweet',
                        'id': row[0],
                        'content_preview': row[1][:100] + '...' if len(row[1]) > 100 else row[1],
                        'account_id': row[2],
                        'created_time': row[3].isoformat() if row[3] else None,
                        'workflow_status': row[4]
                    })

        return {
            'success': True,
            'total_to_reverse': total_count,
            'breakdown_by_type': {
                'replies': replies_count,
                'messages': messages_count,
                'retweets': retweets_count
            },
            'sample_links': sample_content  # Keep key name for compatibility
        }

    except Exception as e:
        logger.error(f"Error getting reversal preview: {e}")
        return {
            'success': False,
            'error': str(e),
            'total_to_reverse': 0,
            'breakdown_by_type': {},
            'sample_links': []
        }


# =============================================================================
# FIXED: create_content_reverse.py
# =============================================================================
"""
FIXED: PostgreSQL reversal for Create Content DAG
Resets workflow connections in the CONTENT table (not deleting content)
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
from .connection import get_postgres_connection

logger = logging.getLogger(__name__)


def reverse_workflow_execution_postgres(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    dag_run_id: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    ✅ FIXED: Reverse Create Content by resetting workflow connections
    in the CONTENT table (unified storage)
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                # Build WHERE clause
                conditions = []
                params = []

                if workflow_type:
                    conditions.append("content_type = %s")
                    params.append(workflow_type)

                if account_id:
                    conditions.append("account_id = %s")
                    params.append(account_id)

                if date_from:
                    conditions.append("created_time >= %s")
                    params.append(date_from)

                if date_to:
                    conditions.append("created_time <= %s")
                    params.append(date_to)

                # Only reverse content that has workflow connections
                conditions.append("automa_workflow_id IS NOT NULL")

                where_clause = " AND ".join(conditions) if conditions else "TRUE"

                # ✅ Reset workflow connections WITHOUT deleting content
                reset_query = f"""
                    UPDATE content SET
                        automa_workflow_id = NULL,
                        workflow_name = NULL,
                        workflow_status = 'pending',
                        has_content = FALSE,
                        workflow_generated_time = NULL
                    WHERE {where_clause}
                    RETURNING content_id, content_type
                """

                cursor.execute(reset_query, params)
                reset_count = cursor.rowcount

                conn.commit()

                logger.info(f"✅ Reset {reset_count} content workflow connections")

                return {
                    'success': True,
                    'total_reversed': reset_count,
                    'details': {
                        'content_reset': reset_count,
                        'message': f'Reset {reset_count} workflow connections (content preserved)'
                    }
                }

    except Exception as e:
        logger.error(f"Error reversing Create Content: {e}")
        return {
            'success': False,
            'error': str(e),
            'total_reversed': 0
        }
