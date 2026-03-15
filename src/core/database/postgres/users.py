import logging
from typing import Dict, Any
from psycopg2.extras import RealDictCursor
from .connection import get_postgres_connection

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

logger = logging.getLogger(__name__)

def get_user_stats() -> Dict[str, Any]:
    """Fetches user statistics from the PostgreSQL user_workflow_summary view."""
    try:
        # Use 'with' statement for the connection
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        COUNT(*) as total_users,
                        COUNT(CASE WHEN active_replies_workflow IS NOT NULL THEN 1 END) as active_replies_workflows,
                        COUNT(CASE WHEN active_messages_workflow IS NOT NULL THEN 1 END) as active_messages_workflows,
                        COUNT(CASE WHEN active_retweets_workflow IS NOT NULL THEN 1 END) as active_retweets_workflows
                    FROM user_workflow_summary
                """)
                stats = cursor.fetchone()
                logger.info("Successfully retrieved user statistics from PostgreSQL.")
                return stats or {}
    except Exception as e:
        logger.error(f"Error fetching user stats: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching user stats: {str(e)}")
        return {}