import logging
from typing import Dict, Any, Optional
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from .connection import get_postgres_connection

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

logger = logging.getLogger(__name__)

def get_workflow_limit(limit_type: str) -> Optional[Dict[str, Any]]:
    """Fetches the workflow limit for a given limit_type from PostgreSQL."""
    conn = get_postgres_connection()
    if not conn:
        logger.error("Failed to connect to PostgreSQL database.")
        return None

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT limit_type, max_count, current_count, reset_time FROM workflow_limits WHERE limit_type = %s",
                (limit_type,)
            )
            result = cursor.fetchone()
            if result and result['reset_time'] <= datetime.now():
                reset_workflow_counts(limit_type)
                cursor.execute(
                    "SELECT limit_type, max_count, current_count, reset_time FROM workflow_limits WHERE limit_type = %s",
                    (limit_type,)
                )
                result = cursor.fetchone()
            logger.info(f"Retrieved workflow limit for {limit_type}: {result}")
            return result
    except Exception as e:
        logger.error(f"Error fetching workflow limit for {limit_type}: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching workflow limit: {str(e)}")
        return None

def increment_workflow_count(limit_type: str):
    """Increments the current_count for a given limit_type in PostgreSQL."""
    conn = get_postgres_connection()
    if not conn:
        raise Exception("Failed to connect to PostgreSQL database")

    try:
        cursor = conn.cursor()
        cursor.execute(
            '''UPDATE workflow_limits SET current_count = current_count + 1 
               WHERE limit_type = %s''',
            (limit_type,)
        )
        conn.commit()
        logger.info(f"✅ Incremented workflow count for {limit_type}")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error incrementing workflow count for {limit_type}: {e}")
        raise

def reset_workflow_counts(limit_type: str = None):
    """Resets the current_count for a given limit_type or all limits if overdue."""
    conn = get_postgres_connection()
    if not conn:
        raise Exception("Failed to connect to PostgreSQL database")

    try:
        cursor = conn.cursor()
        if limit_type:
            cursor.execute(
                '''UPDATE workflow_limits SET current_count = 0, 
                   reset_time = CASE 
                     WHEN limit_type LIKE '%hourly' THEN NOW() + INTERVAL '1 hour' 
                     ELSE NOW() + INTERVAL '1 day' 
                   END
                   WHERE limit_type = %s AND reset_time <= NOW()''',
                (limit_type,)
            )
        else:
            cursor.execute(
                '''UPDATE workflow_limits SET current_count = 0, 
                   reset_time = CASE 
                     WHEN limit_type LIKE '%hourly' THEN NOW() + INTERVAL '1 hour' 
                     ELSE NOW() + INTERVAL '1 day' 
                   END
                   WHERE reset_time <= NOW()'''
            )
        conn.commit()
        logger.info(f"✅ Reset workflow counts for {limit_type or 'all types'}")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error resetting workflow counts: {e}")
        raise