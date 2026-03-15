import logging
from typing import List, Dict, Any
from psycopg2.extras import RealDictCursor
from datetime import datetime
from .connection import get_postgres_connection
import streamlit as st

logger = logging.getLogger(__name__)

def log_workflow_generation(workflow_name: str, links_id: str = None, content_id: int = None):
    """Logs a workflow generation event to the PostgreSQL workflow_generation_log table."""
    with get_postgres_connection() as conn:
        if not conn:
            logger.error("Failed to connect to PostgreSQL database.")
            st.error("❌ Failed to connect to PostgreSQL database.")
            raise Exception("Failed to connect to PostgreSQL database")
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''INSERT INTO workflow_generation_log (workflow_name, generated_time, links_id, content_id)
                       VALUES (%s, %s, %s, %s)''',
                    (workflow_name, datetime.now(), links_id, content_id)
                )
                conn.commit()
                logger.info(f"✅ Workflow generation logged for {workflow_name}, content_id: {content_id}, links_id: {links_id}")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error logging workflow generation for {workflow_name}: {e}")
            st.error(f"❌ Error logging workflow generation: {str(e)}")
            raise

def get_workflow_generation_logs(workflow_name: str = None, limit: int = None) -> List[Dict[str, Any]]:
    """Fetches workflow generation logs from PostgreSQL with optional filters."""
    with get_postgres_connection() as conn:
        if not conn:
            logger.error("Failed to connect to PostgreSQL database.")
            st.error("❌ Failed to connect to PostgreSQL database.")
            return []
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = "SELECT id, workflow_name, generated_time, links_id, content_id FROM workflow_generation_log"
                conditions = []
                params = []
                if workflow_name:
                    conditions.append("workflow_name = %s")
                    params.append(workflow_name)
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                query += " ORDER BY generated_time DESC"
                if limit is not None:
                    query += f" LIMIT {limit}"
                cursor.execute(query, params)
                logs = cursor.fetchall()
                logger.info(f"Retrieved {len(logs)} workflow generation logs from PostgreSQL.")
                return logs
        except Exception as e:
            logger.error(f"Error fetching workflow generation logs: {e}")
            st.error(f"❌ Error fetching workflow generation logs: {str(e)}")
            return []