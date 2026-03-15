import logging
from typing import List, Dict, Any
from psycopg2.extras import RealDictCursor
from datetime import datetime
from .connection import get_postgres_connection
import streamlit as st

logger = logging.getLogger(__name__)

def log_workflow_sync(user_id: int, workflow_type: str, mongo_workflow_id: str, sync_direction: str, sync_status: str, records_synced: int = 0, error_message: str = None):
    """Logs a workflow sync event to the PostgreSQL workflow_sync_log table."""
    with get_postgres_connection() as conn:
        if not conn:
            logger.error("Failed to connect to PostgreSQL database.")
            st.error("❌ Failed to connect to PostgreSQL database.")
            raise Exception("Failed to connect to PostgreSQL database")
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''INSERT INTO workflow_sync_log (user_id, workflow_type, mongo_workflow_id, sync_direction, sync_status, records_synced, error_message, sync_time)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''',
                    (user_id, workflow_type, mongo_workflow_id, sync_direction, sync_status, records_synced, error_message, datetime.now())
                )
                conn.commit()
                logger.info(f"✅ Workflow sync logged for user {user_id}, type {workflow_type}, mongo_workflow_id: {mongo_workflow_id}, status: {sync_status}")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error logging workflow sync for user {user_id}, type {workflow_type}: {e}")
            st.error(f"❌ Error logging workflow sync: {str(e)}")
            raise

def get_workflow_sync_logs(workflow_type: str = None, sync_status: str = None, limit: int = None) -> List[Dict[str, Any]]:
    """Fetches workflow sync logs from PostgreSQL with optional filters."""
    with get_postgres_connection() as conn:
        if not conn:
            logger.error("Failed to connect to PostgreSQL database.")
            st.error("❌ Failed to connect to PostgreSQL database.")
            return []
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = "SELECT sync_id, user_id, workflow_type, mongo_workflow_id, sync_direction, sync_status, records_synced, error_message, sync_time FROM workflow_sync_log"
                conditions = []
                params = []
                if workflow_type:
                    conditions.append("workflow_type = %s")
                    params.append(workflow_type.lower())
                if sync_status:
                    conditions.append("sync_status = %s")
                    params.append(sync_status.lower())
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                query += " ORDER BY sync_time DESC"
                if limit is not None:
                    query += f" LIMIT {limit}"
                cursor.execute(query, params)
                logs = cursor.fetchall()
                logger.info(f"Retrieved {len(logs)} workflow sync logs from PostgreSQL.")
                return logs
        except Exception as e:
            logger.error(f"Error fetching workflow sync logs: {e}")
            st.error(f"❌ Error fetching workflow sync logs: {str(e)}")
            return []