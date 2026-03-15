import logging
from pymongo import MongoClient
from core.database.postgres.connection import get_postgres_connection
from .config import MONGODB_URI

logger = logging.getLogger(__name__)


def get_mongo_db():
    """
    Get MongoDB database connection

    Returns:
        tuple: (db, client) - Database object and client connection
    """
    try:
        logger.info(f"Connecting to MongoDB with URI: {MONGODB_URI.replace('app_password', '***')}")
        client = MongoClient(MONGODB_URI)
        db = client.get_database()
        logger.info("Successfully connected to MongoDB")
        return db, client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise


def log_sync_error(workflow_type, automa_workflow_id, sync_type, error_message):
    """
    Log synchronization error to PostgreSQL workflow_sync_log table

    FIXED: Updated to match actual schema columns
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO workflow_sync_log
                    (sync_type, workflow_type, automa_workflow_id, sync_time, sync_status, error_message)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP, 'failed', %s)
                    """,
                    (sync_type, workflow_type, automa_workflow_id, error_message)
                )
                conn.commit()
                logger.info(f"Logged sync error for {workflow_type}: {error_message[:100]}")
    except Exception as e:
        logger.error(f"Failed to log sync error to PostgreSQL: {e}")


def log_successful_sync(workflow_type, automa_workflow_id, records_synced=1):
    """
    Log successful synchronization to PostgreSQL workflow_sync_log table

    FIXED: Updated to match actual schema columns
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO workflow_sync_log
                    (sync_type, workflow_type, automa_workflow_id, sync_time, sync_status, details)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP, 'success', %s)
                    """,
                    ('postgres_to_mongo', workflow_type, automa_workflow_id, {'records_synced': records_synced})
                )
                conn.commit()
                logger.info(f"Logged successful sync for {workflow_type}: {records_synced} records")
    except Exception as e:
        logger.error(f"Failed to log successful sync to PostgreSQL: {e}")


def update_workflow_connection_in_db(workflow_type, content_id, automa_workflow_id, workflow_name):
    """
    Update workflow connection in PostgreSQL content table

    FIXED: Now updates content table instead of type-specific tables
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE content
                    SET automa_workflow_id = %s,
                        workflow_name = %s,
                        has_content = TRUE,
                        workflow_generated_time = CURRENT_TIMESTAMP
                    WHERE content_id = %s AND content_type = %s
                    """,
                    (str(automa_workflow_id), workflow_name, content_id, workflow_type)
                )
                conn.commit()
                logger.info(f"Updated workflow connection for content_id {content_id} (type: {workflow_type})")
                return True
    except Exception as e:
        logger.error(f"Failed to update workflow connection in database: {e}")
        log_sync_error(workflow_type, str(automa_workflow_id), "postgres_to_mongo", f"Failed to update connection: {str(e)}")
        return False


def mark_content_as_used(workflow_type, content_id):
    """
    Mark content as used in PostgreSQL content table

    FIXED: Now updates content table instead of type-specific tables
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE content
                    SET used = TRUE,
                        used_time = CURRENT_TIMESTAMP
                    WHERE content_id = %s AND content_type = %s
                    """,
                    (content_id, workflow_type)
                )
                conn.commit()
                logger.info(f"Marked content_id {content_id} (type: {workflow_type}) as used")
                return True
    except Exception as e:
        logger.error(f"Failed to mark content as used: {e}")
        return False
