from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed
import logging
from .db_utils import get_mongo_db, log_sync_error
from core.database.postgres.connection import get_postgres_connection

# Setup logging within this module
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/automa_workflow.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def log_workflow_generation(workflow_type, name, content_id, automa_workflow_id, account_id, 
                           prompt_id=None, workflow_id=None, username=None, profile_id=None):
    """
    Log workflow generation event to PostgreSQL workflow_generation_log table
    
    FIXED: Now passes workflow_type parameter correctly
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO workflow_generation_log 
                    (workflow_type, workflow_name, content_id, automa_workflow_id, account_id, prompt_id, workflow_id, username, profile_id, generated_time, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'success')
                    """,
                    (workflow_type, name, content_id, str(automa_workflow_id), account_id, prompt_id, workflow_id, username, profile_id)
                )
                conn.commit()
                logger.info(f"Logged workflow generation: {name} for {workflow_type}_id {content_id}")
                return True
    except Exception as e:
        logger.error(f"Failed to log workflow generation: {e}")
        log_sync_error(workflow_type, str(automa_workflow_id), "postgres_to_mongo", 
                      f"Failed to log generation: {str(e)}")
        return False


def update_workflow_connection(workflow_type, content_id, automa_workflow_id, workflow_name, 
                              account_id, prompt_id=None, workflow_id=None):
    """
    Update workflow connection in PostgreSQL content table
    
    FIXED: Now updates the content table instead of non-existent type-specific tables
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                # Update content table with workflow connection
                cursor.execute("""
                    UPDATE content
                    SET automa_workflow_id = %s,
                        workflow_name = %s,
                        workflow_status = 'completed',
                        has_content = TRUE,
                        workflow_generated_time = CURRENT_TIMESTAMP
                    WHERE content_id = %s AND content_type = %s
                """, (str(automa_workflow_id), workflow_name, content_id, workflow_type))
                
                rows_affected = cursor.rowcount
                conn.commit()
                
                if rows_affected > 0:
                    logger.info(f"✅ Updated workflow connection for content_id {content_id} (type: {workflow_type})")
                    return True
                else:
                    logger.error(f"❌ No rows updated for content_id {content_id} (type: {workflow_type})")
                    return False
        
    except Exception as e:
        logger.error(f"Error updating workflow connection: {e}")
        log_sync_error(workflow_type, str(automa_workflow_id), "postgres_to_mongo",
                      f"Failed to update connection: {str(e)}")
        return False


def log_execution_record(workflow_type, content_id, automa_workflow_id, account_id, 
                        execution_start, execution_end, blocks_generated, success=True, 
                        error_message=None):
    """
    Log workflow execution record - simplified for content-only architecture
    """
    try:
        execution_time_ms = int((execution_end - execution_start).total_seconds() * 1000)
        status = "success" if success else "failed"
        
        # Log to workflow_sync_log instead of non-existent workflow_runs table
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO workflow_sync_log 
                    (sync_type, workflow_type, content_id, automa_workflow_id, workflow_name, account_id, sync_time, sync_status, error_message)
                    VALUES ('execution_log', %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s)
                    """,
                    (workflow_type, content_id, str(automa_workflow_id), f"{workflow_type}_{content_id}", account_id, status, error_message)
                )
                conn.commit()
                logger.info(f"Logged execution record: {workflow_type}_{content_id} - {status} ({execution_time_ms}ms)")
                return True
    except Exception as e:
        logger.error(f"Failed to log execution record: {e}")
        return False


def get_workflow_generation_stats(account_id=None, days=1):
    """
    Get workflow generation statistics from content table
    
    FIXED: Now queries the content table instead of non-existent type-specific tables
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT 
                        content_type as type,
                        COUNT(*) as total,
                        SUM(CASE WHEN used THEN 1 ELSE 0 END) as used,
                        SUM(CASE WHEN NOT used THEN 1 ELSE 0 END) as unused,
                        SUM(CASE WHEN has_content THEN 1 ELSE 0 END) as has_content_count,
                        SUM(CASE WHEN workflow_status = 'completed' THEN 1 ELSE 0 END) as completed
                    FROM content
                    WHERE created_time >= NOW() - INTERVAL '%s days'
                """
                params = [days]
                
                if account_id is not None:
                    query += " AND account_id = %s"
                    params.append(account_id)
                
                query += " GROUP BY content_type"
                
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                stats = {}
                for row in results:
                    content_type, total, used, unused, has_content_count, completed = row
                    stats[content_type] = {
                        "total": total or 0,
                        "used": used or 0,
                        "unused": unused or 0,
                        "has_content": has_content_count or 0,
                        "completed": completed or 0,
                        "pending": (total or 0) - (completed or 0)
                    }
                
                return stats
    except Exception as e:
        logger.error(f"Failed to get workflow generation stats: {e}")
        return {}


def get_sync_errors(workflow_type=None, limit=100):
    """
    Get recent sync errors from PostgreSQL workflow_sync_log table
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT sync_id, workflow_type, sync_status, error_message, sync_time FROM workflow_sync_log WHERE sync_status = 'failed'"
                params = []
                
                if workflow_type is not None:
                    query += " AND workflow_type = %s"
                    params.append(workflow_type)
                
                query += " ORDER BY sync_time DESC LIMIT %s"
                params.append(limit)
                
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                errors = []
                for row in results:
                    sync_id, wf_type, status, error_msg, sync_time = row
                    errors.append({
                        "sync_id": sync_id,
                        "workflow_type": wf_type,
                        "status": status,
                        "error_message": error_msg,
                        "sync_time": sync_time
                    })
                
                return errors
    except Exception as e:
        logger.error(f"Failed to get sync errors: {e}")
        return []


# MongoDB-related functions remain the same
def log_workflow_execution_completion(automa_workflow_id, success=True, error_message=None, execution_time_ms=None):
    """Log actual workflow execution completion in MongoDB"""
    db, client = get_mongo_db()
    try:
        with client.start_session() as session:
            update_data = {
                "executed": True,
                "success": success,
                "executed_at": datetime.now().isoformat(),
                "status": "completed" if success else "failed"
            }
            
            if execution_time_ms:
                update_data["actual_execution_time"] = execution_time_ms
            
            if error_message:
                update_data["last_error_message"] = error_message
                update_data["last_error_timestamp"] = datetime.now().isoformat()
                
            result = db.workflow_metadata.update_one(
                {"automa_workflow_id": automa_workflow_id},
                {"$set": update_data},
                session=session
            )
            
            if result.modified_count > 0:
                logger.info(f"Updated workflow {automa_workflow_id} execution status: {'completed' if success else 'failed'}")
            else:
                logger.warning(f"No metadata record found for Automa workflow {automa_workflow_id}")
                
    except Exception as e:
        logger.error(f"Failed to update execution status for workflow {automa_workflow_id}: {e}")
        raise
    finally:
        client.close()


def get_unexecuted_workflows(workflow_type=None, limit=20, account_id=None):
    """Get unexecuted workflows from MongoDB"""
    db, client = get_mongo_db()
    try:
        query = {"executed": False, "status": "generated"}
        if workflow_type:
            query["workflow_type"] = workflow_type
        if account_id:
            query["postgres_account_id"] = account_id
        
        execution_records = list(db.workflow_metadata.find(
            query,
            {
                "automa_workflow_id": 1,
                "workflow_type": 1,
                "postgres_content_id": 1,
                "postgres_account_id": 1,
                "username": 1,
                "profile_id": 1,
                "postgres_prompt_id": 1,
                "postgres_workflow_id": 1,
                "generated_at": 1,
                "processing_priority": 1
            }
        ).sort([("processing_priority", 1), ("generated_at", 1)]).limit(limit))
        
        logger.info(f"Found {len(execution_records)} unexecuted workflows" + 
                   (f" for account {account_id}" if account_id else ""))
        return execution_records
        
    except Exception as e:
        logger.error(f"Failed to get unexecuted workflows: {e}")
        return []
    finally:
        client.close()