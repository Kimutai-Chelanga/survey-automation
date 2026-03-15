import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta, timezone
from bson import ObjectId
from pymongo.collection import Collection

from .connection import get_mongo_collection

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

logger = logging.getLogger(__name__)

# =================== HELPER FUNCTIONS ===================

def _convert_objectids(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Convert ObjectId fields to string for API responses."""
    if '_id' in doc:
        doc['_id'] = str(doc['_id'])
    if 'automa_workflow_id' in doc and doc['automa_workflow_id']:
        doc['automa_workflow_id'] = str(doc['automa_workflow_id'])
    if 'content_link_id' in doc and doc['content_link_id']:
        doc['content_link_id'] = str(doc['content_link_id'])
    return doc


def _ensure_collection(name: str) -> Optional[Collection]:
    """Safely get collection with error logging."""
    collection = get_mongo_collection(name)
    if collection is None:
        logger.error(f"Failed to access MongoDB collection: {name}")
    return collection


# =================== CORE EXECUTION FUNCTIONS ===================

def get_filtered_executions_from_mongodb(
    filters: Dict[str, Any] = None,
    limit: int = None,
    workflow_type: str = None
) -> List[Dict[str, Any]]:
    """Fetches filtered workflow executions."""
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return []

    try:
        query = filters or {}
        if workflow_type:
            query["workflow_type"] = workflow_type

        projection = {
            "_id": 1, "automa_workflow_id": 1, "content_link_id": 1,
            "postgres_content_id": 1, "postgres_account_id": 1,
            "workflow_type": 1, "status": 1, "executed": 1, "success": 1,
            "generated_at": 1, "executed_at": 1, "started_at": 1, "completed_at": 1,
            "generation_time": 1, "execution_time": 1, "actual_execution_time": 1,
            "blocks_generated": 1, "template_used": 1, "error_message": 1,
            "execution_attempts": 1, "last_error_message": 1, "last_error_timestamp": 1,
            "single_workflow_execution": 1, "processing_priority": 1,
            "retry_count": 1, "chrome_session_id": 1, "injection_time": 1,
            "trigger_time": 1, "processing_duration": 1
        }

        cursor = collection.find(query, projection).sort("generated_at", -1)
        if limit is not None:
            cursor = cursor.limit(limit)

        executions = [_convert_objectids(e) for e in cursor]
        logger.info(f"Retrieved {len(executions)} workflow executions.")
        return executions

    except Exception as e:
        logger.error(f"Error fetching executions: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error fetching executions: {str(e)}")
        return []


def create_execution_record(
    automa_workflow_id: str,
    content_link_id: str,
    postgres_content_id: int,
    postgres_account_id: int,
    workflow_type: str,
    status: str = "generated",
    **kwargs
) -> Optional[str]:
    """Creates a new execution record."""
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return None

    try:
        now = datetime.now(timezone.utc)
        execution_doc = {
            "automa_workflow_id": ObjectId(automa_workflow_id),
            "content_link_id": ObjectId(content_link_id),
            "postgres_content_id": postgres_content_id,
            "postgres_account_id": postgres_account_id,
            "workflow_type": workflow_type,
            "status": status,
            "executed": False,
            "success": False,
            "generated_at": now,
            "executed_at": None,
            "started_at": None,
            "completed_at": None,
            "generation_time": kwargs.get('generation_time', 0),
            "execution_time": None,
            "actual_execution_time": None,
            "blocks_generated": kwargs.get('blocks_generated', 0),
            "template_used": kwargs.get('template_used'),
            "error_message": None,
            "execution_attempts": 0,
            "last_error_message": None,
            "last_error_timestamp": None,
            "single_workflow_execution": kwargs.get('single_workflow_execution', False),
            "processing_priority": kwargs.get('processing_priority', 1),
            "retry_count": kwargs.get('retry_count', 0),
            "chrome_session_id": kwargs.get('chrome_session_id'),
            "injection_time": kwargs.get('injection_time'),
            "trigger_time": kwargs.get('trigger_time'),
            "processing_duration": kwargs.get('processing_duration')
        }

        result = collection.insert_one(execution_doc)
        logger.info(f"Created execution record: {result.inserted_id}")
        return str(result.inserted_id)

    except Exception as e:
        logger.error(f"Error creating execution record: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error creating execution record: {str(e)}")
        return None


def update_execution_record(execution_id: str, updates: Dict[str, Any]) -> bool:
    """Updates an execution record."""
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return False

    try:
        updates["last_updated"] = datetime.now(timezone.utc)
        result = collection.update_one(
            {"_id": ObjectId(execution_id)},
            {"$set": updates}
        )
        if result.modified_count > 0:
            logger.info(f"Updated execution {execution_id}")
            return True
        else:
            logger.warning(f"No changes for execution {execution_id}")
            return False
    except Exception as e:
        logger.error(f"Error updating execution {execution_id}: {e}")
        return False


def mark_execution_started(execution_id: str, chrome_session_id: str = None):
    """Mark execution as started."""
    updates = {
        "status": "running",
        "started_at": datetime.now(timezone.utc)
    }
    if chrome_session_id:
        updates["chrome_session_id"] = chrome_session_id
    update_execution_record(execution_id, updates)


def mark_execution_completed(
    execution_id: str,
    success: bool,
    execution_time: int = None,
    error_message: str = None
):
    """Mark execution as completed with retry increment."""
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return

    try:
        updates = {
            "status": "completed" if success else "failed",
            "executed": True,
            "success": success,
            "completed_at": datetime.now(timezone.utc)
        }
        if execution_time is not None:
            updates["actual_execution_time"] = execution_time
        if error_message:
            updates["error_message"] = error_message
            updates["last_error_message"] = error_message
            updates["last_error_timestamp"] = datetime.now(timezone.utc)

        collection.update_one(
            {"_id": ObjectId(execution_id)},
            {"$set": updates, "$inc": {"execution_attempts": 1}}
        )
        logger.info(f"Marked execution {execution_id} as {'success' if success else 'failed'}")

    except Exception as e:
        logger.error(f"Error marking execution complete {execution_id}: {e}")


def get_execution_by_id(execution_id: str) -> Optional[Dict[str, Any]]:
    """Fetch execution by ID."""
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return None

    try:
        execution = collection.find_one({"_id": ObjectId(execution_id)})
        return _convert_objectids(execution) if execution else None
    except Exception as e:
        logger.error(f"Error fetching execution {execution_id}: {e}")
        return None


def get_executions_by_content_id(
    content_id: int,
    workflow_type: str = None
) -> List[Dict[str, Any]]:
    """Fetch executions by content ID."""
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return []

    try:
        query = {"postgres_content_id": content_id}
        if workflow_type:
            query["workflow_type"] = workflow_type

        cursor = collection.find(query).sort("generated_at", -1)
        return [_convert_objectids(e) for e in cursor]
    except Exception as e:
        logger.error(f"Error fetching executions for content {content_id}: {e}")
        return []


def get_pending_executions(
    workflow_type: str = None,
    limit: int = None
) -> List[Dict[str, Any]]:
    """Fetch pending (unexecuted) records."""
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return []

    try:
        query = {"executed": False}
        if workflow_type:
            query["workflow_type"] = workflow_type

        cursor = collection.find(query).sort("generated_at", 1)
        if limit:
            cursor = cursor.limit(limit)

        return [_convert_objectids(e) for e in cursor]
    except Exception as e:
        logger.error(f"Error fetching pending executions: {e}")
        return []


def get_execution_statistics(workflow_type: str = None) -> Dict[str, Any]:
    """Get execution stats."""
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return {}

    try:
        match_filter = {}
        if workflow_type:
            match_filter["workflow_type"] = workflow_type

        pipeline = [
            {"$match": match_filter},
            {"$group": {
                "_id": None,
                "total_executions": {"$sum": 1},
                "executed_count": {"$sum": {"$cond": ["$executed", 1, 0]}},
                "successful_count": {"$sum": {"$cond": ["$success", 1, 0]}},
                "failed_count": {"$sum": {"$cond": [{"$and": ["$executed", {"$eq": ["$success", False]}]}, 1, 0]}},
                "avg_generation_time": {"$avg": "$generation_time"},
                "avg_execution_time": {"$avg": "$actual_execution_time"}
            }}
        ]

        result = list(collection.aggregate(pipeline))
        if not result:
            return {
                "total_executions": 0, "executed_count": 0, "successful_count": 0,
                "failed_count": 0, "success_rate": 0.0, "avg_generation_time": 0, "avg_execution_time": 0
            }

        stats = result[0]
        executed = stats.get('executed_count', 0)
        stats['success_rate'] = round((stats.get('successful_count', 0) / executed * 100), 2) if executed > 0 else 0.0
        stats.pop('_id', None)
        return stats

    except Exception as e:
        logger.error(f"Error in execution stats: {e}")
        return {}


# =================== REVERSAL & CLEANUP ===================

def reverse_js_workflow_operations(
    workflow_type: str = None,
    account_id: Optional[int] = None,
    dag_run_id: str = None
) -> dict:
    """Safely reverse JS orchestrator operations."""
    try:
        executions_col = _ensure_collection("workflow_executions")
        if executions_col is None:
            return {"success": False, "error": "No DB access", "total_reversed": 0}

        reversed_counts = {
            "workflow_executions_reset": 0, "account_profile_assignments_reversed": 0,
            "daily_workflow_limits_reset": 0, "workflow_execution_tracking_deleted": 0,
            "workflow_copies_deleted": 0, "content_links_deleted": 0,
            "execution_batches_deleted": 0, "browser_sessions_closed": 0,
            "recording_sessions_cleaned": 0
        }

        # Build filter
        filt = {}
        if workflow_type:
            filt["workflow_type"] = workflow_type
        if account_id is not None:
            filt["postgres_account_id"] = account_id
        if dag_run_id:
            filt["dag_run_id"] = dag_run_id
        filt["_id"] = {"$not": {"$regex": "^(direct_|test_)"}}

        # 1. Reset workflow_executions
        reset_data = {
            "executed": False, "executed_at": None, "execution_success": False,
            "execution_mode": None, "updated_at": None, "postgres_account_id": None,
            "account_username": None, "profile_id": None, "profile_specific_execution": None,
            "extension_id": None, "video_recording_session_id": None,
            "video_recording_enabled": None, "dag_run_id": None,
            "final_result": None, "execution_time": None, "steps_taken": None,
            "execution_error": None, "error_category": None,
            "reversed_at": datetime.now(timezone.utc),
            "reversed_by": "reverse_js_workflow_operations",
            "reversal_reason": "undo_updateWorkflowExecution_operation"
        }
        result = executions_col.update_many(filt, {"$set": reset_data})
        reversed_counts["workflow_executions_reset"] = result.modified_count

        # 2. Reset profile assignments
        profiles_col = _ensure_collection("account_profile_assignments")
        if profiles_col and account_id is not None:
            executed = list(executions_col.find(filt, {"profile_id": 1, "postgres_account_id": 1}))
            profiles_to_reset = {(r["postgres_account_id"], r["profile_id"]) for r in executed if r.get("profile_id")}
            for acc_id, prof_id in profiles_to_reset:
                result = profiles_col.update_one(
                    {"postgres_account_id": acc_id, "profile_id": prof_id, "is_active": True},
                    {"$set": {
                        "usage_stats.workflows_executed": 0,
                        "usage_stats.last_workflow_date": None,
                        "usage_stats.success_rate": 0.0,
                        "usage_stats.total_sessions": 0,
                        "reversed_at": datetime.now(timezone.utc)
                    }}
                )
                reversed_counts["account_profile_assignments_reversed"] += result.modified_count

        # 3. Reset daily limits
        analytics_col = _ensure_collection("daily_workflow_analytics")
        if analytics_col:
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            date_filter = {"date": {"$gte": today - timedelta(days=1), "$lte": today}}
            if account_id is not None:
                date_filter["postgres_account_id"] = account_id
            result = analytics_col.update_many(date_filter, {"$set": {
                "workflows_executed": 0, "workflows_remaining": None, "limit_reached": False,
                "limit_utilization_percentage": 0.0, "last_execution_at": None,
                "current_count": 0, "daily_limit": None, "needs_recalculation": True,
                "reversed_at": datetime.now(timezone.utc)
            }})
            reversed_counts["daily_workflow_limits_reset"] = result.modified_count

        # 4. Delete tracking
        tracking_col = _ensure_collection("workflow_execution_tracking")
        if tracking_col:
            track_filt = {}
            if dag_run_id:
                track_filt["dag_run_id"] = dag_run_id
            if account_id is not None:
                exec_ids = [str(e["_id"]) for e in executions_col.find(filt, {"_id": 1})]
                if exec_ids:
                    track_filt["execution_id"] = {"$in": exec_ids}
            result = tracking_col.delete_many(track_filt)
            reversed_counts["workflow_execution_tracking_deleted"] = result.deleted_count

        # 5. Delete copies
        copies_col = _ensure_collection("workflow_modified_copies")
        if copies_col:
            filt_copy = {}
            if workflow_type:
                filt_copy["workflow_type"] = workflow_type
            if account_id is not None:
                filt_copy["account_id"] = account_id
            if dag_run_id:
                filt_copy["created_for_dag_run"] = dag_run_id
            result = copies_col.delete_many(filt_copy)
            reversed_counts["workflow_copies_deleted"] = result.deleted_count

        # 6. Delete content links
        links_col = _ensure_collection("content_workflow_links")
        if links_col:
            filt_link = {}
            if workflow_type:
                filt_link["content_type"] = workflow_type
            if account_id is not None:
                filt_link["account_id"] = account_id
            result = links_col.delete_many(filt_link)
            reversed_counts["content_links_deleted"] = result.deleted_count

        # 7. Delete batches
        batches_col = _ensure_collection("multi_type_execution_batches")
        if batches_col and account_id is not None:
            result = batches_col.delete_many({"account_id": account_id})
            reversed_counts["execution_batches_deleted"] = result.deleted_count

        # 8. Close browser sessions
        sessions_col = _ensure_collection("browser_sessions")
        if sessions_col:
            sess_filt = {
                "session_purpose": {"$in": ["account_workflow_execution", "account_specific_execution"]},
                "workflow_type": "account_specific_execution"
            }
            if account_id is not None:
                sess_filt["postgres_account_id"] = account_id
            if dag_run_id:
                sess_filt["dag_run_id"] = dag_run_id
            result = sessions_col.update_many(sess_filt, {"$set": {
                "is_active": False, "session_status": "reversed", "ended_at": datetime.now(timezone.utc),
                "reversed_at": datetime.now(timezone.utc)
            }})
            reversed_counts["browser_sessions_closed"] = result.modified_count

        # 9. Clean recordings
        total_cleaned = 0
        for col_name in ["session_recordings", "video_recordings", "workflow_execution_recordings"]:
            col = _ensure_collection(col_name)
            if col:
                filt_rec = {}
                if account_id is not None:
                    filt_rec["accountId"] = account_id
                if dag_run_id:
                    filt_rec["dagRunId"] = dag_run_id
                result = col.delete_many(filt_rec)
                total_cleaned += result.deleted_count
        reversed_counts["recording_sessions_cleaned"] = total_cleaned

        total_reversed = sum(reversed_counts.values())
        logger.info(f"Reversed {total_reversed} JS operations")
        return {
            "success": True, "total_reversed": total_reversed, "details": reversed_counts,
            "filters_applied": {"workflow_type": workflow_type, "account_id": account_id, "dag_run_id": dag_run_id}
        }

    except Exception as e:
        logger.error(f"Reverse failed: {e}")
        return {"success": False, "error": str(e), "total_reversed": 0}


def delete_content_workflow_links(workflow_type: str = None) -> int:
    collection = _ensure_collection("content_workflow_links")
    if collection is None:
        return 0
    try:
        query = {"content_type": workflow_type} if workflow_type else {}
        result = collection.delete_many(query)
        logger.info(f"Deleted {result.deleted_count} content links")
        return result.deleted_count
    except Exception as e:
        logger.error(f"Delete links failed: {e}")
        return 0


def reverse_link_extraction_operations(
    workflow_type: str = None,
    account_id: Optional[int] = None
) -> dict:
    """Reverse link extraction operations."""
    try:
        reversed_counts = {
            "links_updated_deleted": 0, "content_workflow_links_deleted": 0,
            "workflow_executions_unlinked": 0, "account_statistics_reset": 0
        }

        base_filter = {}
        if workflow_type:
            base_filter["content_type"] = workflow_type
        if account_id is not None:
            base_filter["postgres_account_id"] = account_id

        # 1. Delete links_updated
        links_col = _ensure_collection("links_updated")
        if links_col:
            filt = {"postgres_account_id": account_id} if account_id is not None else {}
            result = links_col.delete_many(filt)
            reversed_counts["links_updated_deleted"] = result.deleted_count

        # 2. Delete content_workflow_links
        content_col = _ensure_collection("content_workflow_links")
        if content_col:
            result = content_col.delete_many(base_filter)
            reversed_counts["content_workflow_links_deleted"] = result.deleted_count

        # 3. Reset workflow_executions
        exec_col = _ensure_collection("workflow_executions")
        if exec_col:
            exec_filt = {}
            if workflow_type:
                exec_filt["workflow_type"] = workflow_type
            if account_id is not None:
                exec_filt["postgres_account_id"] = account_id
            result = exec_col.update_many(exec_filt, {"$set": {
                "postgres_link_id": None, "associated_link_url": None,
                "link_tweet_id": None, "link_source_page": None, "has_link": False,
                "link_association_reset_at": datetime.now(timezone.utc)
            }})
            reversed_counts["workflow_executions_unlinked"] = result.modified_count

        # 4. Reset account stats
        accounts_col = _ensure_collection("accounts")
        if accounts_col and account_id is not None:
            result = accounts_col.update_one(
                {"postgres_account_id": account_id},
                {"$set": {
                    "total_links_processed": 0, "total_replies_processed": 0,
                    "total_messages_processed": 0, "total_retweets_processed": 0,
                    "last_workflow_sync": None, "account_statistics_reset_at": datetime.now(timezone.utc)
                }}
            )
            reversed_counts["account_statistics_reset"] = result.modified_count

        total = sum(reversed_counts.values())
        logger.info(f"Reversed {total} link extraction operations")
        return {"success": True, "total_reversed": total, "details": reversed_counts}

    except Exception as e:
        logger.error(f"Link reverse failed: {e}")
        return {"success": False, "error": str(e), "total_reversed": 0}


def delete_execution_records(workflow_type: str = None) -> int:
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return 0
    try:
        query = {"workflow_type": workflow_type} if workflow_type else {}
        result = collection.delete_many(query)
        logger.info(f"Deleted {result.deleted_count} execution records")
        return result.deleted_count
    except Exception as e:
        logger.error(f"Delete executions failed: {e}")
        return 0


# =================== ENHANCED & SINGLE WORKFLOW ===================

def get_executions_by_workflow_id(automa_workflow_id: str, limit: int = None) -> List[Dict[str, Any]]:
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return []
    try:
        cursor = collection.find({"automa_workflow_id": ObjectId(automa_workflow_id)}).sort("generated_at", -1)
        if limit:
            cursor = cursor.limit(limit)
        return [_convert_objectids(e) for e in cursor]
    except Exception as e:
        logger.error(f"Error fetching by workflow ID: {e}")
        return []


def get_recent_executions_trend(workflow_type: str = None, days: int = 7) -> List[Dict[str, Any]]:
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return []
    try:
        match_stage = {}
        if workflow_type:
            match_stage["workflow_type"] = workflow_type
        start_date = datetime.now() - timedelta(days=days)
        match_stage["generated_at"] = {"$gte": start_date}

        pipeline = [
            {"$match": match_stage},
            {"$addFields": {
                "generated_date": {
                    "$dateToString": {"format": "%Y-%m-%d", "date": "$generated_at"}
                }
            }},
            {"$group": {
                "_id": "$generated_date",
                "total_executions": {"$sum": 1},
                "executed_count": {"$sum": {"$cond": [{"$eq": ["$executed", True]}, 1, 0]}},
                "successful_count": {"$sum": {"$cond": [{"$eq": ["$success", True]}, 1, 0]}},
                "failed_count": {"$sum": {"$cond": [{"$and": [{"$eq": ["$executed", True]}, {"$eq": ["$success", False]}]}, 1, 0]}}
            }},
            {"$sort": {"_id": 1}}
        ]
        result = list(collection.aggregate(pipeline))
        return [{
            "date": r["_id"],
            "total_executions": r["total_executions"],
            "executed": r["executed_count"],
            "successful": r["successful_count"],
            "failed": r["failed_count"]
        } for r in result]
    except Exception as e:
        logger.error(f"Trend fetch failed: {e}")
        return []


def update_execution_status(execution_id: str, status_updates: Dict[str, Any]) -> bool:
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return False
    try:
        status_updates["last_updated"] = datetime.now(timezone.utc)
        result = collection.update_one({"_id": ObjectId(execution_id)}, {"$set": status_updates})
        return result.modified_count > 0
    except Exception as e:
        logger.error(f"Status update failed: {e}")
        return False


def get_unexecuted_workflows(workflow_type: str = None, limit: int = None) -> List[Dict[str, Any]]:
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return []
    try:
        query = {"executed": False}
        if workflow_type:
            query["workflow_type"] = workflow_type
        cursor = collection.find(query).sort([("processing_priority", 1), ("generated_at", 1)])
        if limit:
            cursor = cursor.limit(limit)
        return [_convert_objectids(e) for e in cursor]
    except Exception as e:
        logger.error(f"Error fetching unexecuted: {e}")
        return []


def get_execution_logs_enhanced(execution_id: str = None, workflow_id: str = None, limit: int = None) -> List[Dict[str, Any]]:
    collection = _ensure_collection("workflow_logs_enhanced")
    if collection is None:
        return []
    try:
        query = {}
        if execution_id:
            query["execution_id"] = execution_id
        if workflow_id:
            query["workflow_id"] = workflow_id
        cursor = collection.find(query).sort("timestamp", -1)
        if limit:
            cursor = cursor.limit(limit)
        return list(cursor)
    except Exception as e:
        logger.error(f"Logs fetch failed: {e}")
        return []


def get_single_workflow_performance_metrics(workflow_type: str = None, limit: int = None) -> List[Dict[str, Any]]:
    collection = _ensure_collection("single_workflow_performance")
    if collection is None:
        logger.warning("single_workflow_performance collection not available")
        return []
    try:
        query = {}
        if workflow_type:
            query["workflow_type"] = workflow_type
        cursor = collection.find(query).sort("measured_at", -1)
        if limit:
            cursor = cursor.limit(limit)
        metrics = list(cursor)
        for m in metrics:
            m['_id'] = str(m['_id'])
            if m.get('automa_workflow_id'):
                m['automa_workflow_id'] = str(m['automa_workflow_id'])
        return metrics
    except Exception as e:
        logger.error(f"Performance metrics failed: {e}")
        return []


def get_single_workflow_execution_logs(workflow_type: str = None, limit: int = None) -> List[Dict[str, Any]]:
    collection = _ensure_collection("single_workflow_execution_log")
    if collection is None:
        logger.warning("single_workflow_execution_log collection not available")
        return []
    try:
        query = {}
        if workflow_type:
            query["workflow_type"] = workflow_type
        cursor = collection.find(query).sort("started_at", -1)
        if limit:
            cursor = cursor.limit(limit)
        logs = list(cursor)
        for l in logs:
            l['_id'] = str(l['_id'])
            if l.get('automa_workflow_id'):
                l['automa_workflow_id'] = str(l['automa_workflow_id'])
        return logs
    except Exception as e:
        logger.error(f"Execution logs failed: {e}")
        return []


def delete_execution_by_id(execution_id: str) -> bool:
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return False
    try:
        result = collection.delete_one({"_id": ObjectId(execution_id)})
        return result.deleted_count > 0
    except Exception as e:
        logger.error(f"Delete execution failed: {e}")
        return False


def mark_single_workflow_injection(execution_id: str, injection_time: Union[str, datetime], chrome_session_id: str = None):
    updates = {
        "injection_time": injection_time if isinstance(injection_time, datetime) else injection_time,
        "single_workflow_execution": True,
        "status": "injected"
    }
    if chrome_session_id:
        updates["chrome_session_id"] = chrome_session_id
    update_execution_record(execution_id, updates)


def mark_single_workflow_triggered(execution_id: str, trigger_time: Union[str, datetime]):
    updates = {
        "trigger_time": trigger_time if isinstance(trigger_time, datetime) else trigger_time,
        "status": "triggered"
    }
    update_execution_record(execution_id, updates)


def get_oldest_unexecuted_workflow(workflow_type: str = None) -> Optional[Dict[str, Any]]:
    executions = get_unexecuted_workflows(workflow_type=workflow_type, limit=1)
    return executions[0] if executions else None


def increment_retry_count(execution_id: str):
    collection = _ensure_collection("workflow_executions")
    if collection is not None:
        try:
            collection.update_one({"_id": ObjectId(execution_id)}, {"$inc": {"retry_count": 1}})
            logger.info(f"Incremented retry count for {execution_id}")
        except Exception as e:
            logger.error(f"Retry increment failed: {e}")


def set_processing_priority(execution_id: str, priority: int):
    update_execution_record(execution_id, {"processing_priority": priority})


def get_executions_by_priority(workflow_type: str = None, priority: int = None, limit: int = None) -> List[Dict[str, Any]]:
    collection = _ensure_collection("workflow_executions")
    if collection is None:
        return []
    try:
        query = {"executed": False}
        if workflow_type:
            query["workflow_type"] = workflow_type
        if priority is not None:
            query["processing_priority"] = priority
        cursor = collection.find(query).sort([("processing_priority", 1), ("generated_at", 1)])
        if limit:
            cursor = cursor.limit(limit)
        return [_convert_objectids(e) for e in cursor]
    except Exception as e:
        logger.error(f"Priority fetch failed: {e}")
        return []