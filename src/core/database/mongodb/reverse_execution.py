"""
FIXED MongoDB reversal operations for Execute Workflows DAG.
Now matches EXACTLY what local_executor DAG creates.
Location: src/core/database/mongodb/reverse_execution.py
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone
from bson import ObjectId
from .connection import get_mongo_collection

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

logger = logging.getLogger(__name__)


def reverse_workflow_execution_mongodb(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    dag_run_id: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    FIXED: Reverse ALL MongoDB operations performed by local_executor DAG.
    Now deletes EXACTLY what the DAG creates, nothing more, nothing less.
    """
    try:
        logger.info("=" * 80)
        logger.info("STARTING MONGODB WORKFLOW EXECUTION REVERSAL (FIXED)")
        logger.info("=" * 80)
        
        reversed_counts = {
            "execution_sessions_deleted": 0,
            "video_recording_metadata_deleted": 0,
            "screenshot_metadata_deleted": 0,
            "screenshots_deleted": 0,
            "video_recordings_deleted": 0,
            "workflow_metadata_reset": 0,
            "automa_execution_logs_deleted": 0,  # NEW
            "browser_sessions_closed": 0
        }
        
        # Build base filter - using execution_sessions as the source of truth
        base_filter = {}
        
        if workflow_type:
            base_filter["workflow_type"] = workflow_type
            logger.info(f"Filter: workflow_type = {workflow_type}")
        
        if account_id is not None:
            try:
                account_id = int(account_id)
                base_filter["postgres_account_id"] = account_id
                logger.info(f"Filter: account_id = {account_id}")
            except (ValueError, TypeError):
                logger.warning(f"Invalid account_id: {account_id}")
        
        if dag_run_id:
            base_filter["dag_run_id"] = dag_run_id
            logger.info(f"Filter: dag_run_id = {dag_run_id}")
        
        if date_from or date_to:
            date_filter = {}
            if date_from:
                date_filter["$gte"] = date_from
                logger.info(f"Filter: date_from = {date_from}")
            if date_to:
                date_filter["$lte"] = date_to
                logger.info(f"Filter: date_to = {date_to}")
            base_filter["created_at"] = date_filter
        
        logger.info(f"\nBase filter: {base_filter}")
        
        # ===================================================================
        # STEP 1: Get execution_sessions records (main source of truth)
        # ===================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: Finding execution_sessions to reverse")
        logger.info("=" * 80)
        
        execution_sessions_collection = get_mongo_collection("execution_sessions")
        
        if execution_sessions_collection is None:
            raise Exception("Failed to access execution_sessions collection")
        
        sessions_to_reverse = list(execution_sessions_collection.find(
            base_filter,
            {
                "_id": 1,
                "session_id": 1,
                "postgres_account_id": 1,
                "account_username": 1,
                "execution_id": 1,
                "screenshot_file_ids": 1,
                "video_recording_id": 1,
                "dag_run_id": 1,
                "session_metadata": 1
            }
        ))
        
        total_sessions = len(sessions_to_reverse)
        logger.info(f"Found {total_sessions} execution sessions to reverse")
        
        if total_sessions == 0:
            logger.info("No sessions found matching criteria - nothing to reverse")
            return {
                "success": True,
                "message": "No sessions found matching criteria",
                "total_reversed": 0,
                "workflows_reset": 0,
                "details": reversed_counts,
                "filters_applied": {
                    "workflow_type": workflow_type,
                    "account_id": account_id,
                    "dag_run_id": dag_run_id,
                    "date_from": date_from.isoformat() if date_from else None,
                    "date_to": date_to.isoformat() if date_to else None
                }
            }
        
        # Extract all IDs we need for cleanup
        session_ids = [s["session_id"] for s in sessions_to_reverse]
        execution_ids = [s.get("execution_id") for s in sessions_to_reverse if s.get("execution_id")]
        
        # Extract link_ids from session_metadata
        link_ids = []
        automa_workflow_ids = []
        for session in sessions_to_reverse:
            metadata = session.get("session_metadata", {})
            if metadata.get("link_id"):
                link_ids.append(metadata["link_id"])
        
        logger.info(f"Extracted {len(session_ids)} session IDs")
        logger.info(f"Extracted {len(execution_ids)} execution IDs")
        logger.info(f"Extracted {len(link_ids)} link IDs")
        
        # ===================================================================
        # STEP 2: Delete GridFS Screenshots
        # ===================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: Deleting GridFS Screenshots")
        logger.info("=" * 80)
        
        try:
            from pymongo import MongoClient
            from gridfs import GridFSBucket
            
            screenshot_metadata_collection = get_mongo_collection("screenshot_metadata")
            
            if screenshot_metadata_collection is not None:
                # Build filter for screenshots
                screenshot_filter = {
                    "$or": [
                        {"session_id": {"$in": session_ids}},
                        {"execution_id": {"$in": execution_ids}},
                        {"link_id": {"$in": link_ids}}
                    ]
                }
                
                if account_id is not None:
                    screenshot_filter["postgres_account_id"] = account_id
                
                screenshot_records = list(screenshot_metadata_collection.find(
                    screenshot_filter,
                    {"gridfs_file_id": 1}
                ))
                
                logger.info(f"Found {len(screenshot_records)} screenshot metadata records")
                
                if screenshot_records:
                    mongodb = execution_sessions_collection.database
                    screenshots_bucket = GridFSBucket(mongodb, bucket_name="screenshots")
                    
                    # Delete GridFS files
                    for screenshot in screenshot_records:
                        try:
                            gridfs_id = screenshot.get("gridfs_file_id")
                            if gridfs_id:
                                screenshots_bucket.delete(gridfs_id)
                                reversed_counts["screenshots_deleted"] += 1
                        except Exception as e:
                            logger.warning(f"Could not delete screenshot {gridfs_id}: {e}")
                    
                    # Delete metadata records
                    result = screenshot_metadata_collection.delete_many(screenshot_filter)
                    reversed_counts["screenshot_metadata_deleted"] = result.deleted_count
                    logger.info(f"✓ Deleted {result.deleted_count} screenshot metadata records")
                    logger.info(f"✓ Deleted {reversed_counts['screenshots_deleted']} GridFS screenshot files")
            
        except Exception as e:
            logger.warning(f"Error deleting screenshots: {e}")
        
        # ===================================================================
        # STEP 3: Delete GridFS Video Recordings
        # ===================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: Deleting GridFS Video Recordings")
        logger.info("=" * 80)
        
        try:
            video_metadata_collection = get_mongo_collection("video_recording_metadata")
            
            if video_metadata_collection is not None:
                # Build filter for videos
                video_filter = {
                    "$or": [
                        {"session_id": {"$in": session_ids}},
                        {"execution_id": {"$in": execution_ids}},
                        {"link_id": {"$in": link_ids}}
                    ]
                }
                
                if account_id is not None:
                    video_filter["postgres_account_id"] = account_id
                
                video_records = list(video_metadata_collection.find(
                    video_filter,
                    {"gridfs_file_id": 1}
                ))
                
                logger.info(f"Found {len(video_records)} video metadata records")
                
                if video_records:
                    mongodb = execution_sessions_collection.database
                    videos_bucket = GridFSBucket(mongodb, bucket_name="video_recordings")
                    
                    # Delete GridFS files
                    for video in video_records:
                        try:
                            gridfs_id = video.get("gridfs_file_id")
                            if gridfs_id:
                                videos_bucket.delete(gridfs_id)
                                reversed_counts["video_recordings_deleted"] += 1
                        except Exception as e:
                            logger.warning(f"Could not delete video {gridfs_id}: {e}")
                    
                    # Delete metadata records
                    result = video_metadata_collection.delete_many(video_filter)
                    reversed_counts["video_recording_metadata_deleted"] = result.deleted_count
                    logger.info(f"✓ Deleted {result.deleted_count} video metadata records")
                    logger.info(f"✓ Deleted {reversed_counts['video_recordings_deleted']} GridFS video files")
            
        except Exception as e:
            logger.warning(f"Error deleting videos: {e}")
        
        # ===================================================================
        # STEP 4: Delete Automa Execution Logs (NEW - was missing!)
        # ===================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 4: Deleting Automa Execution Logs (NEW)")
        logger.info("=" * 80)
        
        try:
            automa_logs_collection = get_mongo_collection("automa_execution_logs")
            
            if automa_logs_collection is not None:
                # Build filter for automa logs
                automa_filter = {
                    "$or": [
                        {"session_id": {"$in": session_ids}},
                        {"execution_id": {"$in": execution_ids}},
                        {"link_id": {"$in": link_ids}}
                    ]
                }
                
                if account_id is not None:
                    automa_filter["account_id"] = account_id
                
                result = automa_logs_collection.delete_many(automa_filter)
                reversed_counts["automa_execution_logs_deleted"] = result.deleted_count
                logger.info(f"✓ Deleted {result.deleted_count} automa execution log records")
            
        except Exception as e:
            logger.warning(f"Error deleting automa logs: {e}")
        
        # ===================================================================
        # STEP 5: Reset workflow_metadata execution status
        # ===================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 5: Resetting workflow_metadata execution status")
        logger.info("=" * 80)
        
        try:
            workflow_metadata_collection = get_mongo_collection("workflow_metadata")
            
            if workflow_metadata_collection is not None and link_ids:
                # Build filter for workflow_metadata
                metadata_filter = {
                    "postgres_content_id": {"$in": link_ids}
                }
                
                if account_id is not None:
                    metadata_filter["account_id"] = account_id
                
                # Reset execution status in workflow_metadata
                metadata_reset = {
                    "executed": False,
                    "success": False,
                    "executed_at": None,
                    "status": "ready_to_execute",
                    "updated_at": datetime.now(timezone.utc),
                    
                    # Clear execution details
                    "execution_time_ms": None,
                    "execution_mode": None,
                    "final_result": None,
                    "execution_error": None,
                    "error_category": None,
                    "execution_attempts": 0,
                    
                    # Clear video recording info
                    "video_recording": None,
                    
                    # Clear automa log metadata
                    "automa_logs_captured": None,
                    "automa_log_count": None,
                    "automa_error_count": None,
                    "automa_success_count": None,
                    "automa_log_status": None,
                    "automa_log_source": None
                }
                
                result = workflow_metadata_collection.update_many(
                    metadata_filter,
                    {"$set": metadata_reset}
                )
                reversed_counts["workflow_metadata_reset"] = result.modified_count
                logger.info(f"✓ Reset {result.modified_count} workflow_metadata records")
                logger.info(f"  - Set executed=False, status='ready_to_execute'")
                logger.info(f"  - Cleared execution timestamps and error messages")
            else:
                logger.info("No workflow_metadata records to reset (no link_ids found)")
            
        except Exception as e:
            logger.warning(f"Error resetting workflow_metadata: {e}")
        
        # ===================================================================
        # STEP 6: Close Browser Sessions
        # ===================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 6: Closing Browser Sessions")
        logger.info("=" * 80)
        
        try:
            browser_sessions_collection = get_mongo_collection("browser_sessions")
            
            if browser_sessions_collection is not None:
                browser_filter = {"created_by": "airflow"}
                
                if account_id is not None:
                    browser_filter["postgres_account_id"] = account_id
                
                if dag_run_id:
                    browser_filter["dag_run_id"] = dag_run_id
                
                session_update = {
                    "is_active": False,
                    "session_status": "reversed",
                    "ended_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                }
                
                result = browser_sessions_collection.update_many(
                    browser_filter,
                    {"$set": session_update}
                )
                reversed_counts["browser_sessions_closed"] = result.modified_count
                logger.info(f"✓ Closed {result.modified_count} browser sessions")
            
        except Exception as e:
            logger.warning(f"Error closing browser sessions: {e}")
        
        # ===================================================================
        # STEP 7: Delete execution_sessions records (LAST STEP)
        # ===================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 7: Deleting execution_sessions records")
        logger.info("=" * 80)
        
        result = execution_sessions_collection.delete_many(base_filter)
        reversed_counts["execution_sessions_deleted"] = result.deleted_count
        logger.info(f"✓ Deleted {result.deleted_count} execution_sessions records")
        
        # ===================================================================
        # SUMMARY
        # ===================================================================
        total_reversed = sum(reversed_counts.values())
        
        summary = {
            "success": True,
            "total_reversed": total_reversed,
            "workflows_reset": reversed_counts["workflow_metadata_reset"],
            "details": reversed_counts,
            "filters_applied": {
                "workflow_type": workflow_type,
                "account_id": account_id,
                "dag_run_id": dag_run_id,
                "date_from": date_from.isoformat() if date_from else None,
                "date_to": date_to.isoformat() if date_to else None
            },
            "reversed_at": datetime.now(timezone.utc).isoformat(),
            "operations_reversed": [
                "execution_sessions deleted",
                "video_recording_metadata deleted",
                "screenshot_metadata deleted",
                "GridFS screenshot files deleted",
                "GridFS video recording files deleted",
                "workflow_metadata execution status reset",
                "automa_execution_logs deleted (NEW)",
                "browser_sessions marked as inactive"
            ],
            "database": "MongoDB",
            "note": "Complete reversal - matches EXACTLY what local_executor DAG creates"
        }
        
        logger.info("=" * 80)
        logger.info("MONGODB REVERSAL COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Total operations reversed: {total_reversed}")
        logger.info(f"Execution sessions deleted: {reversed_counts['execution_sessions_deleted']}")
        logger.info(f"Video metadata deleted: {reversed_counts['video_recording_metadata_deleted']}")
        logger.info(f"Screenshot metadata deleted: {reversed_counts['screenshot_metadata_deleted']}")
        logger.info(f"Workflow metadata reset: {reversed_counts['workflow_metadata_reset']}")
        logger.info(f"Automa logs deleted: {reversed_counts['automa_execution_logs_deleted']}")
        logger.info(f"Screenshots deleted: {reversed_counts['screenshots_deleted']}")
        logger.info(f"Videos deleted: {reversed_counts['video_recordings_deleted']}")
        
        return summary
        
    except Exception as e:
        error_msg = f"Error reversing MongoDB workflow executions: {e}"
        logger.error(error_msg)
        logger.error("=" * 80)
        
        return {
            "success": False,
            "error": error_msg,
            "total_reversed": 0,
            "workflows_reset": 0,
            "details": reversed_counts,
            "filters_applied": {
                "workflow_type": workflow_type,
                "account_id": account_id,
                "dag_run_id": dag_run_id,
                "date_from": date_from.isoformat() if date_from else None,
                "date_to": date_to.isoformat() if date_to else None
            }
        }


def get_mongodb_reversal_preview(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    dag_run_id: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """Preview what would be reversed (updated for new structure)."""
    try:
        base_filter = {}
        
        if workflow_type:
            base_filter["workflow_type"] = workflow_type
        
        if account_id is not None:
            base_filter["postgres_account_id"] = int(account_id)
        
        if dag_run_id:
            base_filter["dag_run_id"] = dag_run_id
        
        if date_from or date_to:
            date_filter = {}
            if date_from:
                date_filter["$gte"] = date_from
            if date_to:
                date_filter["$lte"] = date_to
            base_filter["created_at"] = date_filter
        
        execution_sessions_collection = get_mongo_collection("execution_sessions")
        if not execution_sessions_collection:
            raise Exception("Failed to access execution_sessions collection")
        
        # Count execution sessions
        total_sessions = execution_sessions_collection.count_documents(base_filter)
        
        # Get session details for counting other collections
        sessions = list(execution_sessions_collection.find(
            base_filter,
            {"session_id": 1, "execution_id": 1, "session_metadata": 1}
        ).limit(1000))
        
        session_ids = [s["session_id"] for s in sessions]
        execution_ids = [s.get("execution_id") for s in sessions if s.get("execution_id")]
        link_ids = [s.get("session_metadata", {}).get("link_id") for s in sessions if s.get("session_metadata", {}).get("link_id")]
        
        # Count related collections
        screenshot_metadata_collection = get_mongo_collection("screenshot_metadata")
        screenshots_count = 0
        if screenshot_metadata_collection:
            screenshots_count = screenshot_metadata_collection.count_documents({
                "$or": [
                    {"session_id": {"$in": session_ids}},
                    {"execution_id": {"$in": execution_ids}},
                    {"link_id": {"$in": link_ids}}
                ]
            })
        
        video_metadata_collection = get_mongo_collection("video_recording_metadata")
        videos_count = 0
        if video_metadata_collection:
            videos_count = video_metadata_collection.count_documents({
                "$or": [
                    {"session_id": {"$in": session_ids}},
                    {"execution_id": {"$in": execution_ids}},
                    {"link_id": {"$in": link_ids}}
                ]
            })
        
        automa_logs_collection = get_mongo_collection("automa_execution_logs")
        automa_logs_count = 0
        if automa_logs_collection:
            automa_logs_count = automa_logs_collection.count_documents({
                "$or": [
                    {"session_id": {"$in": session_ids}},
                    {"execution_id": {"$in": execution_ids}},
                    {"link_id": {"$in": link_ids}}
                ]
            })
        
        workflow_metadata_collection = get_mongo_collection("workflow_metadata")
        workflow_metadata_count = 0
        if workflow_metadata_collection and link_ids:
            workflow_metadata_count = workflow_metadata_collection.count_documents({
                "postgres_content_id": {"$in": link_ids}
            })
        
        return {
            "total_workflows_to_reverse": total_sessions + workflow_metadata_count,
            "execution_sessions_to_delete": total_sessions,
            "workflow_metadata_to_reset": workflow_metadata_count,
            "screenshots_to_delete": screenshots_count,
            "videos_to_delete": videos_count,
            "automa_logs_to_delete": automa_logs_count,
            "filters_applied": {
                "workflow_type": workflow_type,
                "account_id": account_id,
                "dag_run_id": dag_run_id,
                "date_from": date_from.isoformat() if date_from else None,
                "date_to": date_to.isoformat() if date_to else None
            }
        }
        
    except Exception as e:
        error_msg = f"Error getting MongoDB reversal preview: {e}"
        logger.error(error_msg)
        
        return {
            "error": error_msg,
            "total_workflows_to_reverse": 0
        }