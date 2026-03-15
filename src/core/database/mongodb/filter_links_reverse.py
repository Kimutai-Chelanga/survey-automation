"""
MongoDB reversal operations for Filter Links DAG.
Resets workflow metadata and removes link assignments.

Location: src/core/database/mongodb/filter_links_reverse.py
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any
from .connection import get_mongo_collection

logger = logging.getLogger(__name__)


def get_mongodb_reversal_preview(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    dag_run_id: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Preview MongoDB operations for Filter Links reversal.
    
    IMPORTANT: Ignores account_id - operates on all workflows
    """
    try:
        # Get collections using get_mongo_collection
        workflow_metadata_collection = get_mongo_collection("workflow_metadata")
        if workflow_metadata_collection is None:
            raise Exception("Failed to access workflow_metadata collection")
        
        automa_workflows_collection = get_mongo_collection("automa_workflows")
        content_workflow_links_collection = get_mongo_collection("content_workflow_links")
        execution_batches_collection = get_mongo_collection("multi_type_execution_batches")
        workflow_executions_collection = get_mongo_collection("workflow_executions")
        
        # Build query (optional date filtering)
        query = {}
        
        if date_from or date_to:
            date_query = {}
            if date_from:
                date_query["$gte"] = date_from
            if date_to:
                date_query["$lte"] = date_to
            query["$or"] = [
                {"link_assigned_at": date_query},
                {"updated_at": date_query}
            ]
        
        # Count workflow_metadata with links
        metadata_count = workflow_metadata_collection.count_documents(query if query else {})
        
        # Count by workflow type
        pipeline = [
            {"$match": query if query else {}},
            {"$group": {"_id": "$workflow_type", "count": {"$sum": 1}}}
        ]
        breakdown = {}
        for result in workflow_metadata_collection.aggregate(pipeline):
            breakdown[result["_id"] or "unknown"] = result["count"]
        
        # Count other collections (only if they exist) - ✅ FIXED: Use `is not None`
        automa_count = 0
        if automa_workflows_collection is not None:
            automa_count = automa_workflows_collection.count_documents({"has_real_link": True})
        
        content_links_count = 0
        if content_workflow_links_collection is not None:
            content_links_count = content_workflow_links_collection.count_documents({})
        
        execution_batches_count = 0
        if execution_batches_collection is not None:
            execution_batches_count = execution_batches_collection.count_documents({})
        
        workflow_executions_count = 0
        if workflow_executions_collection is not None:
            workflow_executions_count = workflow_executions_collection.count_documents({})
        
        # Sample workflows
        sample_workflows = []
        for record in list(workflow_metadata_collection.find(query if query else {}).limit(5)):
            sample_workflows.append({
                'workflow_type': record.get('workflow_type'),
                'workflow_name': record.get('workflow_name'),
                'has_link': record.get('has_link'),
                'link_url': record.get('link_url', '')[:50] if record.get('link_url') else None,
                'status': record.get('status')
            })
        
        total_to_reverse = (
            metadata_count + 
            automa_count + 
            content_links_count + 
            execution_batches_count + 
            workflow_executions_count
        )
        
        return {
            'success': True,
            'total_workflows_to_reverse': total_to_reverse,
            'breakdown_by_type': breakdown,
            'sample_workflows': sample_workflows,
            'screenshots_to_delete': 0,
            'videos_to_delete': 0
        }
        
    except Exception as e:
        logger.error(f"Error getting Filter Links MongoDB preview: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'total_workflows_to_reverse': 0
        }


def reverse_workflow_execution_mongodb(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    dag_run_id: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Reverse Filter Links operations in MongoDB.
    
    IMPORTANT: Ignores account_id - operates on all workflows
    
    Based on the reset script:
    1. Reset workflow_metadata - remove all link assignments
    2. Reset automa_workflows link assignments
    3. Clear content_workflow_links collection
    4. Clear multi_type_execution_batches collection
    5. Clear workflow_executions collection
    """
    try:
        # Get collections using get_mongo_collection
        workflow_metadata_collection = get_mongo_collection("workflow_metadata")
        if workflow_metadata_collection is None:
            raise Exception("Failed to access workflow_metadata collection")
        
        automa_workflows_collection = get_mongo_collection("automa_workflows")
        content_workflow_links_collection = get_mongo_collection("content_workflow_links")
        execution_batches_collection = get_mongo_collection("multi_type_execution_batches")
        workflow_executions_collection = get_mongo_collection("workflow_executions")
        
        # Build query (optional date filtering)
        query = {}
        if date_from or date_to:
            date_query = {}
            if date_from:
                date_query["$gte"] = date_from
            if date_to:
                date_query["$lte"] = date_to
            query["$or"] = [
                {"link_assigned_at": date_query},
                {"updated_at": date_query}
            ]
        
        # Step 1: Reset workflow_metadata - remove link assignments
        metadata_result = workflow_metadata_collection.update_many(
            query if query else {},
            {
                "$set": {
                    # Reset link assignment
                    "has_link": False,
                    "link_url": None,
                    "link_assigned_at": None,
                    
                    # Reset content tracking
                    "postgres_content_id": None,
                    "content_preview": None,
                    "content_hash": None,
                    
                    # Reset execution status
                    "status": "generated",
                    "execute": False,
                    "executed": False,
                    "success": False,
                    "execution_attempts": 0,
                    
                    # Update timestamp
                    "updated_at": datetime.now(),
                    
                    # Clear assignment metadata
                    "assignment_method": None,
                    "assignment_source": None,
                    "tweeted_date": None
                },
                "$unset": {
                    "link_id": "",
                    "workflow_processed_time": "",
                    "execution_time": "",
                    "error_message": ""
                }
            }
        )
        metadata_reset = metadata_result.modified_count
        
        # Step 2: Reset automa_workflows link assignments - ✅ FIXED: Use `is not None`
        automa_reset = 0
        if automa_workflows_collection is not None:
            automa_result = automa_workflows_collection.update_many(
                {"has_real_link": True},
                {
                    "$set": {"has_real_link": False, "link_assigned": False},
                    "$unset": {"assigned_link": "", "assigned_content_id": "", "assignment_time": ""}
                }
            )
            automa_reset = automa_result.modified_count
        
        # Step 3: Clear content_workflow_links - ✅ FIXED: Use `is not None`
        content_links_deleted = 0
        if content_workflow_links_collection is not None:
            content_links_result = content_workflow_links_collection.delete_many({})
            content_links_deleted = content_links_result.deleted_count
        
        # Step 4: Clear execution_batches - ✅ FIXED: Use `is not None`
        batches_deleted = 0
        if execution_batches_collection is not None:
            batches_result = execution_batches_collection.delete_many({})
            batches_deleted = batches_result.deleted_count
        
        # Step 5: Clear workflow_executions - ✅ FIXED: Use `is not None`
        executions_deleted = 0
        if workflow_executions_collection is not None:
            executions_result = workflow_executions_collection.delete_many({})
            executions_deleted = executions_result.deleted_count
        
        logger.info(f"Reversed Filter Links in MongoDB:")
        logger.info(f"  - Reset {metadata_reset} workflow_metadata")
        logger.info(f"  - Reset {automa_reset} automa_workflows")
        logger.info(f"  - Deleted {content_links_deleted} content_workflow_links")
        logger.info(f"  - Deleted {batches_deleted} execution_batches")
        logger.info(f"  - Deleted {executions_deleted} workflow_executions")
        
        total_reversed = (
            metadata_reset + 
            automa_reset + 
            content_links_deleted + 
            batches_deleted + 
            executions_deleted
        )
        
        return {
            'success': True,
            'total_reversed': total_reversed,
            'workflows_reset': metadata_reset,
            'details': {
                'metadata_reset': metadata_reset,
                'automa_workflows_reset': automa_reset,
                'content_links_deleted': content_links_deleted,
                'execution_batches_deleted': batches_deleted,
                'workflow_executions_deleted': executions_deleted,
                'screenshots_deleted': 0,
                'video_recordings_deleted': 0
            },
            'message': f'Successfully reversed {total_reversed} MongoDB operations'
        }
        
    except Exception as e:
        logger.error(f"Error reversing Filter Links in MongoDB: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'total_reversed': 0
        }