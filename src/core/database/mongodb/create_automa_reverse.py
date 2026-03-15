"""
MongoDB reversal operations for Create Automa DAG.
FIXED to use correct connection method.
Deletes automa_workflows and workflow_metadata created by the DAG.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any
from bson import ObjectId
from pymongo import MongoClient
import os

logger = logging.getLogger(__name__)


def get_mongo_client():
    """Get MongoDB client connection"""
    try:
        mongo_uri = os.getenv(
            "MONGODB_URI",
            "mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin"
        )
        
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        
        return client
        
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise


def get_mongodb_reversal_preview_create_automa(
    account_id: Optional[int] = None,
    dag_run_id: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Preview MongoDB operations for Create Automa reversal.
    Shows workflows and metadata that will be deleted.
    """
    client = None
    try:
        client = get_mongo_client()
        db = client.messages_db
        
        # Build query
        query = {}
        
        if account_id:
            query["postgres_account_id"] = account_id
        
        if date_from or date_to:
            query["created_at"] = {}
            if date_from:
                query["created_at"]["$gte"] = date_from.isoformat()
            if date_to:
                query["created_at"]["$lte"] = date_to.isoformat()
        
        # Count workflow_metadata
        metadata_count = db.workflow_metadata.count_documents(query)
        
        # Count by workflow type
        pipeline = [
            {"$match": query},
            {"$group": {"_id": "$workflow_type", "count": {"$sum": 1}}}
        ]
        breakdown = {}
        for result in db.workflow_metadata.aggregate(pipeline):
            breakdown[result["_id"]] = result["count"]
        
        # Get automa_workflow_ids to count actual workflows
        metadata_records = list(db.workflow_metadata.find(
            query,
            {"automa_workflow_id": 1}
        ).limit(1000))
        
        automa_ids = [r["automa_workflow_id"] for r in metadata_records if "automa_workflow_id" in r]
        automa_count = db.automa_workflows.count_documents({"_id": {"$in": automa_ids}}) if automa_ids else 0
        
        # Sample workflows
        sample_workflows = []
        for record in list(db.workflow_metadata.find(query).limit(5)):
            sample_workflows.append({
                'workflow_type': record.get('workflow_type'),
                'workflow_name': record.get('workflow_name'),
                'account_id': record.get('postgres_account_id'),
                'created_at': record.get('created_at'),
                'has_content': record.get('has_content', False)
            })
        
        return {
            'success': True,
            'total_workflows_to_reverse': metadata_count,
            'automa_workflows_count': automa_count,
            'breakdown_by_type': breakdown,
            'sample_workflows': sample_workflows
        }
        
    except Exception as e:
        logger.error(f"Error getting Create Automa MongoDB preview: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': str(e),
            'total_workflows_to_reverse': 0
        }
    finally:
        if client:
            client.close()


def reverse_create_automa_mongodb(
    account_id: Optional[int] = None,
    dag_run_id: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Reverse Create Automa operations in MongoDB.
    - Deletes workflow_metadata records
    - Deletes corresponding automa_workflows
    - Does NOT delete content_workflow_links (handled by filter_links reversal)
    """
    client = None
    try:
        client = get_mongo_client()
        db = client.messages_db
        
        # Build query
        query = {}
        
        if account_id:
            query["postgres_account_id"] = account_id
        
        if date_from or date_to:
            query["created_at"] = {}
            if date_from:
                query["created_at"]["$gte"] = date_from.isoformat()
            if date_to:
                query["created_at"]["$lte"] = date_to.isoformat()
        
        # Get automa_workflow_ids before deleting metadata
        metadata_records = list(db.workflow_metadata.find(
            query,
            {"automa_workflow_id": 1}
        ))
        
        automa_ids = [r["automa_workflow_id"] for r in metadata_records if "automa_workflow_id" in r]
        
        # Delete workflow_metadata
        metadata_result = db.workflow_metadata.delete_many(query)
        metadata_deleted = metadata_result.deleted_count
        
        # Delete corresponding automa_workflows
        automa_deleted = 0
        if automa_ids:
            automa_result = db.automa_workflows.delete_many({"_id": {"$in": automa_ids}})
            automa_deleted = automa_result.deleted_count
        
        logger.info(f"Reversed Create Automa in MongoDB:")
        logger.info(f"  - Deleted {metadata_deleted} workflow_metadata records")
        logger.info(f"  - Deleted {automa_deleted} automa_workflows")
        
        return {
            'success': True,
            'total_reversed': metadata_deleted,
            'workflows_reset': metadata_deleted,
            'details': {
                'metadata_deleted': metadata_deleted,
                'automa_workflows_deleted': automa_deleted
            },
            'message': f'Successfully deleted {metadata_deleted} workflows'
        }
        
    except Exception as e:
        logger.error(f"Error reversing Create Automa in MongoDB: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': str(e),
            'total_reversed': 0
        }
    finally:
        if client:
            client.close()