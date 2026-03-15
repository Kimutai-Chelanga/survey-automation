import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from bson import ObjectId
from .connection import get_mongo_collection

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

logger = logging.getLogger(__name__)

def create_workflow_in_mongodb(name: str, description: str, drawflow: Dict[str, Any], version: str = "1.0.0") -> Optional[str]:
    """Creates a new automa workflow document in the MongoDB automa_workflows collection."""
    collection = get_mongo_collection("automa_workflows")
    if collection is None:
        logger.error("Failed to access MongoDB automa_workflows collection.")
        return None

    try:
        workflow_doc = {
            "name": name,
            "description": description,
            "version": version,
            "drawflow": drawflow,
            "extVersion": "1.0.0",  # Add required field
            "icon": "",  # Add required field
            "settings": {},  # Add required field
            "globalData": "{}",  # Add required field
            "table": [],  # Add required field
            "includedWorkflows": {}  # Add required field
        }
        result = collection.insert_one(workflow_doc)
        logger.info(f"✅ Created automa workflow in MongoDB with ID: {result.inserted_id}")
        return str(result.inserted_id)
    except Exception as e:
        logger.error(f"❌ Error creating automa workflow in MongoDB: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error creating automa workflow in MongoDB: {str(e)}")
        return None

def update_workflow_in_mongodb(workflow_id: str, updates: Dict[str, Any]):
    """Updates an automa workflow document in the MongoDB automa_workflows collection."""
    collection = get_mongo_collection("automa_workflows")
    if collection is None:
        logger.error("Failed to access MongoDB automa_workflows collection.")
        return

    try:
        result = collection.update_one({"_id": ObjectId(workflow_id)}, {"$set": updates})
        if result.modified_count > 0:
            logger.info(f"✅ Updated automa workflow {workflow_id} in MongoDB")
        else:
            logger.warning(f"No automa workflow found with ID {workflow_id} or no changes applied")
    except Exception as e:
        logger.error(f"❌ Error updating automa workflow {workflow_id} in MongoDB: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error updating automa workflow in MongoDB: {str(e)}")

def get_filtered_workflows_from_mongodb(filters: Dict[str, Any] = None, limit: int = None) -> List[Dict[str, Any]]:
    """Fetches filtered content workflow links from the MongoDB content_workflow_links collection."""
    collection = get_mongo_collection("content_workflow_links")
    if collection is None:
        logger.error("Failed to access MongoDB content_workflow_links collection.")
        return []

    try:
        query = filters or {}
        # Filter for messages content type
        query["content_type"] = "messages"
        
        projection = {
            "_id": 1,
            "postgres_content_id": 1,
            "postgres_account_id": 1,  # Updated to account-centric field
            "workflow_name": 1,
            "linked_at": 1,
            "content_preview": 1,
            "has_link": 1,
            "has_content": 1,  # New field from schema
            "automa_workflow_id": 1
        }
        cursor = collection.find(query, projection).sort("linked_at", -1)
        if limit is not None:
            cursor = cursor.limit(limit)
        workflows = list(cursor)
        for workflow in workflows:
            workflow['_id'] = str(workflow['_id'])
            if 'automa_workflow_id' in workflow:
                workflow['automa_workflow_id'] = str(workflow['automa_workflow_id'])
        logger.info(f"Retrieved {len(workflows)} message workflow links from MongoDB.")
        return workflows
    except Exception as e:
        logger.error(f"Error fetching message workflow links from MongoDB: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching message workflow links from MongoDB: {str(e)}")
        return []

def create_workflow_with_content_link(
    name: str, 
    description: str, 
    drawflow: Dict[str, Any], 
    content_id: int,
    account_id: int,  # Changed from user_id to account_id
    content_metadata: Optional[Dict[str, Any]] = None,
    version: str = "1.0.0"
) -> Optional[str]:
    """Creates a new automa workflow with content connection using separated schema."""
    # First create the automa workflow
    automa_workflow_id = create_workflow_in_mongodb(name, description, drawflow, version)
    if not automa_workflow_id:
        return None
    
    # Then create the content link
    content_link_collection = get_mongo_collection("content_workflow_links")
    if content_link_collection is None:
        logger.error("Failed to access MongoDB content_workflow_links collection.")
        return None

    try:
        content_metadata = content_metadata or {}
        link_doc = {
            "postgres_content_id": content_id,
            "postgres_account_id": account_id,  # Updated to account-centric field
            "content_type": "messages",
            "automa_workflow_id": ObjectId(automa_workflow_id),
            "workflow_name": name,
            "linked_at": datetime.now().isoformat(),
            "content_preview": content_metadata.get('preview', ''),
            "content_length": content_metadata.get('length', 0),
            "content_hash": content_metadata.get('hash', 0),
            "has_link": content_metadata.get('has_link', False),
            "has_content": content_metadata.get('has_content', False),  # New field from schema
            "content_updated_at": datetime.now().isoformat()  # New field from schema
        }
        
        result = content_link_collection.insert_one(link_doc)
        logger.info(f"✅ Created message workflow link in MongoDB with ID: {result.inserted_id}")
        logger.info(f"   Linked to message_id: {content_id}, account_id: {account_id}")
        
        # Create initial execution record
        _create_initial_execution_record(
            automa_workflow_id, result.inserted_id, content_id, account_id, "messages"
        )
        
        return str(result.inserted_id)
    except Exception as e:
        logger.error(f"❌ Error creating message workflow link in MongoDB: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error creating message workflow link in MongoDB: {str(e)}")
        return None

def _create_initial_execution_record(
    automa_workflow_id: str, 
    content_link_id: ObjectId, 
    content_id: int, 
    account_id: int,  # Changed from user_id to account_id
    workflow_type: str
):
    """Creates initial execution record in workflow_executions collection."""
    execution_collection = get_mongo_collection("workflow_executions")
    if execution_collection is None:
        logger.error("Failed to access MongoDB workflow_executions collection.")
        return

    try:
        execution_doc = {
            "automa_workflow_id": ObjectId(automa_workflow_id),
            "content_link_id": content_link_id,
            "postgres_content_id": content_id,
            "postgres_account_id": account_id,  # Updated to account-centric field
            "workflow_type": workflow_type,
            "status": "generated",
            "executed": False,
            "success": False,
            "has_content": False,  # New field from schema
            "generated_at": datetime.now().isoformat(),
            "executed_at": None,
            "started_at": None,
            "completed_at": None,
            "generation_time": 0,
            "execution_time": None,
            "actual_execution_time": None,
            "blocks_generated": 0,
            "template_used": None,
            "error_message": None,
            "execution_attempts": 0,
            "last_error_message": None,
            "last_error_timestamp": None,
            "content_status_updated_at": datetime.now().isoformat()  # New field from schema
        }
        
        result = execution_collection.insert_one(execution_doc)
        logger.info(f"✅ Created initial execution record with ID: {result.inserted_id}")
    except Exception as e:
        logger.error(f"❌ Error creating initial execution record: {e}")

def get_workflow_by_content_id(content_id: int) -> Optional[Dict[str, Any]]:
    """Fetches a workflow link by its linked content ID."""
    collection = get_mongo_collection("content_workflow_links")
    if collection is None:
        logger.error("Failed to access MongoDB content_workflow_links collection.")
        return None

    try:
        workflow_link = collection.find_one({
            "postgres_content_id": content_id,
            "content_type": "messages"
        })
        if workflow_link:
            workflow_link['_id'] = str(workflow_link['_id'])
            if 'automa_workflow_id' in workflow_link:
                workflow_link['automa_workflow_id'] = str(workflow_link['automa_workflow_id'])
            logger.info(f"Retrieved message workflow link for content_id {content_id}")
            return workflow_link
        else:
            logger.warning(f"No message workflow link found for content_id {content_id}")
            return None
    except Exception as e:
        logger.error(f"Error fetching message workflow link by content_id {content_id}: {e}")
        return None

def get_workflows_by_account_id(account_id: int, limit: int = None) -> List[Dict[str, Any]]:
    """Fetches workflow links for a specific account."""
    collection = get_mongo_collection("content_workflow_links")
    if collection is None:
        logger.error("Failed to access MongoDB content_workflow_links collection.")
        return []

    try:
        query = {
            "postgres_account_id": account_id,  # Updated to account-centric field
            "content_type": "messages"
        }
        cursor = collection.find(query).sort("linked_at", -1)
        if limit is not None:
            cursor = cursor.limit(limit)
        
        workflow_links = list(cursor)
        for link in workflow_links:
            link['_id'] = str(link['_id'])
            if 'automa_workflow_id' in link:
                link['automa_workflow_id'] = str(link['automa_workflow_id'])
        
        logger.info(f"Retrieved {len(workflow_links)} message workflow links for account {account_id}")
        return workflow_links
    except Exception as e:
        logger.error(f"Error fetching message workflow links for account {account_id}: {e}")
        return []

# NEW METHODS BASED ON UPDATED SCHEMA

def get_account_performance_analytics(account_id: int = None) -> List[Dict[str, Any]]:
    """Fetches account performance analytics using the database view."""
    collection = get_mongo_collection("account_performance_analytics")
    if collection is None:
        logger.error("Failed to access MongoDB account_performance_analytics view.")
        return []

    try:
        query = {}
        if account_id:
            query = {"_id.account_id": account_id}
        
        analytics = list(collection.find(query))
        logger.info(f"Retrieved account performance analytics for {'account ' + str(account_id) if account_id else 'all accounts'}")
        return analytics
    except Exception as e:
        logger.error(f"Error fetching account performance analytics: {e}")
        return []

def get_content_integration_status(account_id: int = None) -> Dict[str, Any]:
    """Gets content integration statistics for messages workflow type."""
    collection = get_mongo_collection("content_workflow_links")
    if collection is None:
        logger.error("Failed to access MongoDB content_workflow_links collection.")
        return {}

    try:
        match_stage = {"content_type": "messages"}
        if account_id:
            match_stage["postgres_account_id"] = account_id

        pipeline = [
            {"$match": match_stage},
            {"$group": {
                "_id": "$has_content",
                "count": {"$sum": 1},
                "latest_update": {"$max": "$content_updated_at"}
            }},
            {"$sort": {"_id": 1}}
        ]
        
        result = list(collection.aggregate(pipeline))
        
        # Format results
        stats = {
            "total_workflows": sum(item["count"] for item in result),
            "with_content": 0,
            "without_content": 0,
            "integration_rate": 0.0
        }
        
        for item in result:
            if item["_id"]:
                stats["with_content"] = item["count"]
            else:
                stats["without_content"] = item["count"]
        
        if stats["total_workflows"] > 0:
            stats["integration_rate"] = round(
                (stats["with_content"] / stats["total_workflows"]) * 100, 2
            )
        
        logger.info(f"Retrieved content integration status for messages")
        return stats
    except Exception as e:
        logger.error(f"Error fetching content integration status: {e}")
        return {}

def get_recent_workflow_executions(account_id: int = None, days: int = 7, limit: int = 100) -> List[Dict[str, Any]]:
    """Gets recent workflow executions for messages."""
    collection = get_mongo_collection("workflow_executions")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_executions collection.")
        return []

    try:
        # Calculate date threshold
        date_threshold = datetime.now() - timedelta(days=days)
        
        match_stage = {
            "workflow_type": "messages",
            "executed_at": {"$gte": date_threshold.isoformat()}
        }
        if account_id:
            match_stage["postgres_account_id"] = account_id

        pipeline = [
            {"$match": match_stage},
            {"$lookup": {
                "from": "accounts",
                "localField": "postgres_account_id",
                "foreignField": "postgres_account_id",
                "as": "account_info"
            }},
            {"$unwind": {"path": "$account_info", "preserveNullAndEmptyArrays": True}},
            {"$project": {
                "automa_workflow_id": 1,
                "workflow_type": 1,
                "executed_at": 1,
                "success": 1,
                "has_content": 1,
                "logs_captured": 1,
                "log_items_count": 1,
                "execution_duration": 1,
                "execution_id": 1,
                "username": "$account_info.username",
                "postgres_account_id": 1
            }},
            {"$sort": {"executed_at": -1}},
            {"$limit": limit}
        ]
        
        executions = list(collection.aggregate(pipeline))
        logger.info(f"Retrieved {len(executions)} recent message workflow executions")
        return executions
    except Exception as e:
        logger.error(f"Error fetching recent workflow executions: {e}")
        return []

def get_single_workflow_performance(workflow_type: str = "messages", limit: int = 50) -> List[Dict[str, Any]]:
    """Gets single workflow performance metrics."""
    collection = get_mongo_collection("single_workflow_performance")
    if collection is None:
        logger.error("Failed to access MongoDB single_workflow_performance collection.")
        return []

    try:
        query = {"workflow_type": workflow_type}
        
        cursor = collection.find(query).sort("measured_at", -1)
        if limit:
            cursor = cursor.limit(limit)
        
        performance = list(cursor)
        logger.info(f"Retrieved {len(performance)} single workflow performance records for {workflow_type}")
        return performance
    except Exception as e:
        logger.error(f"Error fetching single workflow performance: {e}")
        return []

def get_automa_execution_logs(execution_id: str = None, account_id: int = None, limit: int = 50) -> List[Dict[str, Any]]:
    """Gets comprehensive Automa execution logs."""
    collection = get_mongo_collection("automa_execution_logs")
    if collection is None:
        logger.error("Failed to access MongoDB automa_execution_logs collection.")
        return []

    try:
        query = {"workflow_type": "messages"}
        if execution_id:
            query["execution_id"] = execution_id
        if account_id:
            query["postgres_account_id"] = account_id
        
        cursor = collection.find(query).sort("captured_at", -1)
        if limit:
            cursor = cursor.limit(limit)
        
        logs = list(cursor)
        logger.info(f"Retrieved {len(logs)} Automa execution logs")
        return logs
    except Exception as e:
        logger.error(f"Error fetching Automa execution logs: {e}")
        return []

def get_chrome_session_logs(account_id: int = None, limit: int = 20) -> List[Dict[str, Any]]:
    """Gets Chrome session logs for tracking browser sessions."""
    collection = get_mongo_collection("chrome_session_logs")
    if collection is None:
        logger.error("Failed to access MongoDB chrome_session_logs collection.")
        return []

    try:
        query = {}
        if account_id:
            query["postgres_account_id"] = account_id
        
        cursor = collection.find(query).sort("session_started_at", -1)
        if limit:
            cursor = cursor.limit(limit)
        
        sessions = list(cursor)
        logger.info(f"Retrieved {len(sessions)} Chrome session logs")
        return sessions
    except Exception as e:
        logger.error(f"Error fetching Chrome session logs: {e}")
        return []

def get_workflow_execution_details(execution_id: str) -> Optional[Dict[str, Any]]:
    """Gets detailed execution information for a specific workflow execution."""
    collection = get_mongo_collection("workflow_execution_details")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_execution_details collection.")
        return None

    try:
        details = collection.find_one({"execution_id": execution_id})
        if details:
            logger.info(f"Retrieved workflow execution details for {execution_id}")
        else:
            logger.warning(f"No execution details found for {execution_id}")
        return details
    except Exception as e:
        logger.error(f"Error fetching workflow execution details: {e}")
        return None

def get_workflow_step_execution(execution_id: str) -> List[Dict[str, Any]]:
    """Gets step-by-step execution details for a workflow."""
    collection = get_mongo_collection("workflow_step_execution")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_step_execution collection.")
        return []

    try:
        steps = list(collection.find({"execution_id": execution_id}).sort("step_order", 1))
        logger.info(f"Retrieved {len(steps)} workflow steps for execution {execution_id}")
        return steps
    except Exception as e:
        logger.error(f"Error fetching workflow step execution: {e}")
        return []

def update_content_status(content_id: int, has_content: bool, metadata: Dict[str, Any] = None):
    """Updates the has_content status for a workflow link."""
    collection = get_mongo_collection("content_workflow_links")
    if collection is None:
        logger.error("Failed to access MongoDB content_workflow_links collection.")
        return

    try:
        update_doc = {
            "has_content": has_content,
            "content_updated_at": datetime.now().isoformat()
        }
        
        if metadata:
            update_doc.update(metadata)
        
        result = collection.update_one(
            {
                "postgres_content_id": content_id, 
                "content_type": "messages"
            }, 
            {"$set": update_doc}
        )
        
        if result.modified_count > 0:
            logger.info(f"✅ Updated content status for message {content_id}: has_content={has_content}")
            
            # Also update execution record
            _update_execution_content_status(content_id, has_content)
        else:
            logger.warning(f"No message workflow link found for content_id {content_id}")
    except Exception as e:
        logger.error(f"❌ Error updating content status: {e}")

def _update_execution_content_status(content_id: int, has_content: bool):
    """Updates content status in workflow execution record."""
    collection = get_mongo_collection("workflow_executions")
    if collection is None:
        return

    try:
        result = collection.update_many(
            {
                "postgres_content_id": content_id,
                "workflow_type": "messages"
            },
            {"$set": {
                "has_content": has_content,
                "content_status_updated_at": datetime.now().isoformat()
            }}
        )
        
        if result.modified_count > 0:
            logger.info(f"Updated content status in {result.modified_count} execution records")
    except Exception as e:
        logger.error(f"Error updating execution content status: {e}")

def get_account_workflow_summary(account_id: int) -> Dict[str, Any]:
    """Gets workflow summary for a specific account."""
    collection = get_mongo_collection("workflows")
    if collection is None:
        logger.error("Failed to access MongoDB workflows collection.")
        return {}

    try:
        pipeline = [
            {"$match": {"postgres_account_id": account_id}},
            {"$group": {
                "_id": "$workflow_type",
                "total_workflows": {"$sum": 1},
                "active_workflows": {
                    "$sum": {"$cond": [{"$eq": ["$is_active", True]}, 1, 0]}
                },
                "last_updated": {"$max": "$updated_time"}
            }},
            {"$sort": {"_id": 1}}
        ]
        
        summary = list(collection.aggregate(pipeline))
        logger.info(f"Retrieved workflow summary for account {account_id}")
        return {
            "account_id": account_id,
            "workflow_summary": summary
        }
    except Exception as e:
        logger.error(f"Error fetching account workflow summary: {e}")
        return {}

def get_browser_session_info(account_id: int = None) -> List[Dict[str, Any]]:
    """Gets browser session information."""
    collection = get_mongo_collection("browser_sessions")
    if collection is None:
        logger.error("Failed to access MongoDB browser_sessions collection.")
        return []

    try:
        query = {}
        if account_id:
            query["postgres_account_id"] = account_id
        
        sessions = list(collection.find(query).sort("created_at", -1))
        logger.info(f"Retrieved {len(sessions)} browser sessions")
        return sessions
    except Exception as e:
        logger.error(f"Error fetching browser session info: {e}")
        return []

def delete_all() -> int:
    """Deletes all message-related data from the separated schema collections."""
    deleted_count = 0
    
    # Delete from content_workflow_links
    content_links_collection = get_mongo_collection("content_workflow_links")
    if content_links_collection:
        try:
            result = content_links_collection.delete_many({"content_type": "messages"})
            deleted_count += result.deleted_count
            logger.info(f"Deleted {result.deleted_count} message workflow links from MongoDB.")
        except Exception as e:
            logger.error(f"❌ Error deleting message workflow links from MongoDB: {e}")
            if STREAMLIT_AVAILABLE:
                st.error(f"❌ Error deleting message workflow links from MongoDB: {str(e)}")

    # Delete from workflow_executions
    executions_collection = get_mongo_collection("workflow_executions")
    if executions_collection:
        try:
            result = executions_collection.delete_many({"workflow_type": "messages"})
            deleted_count += result.deleted_count
            logger.info(f"Deleted {result.deleted_count} message workflow executions from MongoDB.")
        except Exception as e:
            logger.error(f"❌ Error deleting message workflow executions from MongoDB: {e}")
            if STREAMLIT_AVAILABLE:
                st.error(f"❌ Error deleting message workflow executions from MongoDB: {str(e)}")
    
    return deleted_count

def get_workflow_logs(workflow_id: str, limit: int = None) -> List[Dict[str, Any]]:
    """Fetches logs for a specific workflow from workflow_logs_enhanced collection."""
    collection = get_mongo_collection("workflow_logs_enhanced")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_logs_enhanced collection.")
        if STREAMLIT_AVAILABLE:
            st.error("❌ Failed to access MongoDB workflow_logs_enhanced collection.")
        return []

    try:
        query = {"workflow_id": workflow_id}
        projection = {
            "_id": 0,  # Exclude _id for cleaner display
            "workflow_id": 1,
            "workflow_name": 1,
            "workflow_type": 1,
            "status": 1,
            "timestamp": 1,
            "log_level": 1,
            "message": 1,
            "dag_run_id": 1
        }
        cursor = collection.find(query, projection).sort("timestamp", -1)
        if limit is not None:
            cursor = cursor.limit(limit)
        
        logs = list(cursor)
        logger.info(f"Retrieved {len(logs)} logs for workflow {workflow_id} from MongoDB.")
        return logs
    except Exception as e:
        logger.error(f"Error fetching logs for workflow {workflow_id}: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching logs for workflow {workflow_id}: {str(e)}")
        return []

def get_workflow_statistics() -> Dict[str, Any]:
    """Get comprehensive statistics about message workflows and their content connections."""
    collection = get_mongo_collection("content_workflow_links")
    if collection is None:
        logger.error("Failed to access MongoDB content_workflow_links collection.")
        return {}

    try:
        stats = {}
        
        # Total workflow links for messages
        stats['total_workflows'] = collection.count_documents({"content_type": "messages"})
        
        # Content integration statistics
        content_stats = get_content_integration_status()
        stats.update(content_stats)
        
        # Workflows by account (top 10)
        account_pipeline = [
            {"$match": {"content_type": "messages", "postgres_account_id": {"$ne": None}}},
            {"$lookup": {
                "from": "accounts",
                "localField": "postgres_account_id",
                "foreignField": "postgres_account_id",
                "as": "account_info"
            }},
            {"$unwind": {"path": "$account_info", "preserveNullAndEmptyArrays": True}},
            {"$group": {
                "_id": "$postgres_account_id", 
                "count": {"$sum": 1},
                "username": {"$first": "$account_info.username"}
            }},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        stats['workflows_by_account'] = list(collection.aggregate(account_pipeline))
        
        # Recent workflow creation dates
        recent_pipeline = [
            {"$match": {"content_type": "messages"}},
            {"$addFields": {
                "linked_date": {"$dateFromString": {"dateString": "$linked_at"}}
            }},
            {"$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$linked_date"}},
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id": -1}},
            {"$limit": 7}
        ]
        stats['recent_workflow_creation'] = list(collection.aggregate(recent_pipeline))
        
        logger.info("Retrieved comprehensive message workflow statistics")
        return stats
    except Exception as e:
        logger.error(f"Error fetching message workflow statistics: {e}")
        return {}