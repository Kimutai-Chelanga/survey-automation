import os
import logging
from datetime import datetime, timedelta, timezone
from bson import ObjectId
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ensure_integer_account_id(account_id):
    """Ensure account_id is an integer, converting from string if necessary"""
    if account_id is None:
        return 1
    
    if isinstance(account_id, int):
        return account_id
    
    if isinstance(account_id, str):
        if account_id.lower() == 'default':
            return 1
        try:
            return int(account_id)
        except ValueError:
            logger.warning(f"Cannot convert account_id '{account_id}' to integer, using default 1")
            return 1
    
    try:
        return int(account_id)
    except (ValueError, TypeError):
        logger.warning(f"Cannot convert account_id of type {type(account_id)} to integer, using default 1")
        return 1

def count_workflow_blocks(automa_workflow):
    """Count the number of blocks in an automa workflow"""
    try:
        if not automa_workflow:
            return None
            
        drawflow = automa_workflow.get('drawflow', {})
        if drawflow and isinstance(drawflow, dict):
            total_blocks = 0
            for page_key, page_data in drawflow.items():
                if isinstance(page_data, dict) and 'data' in page_data:
                    data_section = page_data['data']
                    if isinstance(data_section, dict):
                        total_blocks += len(data_section)
            return total_blocks if total_blocks > 0 else None
        
        global_data = automa_workflow.get('globalData', '')
        if global_data:
            try:
                if isinstance(global_data, str):
                    parsed_data = json.loads(global_data)
                    if isinstance(parsed_data, dict):
                        return len(parsed_data)
                elif isinstance(global_data, dict):
                    return len(global_data)
            except json.JSONDecodeError:
                pass
        
        return None
        
    except Exception as e:
        logger.debug(f"Error counting workflow blocks: {e}")
        return None

def create_workflow_execution_record(
    mongo_db,
    automa_workflow_id: str,
    workflow_type: str,
    postgres_content_id: int,
    link_url: str,                     # ← REQUIRED: the actual link
    content_link_id: str = None,
    account_id: int = None,
) -> str | None:
    """
    Creates a workflow execution record.
    `has_link` is **True** if and only if `link_url` is a non-empty string that looks like a URL.
    """
    # --------------------------------------------------------------------- #
    #  1. Determine has_link – SIMPLE & RELIABLE
    # --------------------------------------------------------------------- #
    has_link = bool(
        link_url
        and isinstance(link_url, str)
        and link_url.strip()
        and ("http://" in link_url or "https://" in link_url)
    )

    # --------------------------------------------------------------------- #
    #  2. Build the execution document
    # --------------------------------------------------------------------- #
    execution_doc = {
        "automa_workflow_id": ObjectId(automa_workflow_id),
        "content_link_id": ObjectId(content_link_id) if content_link_id else None,
        "postgres_content_id": postgres_content_id,
        "account_id": int(account_id) if account_id else 1,
        "workflow_type": workflow_type,

        "status": "generated",
        "executed": False,
        "success": False,

        "generated_at": datetime.now(timezone.utc),
        "started_at": None,
        "completed_at": None,

        # LINK FIELDS
        "link_url": link_url.strip() if link_url else None,
        "has_link": has_link,                     # ← ALWAYS CORRECT

        # Optional extras (safe defaults)
        "generation_time": 0,
        "execution_time": None,
        "actual_execution_time": None,
        "error_message": None,
        "execution_attempts": 0,
    }

    try:
        result = mongo_db.workflow_executions.insert_one(execution_doc)
        exec_id = str(result.inserted_id)
        logger.info(f"Created execution {exec_id} | has_link={has_link} | url={link_url}")
        return exec_id
    except Exception as e:
        logger.error(f"Failed to create execution record: {e}")
        return None

def create_multi_type_execution_batch(mongo_db, postgres_content_id, link_url, account_id,
                                    workflow_assignments):
    """
    Create a batch of execution records for multi-type assignment.
    
    UPDATED: Works directly with automa_workflows, no copies
    """
    try:
        # Ensure account_id is integer
        account_id = ensure_integer_account_id(account_id)
        
        batch_id = str(ObjectId())
        execution_records = []
        
        for assignment in workflow_assignments:
            if assignment['success']:
                # Create execution record for successful assignment
                execution_record_id = create_workflow_execution_record(
                    mongo_db,
                    assignment.get('automa_workflow_id'),
                    assignment['workflow_type'],
                    postgres_content_id,
                    link_url,
                    assignment.get('content_link_id'),
                    account_id
                )
                
                if execution_record_id:
                    # Update the execution record with batch information
                    mongo_db.workflow_executions.update_one(
                        {'_id': ObjectId(execution_record_id)},
                        {
                            '$set': {
                                'multi_type_batch_id': batch_id,
                                'batch_size': len(workflow_assignments),
                                'successful_assignments_in_batch': len([wa for wa in workflow_assignments if wa['success']]),
                                'failed_assignments_in_batch': len([wa for wa in workflow_assignments if not wa['success']])
                            }
                        }
                    )
                    
                    execution_records.append({
                        'execution_id': execution_record_id,
                        'workflow_type': assignment['workflow_type'],
                        'success': True
                    })
                    
                    logger.info(f"Created execution record {execution_record_id} for {assignment['workflow_type']} in batch {batch_id}")
                else:
                    logger.error(f"Failed to create execution record for {assignment['workflow_type']}")
                    execution_records.append({
                        'execution_id': None,
                        'workflow_type': assignment['workflow_type'],
                        'success': False,
                        'reason': 'execution_record_creation_failed'
                    })
            else:
                logger.warning(f"Assignment failed for {assignment['workflow_type']}: {assignment.get('reason', 'unknown')}")
                execution_records.append({
                    'execution_id': None,
                    'workflow_type': assignment['workflow_type'],
                    'success': False,
                    'reason': assignment.get('reason', 'assignment_failed')
                })
        
        # Create a summary record for this multi-type batch
        batch_summary = {
            'batch_id': batch_id,
            'postgres_content_id': postgres_content_id,
            'account_id': account_id,
            'link_url': link_url,
            'created_at': datetime.now(timezone.utc),
            'total_assignments': len(workflow_assignments),
            'successful_assignments': len([er for er in execution_records if er['success']]),
            'failed_assignments': len([er for er in execution_records if not er['success']]),
            'execution_records': execution_records,
            'assignment_strategy': 'multi_type_per_link',
            'target_workflow_types': ['replies', 'messages', 'retweets'],
            'uses_workflow_copies': False,
            'workflow_modification_method': 'direct_update'
        }
        
        # Store the batch summary
        result = mongo_db.multi_type_execution_batches.insert_one(batch_summary)
        
        logger.info(f"Created multi-type execution batch {batch_id} with {batch_summary['successful_assignments']}/{batch_summary['total_assignments']} successful assignments for content {postgres_content_id}")
        
        return {
            'batch_id': batch_id,
            'batch_summary_id': str(result.inserted_id),
            'execution_records': execution_records,
            'success': batch_summary['successful_assignments'] > 0
        }
        
    except Exception as e:
        logger.error(f"Error creating multi-type execution batch: {e}")
        return {
            'batch_id': None,
            'batch_summary_id': None,
            'execution_records': [],
            'success': False,
            'error': str(e)
        }


def update_execution_status(mongo_db, execution_id, status, error_message=None, execution_time=None):
    """Update the status of a workflow execution"""
    try:
        update_data = {
            "status": status,
            "last_updated": datetime.now(timezone.utc)
        }
        
        if status == "queued":
            update_data["queued_at"] = datetime.now(timezone.utc)
        elif status == "running":
            update_data["started_at"] = datetime.now(timezone.utc)
        elif status in ["completed", "failed"]:
            update_data["completed_at"] = datetime.now(timezone.utc)
            update_data["executed"] = True
            update_data["success"] = (status == "completed")
            
            if execution_time is not None:
                update_data["actual_execution_time"] = execution_time
                
        if error_message:
            update_data["error_message"] = error_message
            update_data["last_error_message"] = error_message
            update_data["last_error_timestamp"] = datetime.now(timezone.utc)
            # Increment attempt counter
            mongo_db.workflow_executions.update_one(
                {"_id": ObjectId(execution_id)},
                {"$inc": {"execution_attempts": 1}}
            )
            
        result = mongo_db.workflow_executions.update_one(
            {"_id": ObjectId(execution_id)},
            {"$set": update_data}
        )
        
        if result.modified_count > 0:
            logger.info(f"Updated execution {execution_id} status to {status}")
            
            # Update batch summary if this execution is part of a batch
            update_batch_summary_on_status_change(mongo_db, execution_id, status)
            
            return True
        else:
            logger.warning(f"No execution record found for {execution_id}")
            return False
            
    except Exception as e:
        logger.error(f"Error updating execution status for {execution_id}: {e}")
        return False


def update_batch_summary_on_status_change(mongo_db, execution_id, new_status):
    """Update batch summary when an individual execution status changes"""
    try:
        # Find the execution record to get batch information
        execution = mongo_db.workflow_executions.find_one(
            {"_id": ObjectId(execution_id)},
            {"multi_type_batch_id": 1, "workflow_type": 1, "postgres_content_id": 1}
        )
        
        if not execution or "multi_type_batch_id" not in execution:
            return  # Not part of a batch
        
        batch_id = execution["multi_type_batch_id"]
        
        # Get all executions in this batch
        batch_executions = list(mongo_db.workflow_executions.find(
            {"multi_type_batch_id": batch_id},
            {"status": 1, "workflow_type": 1, "success": 1}
        ))
        
        # Calculate batch statistics
        total_in_batch = len(batch_executions)
        completed_count = len([ex for ex in batch_executions if ex.get("status") == "completed"])
        failed_count = len([ex for ex in batch_executions if ex.get("status") == "failed"])
        running_count = len([ex for ex in batch_executions if ex.get("status") == "running"])
        successful_count = len([ex for ex in batch_executions if ex.get("success") == True])
        
        # Determine batch status
        if completed_count + failed_count == total_in_batch:
            batch_status = "all_completed"
        elif running_count > 0:
            batch_status = "some_running"
        else:
            batch_status = "pending"
        
        # Update batch summary
        batch_update = {
            "last_updated": datetime.now(timezone.utc),
            "batch_status": batch_status,
            "completed_executions": completed_count,
            "failed_executions": failed_count,
            "successful_executions": successful_count,
            "completion_rate": (completed_count + failed_count) / total_in_batch if total_in_batch > 0 else 0,
            "success_rate": successful_count / total_in_batch if total_in_batch > 0 else 0
        }
        
        if batch_status == "all_completed":
            batch_update["all_completed_at"] = datetime.now(timezone.utc)
        
        result = mongo_db.multi_type_execution_batches.update_one(
            {"batch_id": batch_id},
            {"$set": batch_update}
        )
        
        if result.modified_count > 0:
            logger.debug(f"Updated batch {batch_id} summary: {batch_status} ({successful_count}/{total_in_batch} successful)")
        
    except Exception as e:
        logger.error(f"Error updating batch summary for execution {execution_id}: {e}")


def get_execution_statistics(mongo_db, include_batch_stats=True):
    """Get comprehensive statistics about workflow executions"""
    try:
        stats = {}
        
        # Overall execution statistics
        pipeline = [
            {
                "$group": {
                    "_id": None,
                    "total_executions": {"$sum": 1},
                    "multi_type_executions": {"$sum": {"$cond": ["$is_part_of_multi_assignment", 1, 0]}},
                    "single_type_executions": {"$sum": {"$cond": [{"$ne": ["$is_part_of_multi_assignment", True]}, 1, 0]}},
                    "with_links": {"$sum": {"$cond": ["$has_link", 1, 0]}},
                    "executed": {"$sum": {"$cond": ["$executed", 1, 0]}},
                    "successful": {"$sum": {"$cond": ["$success", 1, 0]}},
                    "avg_generation_time": {"$avg": "$generation_time"},
                    "avg_execution_time": {"$avg": "$actual_execution_time"}
                }
            }
        ]
        
        overall = list(mongo_db.workflow_executions.aggregate(pipeline))
        if overall:
            stats["overall"] = overall[0]
        
        # Status distribution by assignment strategy
        strategy_status_pipeline = [
            {
                "$group": {
                    "_id": {
                        "assignment_strategy": "$assignment_strategy",
                        "status": "$status"
                    },
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"_id.assignment_strategy": 1, "_id.status": 1}}
        ]
        
        strategy_status_dist = list(mongo_db.workflow_executions.aggregate(strategy_status_pipeline))
        stats["status_by_strategy"] = {}
        for item in strategy_status_dist:
            strategy = item["_id"].get("assignment_strategy", "unknown")
            status = item["_id"]["status"]
            if strategy not in stats["status_by_strategy"]:
                stats["status_by_strategy"][strategy] = {}
            stats["status_by_strategy"][strategy][status] = item["count"]
        
        # Multi-type assignment specific statistics by account
        multi_type_pipeline = [
            {"$match": {"is_part_of_multi_assignment": True}},
            {
                "$group": {
                    "_id": {
                        "account_id": "$account_id",
                        "workflow_type": "$workflow_type",
                        "status": "$status"
                    },
                    "count": {"$sum": 1}
                }
            }
        ]
        
        multi_type_dist = list(mongo_db.workflow_executions.aggregate(multi_type_pipeline))
        stats["multi_type_by_account_and_type"] = {}
        for item in multi_type_dist:
            account_id = ensure_integer_account_id(item["_id"]["account_id"])
            workflow_type = item["_id"]["workflow_type"]
            status = item["_id"]["status"]
            count = item["count"]
            
            if account_id not in stats["multi_type_by_account_and_type"]:
                stats["multi_type_by_account_and_type"][account_id] = {}
            if workflow_type not in stats["multi_type_by_account_and_type"][account_id]:
                stats["multi_type_by_account_and_type"][account_id][workflow_type] = {}
            stats["multi_type_by_account_and_type"][account_id][workflow_type][status] = count
        
        # Batch statistics if requested
        if include_batch_stats:
            try:
                batch_stats = get_batch_statistics(mongo_db)
                stats["batch_statistics"] = batch_stats
            except Exception as e:
                logger.warning(f"Could not get batch statistics: {e}")
                stats["batch_statistics"] = {}
        
        # Recent executions (last 24 hours)
        recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        recent_count = mongo_db.workflow_executions.count_documents({
            "generated_at": {"$gte": recent_cutoff}
        })
        stats["recent_24h"] = recent_count
        
        # Schema version distribution
        schema_versions = list(mongo_db.workflow_executions.aggregate([
            {"$group": {"_id": "$schema_version", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]))
        stats["schema_versions"] = {item["_id"]: item["count"] for item in schema_versions}
        
        logger.info(f"Generated comprehensive execution statistics")
        return stats
        
    except Exception as e:
        logger.error(f"Error getting execution statistics: {e}")
        return {}


def get_batch_statistics(mongo_db):
    """Get statistics about multi-type execution batches"""
    try:
        batch_stats = {}
        
        # Overall batch statistics
        total_batches = mongo_db.multi_type_execution_batches.count_documents({})
        batch_stats["total_batches"] = total_batches
        
        if total_batches == 0:
            return batch_stats
        
        # Batch completion statistics
        completion_pipeline = [
            {
                "$group": {
                    "_id": "$batch_status",
                    "count": {"$sum": 1},
                    "avg_success_rate": {"$avg": "$success_rate"},
                    "avg_completion_rate": {"$avg": "$completion_rate"}
                }
            }
        ]
        
        completion_stats = list(mongo_db.multi_type_execution_batches.aggregate(completion_pipeline))
        batch_stats["by_status"] = {item["_id"]: {
            "count": item["count"],
            "avg_success_rate": item["avg_success_rate"],
            "avg_completion_rate": item["avg_completion_rate"]
        } for item in completion_stats}
        
        # Account-level batch statistics
        account_pipeline = [
            {
                "$group": {
                    "_id": "$account_id",
                    "batch_count": {"$sum": 1},
                    "avg_successful_assignments": {"$avg": "$successful_assignments"},
                    "total_successful_assignments": {"$sum": "$successful_assignments"},
                    "total_failed_assignments": {"$sum": "$failed_assignments"}
                }
            }
        ]
        
        account_stats = list(mongo_db.multi_type_execution_batches.aggregate(account_pipeline))
        batch_stats["by_account"] = {}
        for item in account_stats:
            account_id = ensure_integer_account_id(item["_id"])
            batch_stats["by_account"][account_id] = {
                "batch_count": item["batch_count"],
                "avg_successful_assignments": item["avg_successful_assignments"],
                "total_successful": item["total_successful_assignments"],
                "total_failed": item["total_failed_assignments"],
                "overall_success_rate": item["total_successful_assignments"] / (item["total_successful_assignments"] + item["total_failed_assignments"]) if (item["total_successful_assignments"] + item["total_failed_assignments"]) > 0 else 0
            }
        
        # Recent batch activity
        recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        recent_batches = mongo_db.multi_type_execution_batches.count_documents({
            "created_at": {"$gte": recent_cutoff}
        })
        batch_stats["recent_24h"] = recent_batches
        
        return batch_stats
        
    except Exception as e:
        logger.error(f"Error getting batch statistics: {e}")
        return {}


def get_multi_type_assignment_health_check(mongo_db):
    """Perform a health check on multi-type assignments to identify issues"""
    try:
        health_report = {
            "overall_health": "unknown",
            "issues": [],
            "recommendations": [],
            "statistics": {}
        }
        
        # Check 1: Verify each content has assignments for all 3 workflow types
        content_type_distribution = list(mongo_db.content_workflow_links.aggregate([
            {
                "$group": {
                    "_id": {
                        "postgres_content_id": "$postgres_content_id",
                        "account_id": "$account_id"
                    },
                    "workflow_types": {"$addToSet": "$content_type"},
                    "assignment_count": {"$sum": 1}
                }
            }
        ]))
        
        incomplete_assignments = 0
        complete_assignments = 0
        
        for item in content_type_distribution:
            types_count = len(item["workflow_types"])
            if types_count < 3:
                incomplete_assignments += 1
            else:
                complete_assignments += 1
        
        health_report["statistics"]["complete_multi_type_assignments"] = complete_assignments
        health_report["statistics"]["incomplete_multi_type_assignments"] = incomplete_assignments
        
        if incomplete_assignments > complete_assignments * 0.1:  # More than 10% incomplete
            health_report["issues"].append(f"High number of incomplete multi-type assignments: {incomplete_assignments}")
            health_report["recommendations"].append("Review the assign_link_to_all_workflow_types function for failures")
        
        # Check 2: Verify workflows are being updated properly
        total_executions = mongo_db.workflow_executions.count_documents({"is_part_of_multi_assignment": True})
        executions_with_workflows = mongo_db.workflow_executions.count_documents({
            "is_part_of_multi_assignment": True,
            "automa_workflow_id": {"$ne": None}
        })
        
        workflow_linkage_rate = executions_with_workflows / total_executions if total_executions > 0 else 0
        health_report["statistics"]["workflow_linkage_rate"] = workflow_linkage_rate
        
        if workflow_linkage_rate < 0.95:  # Less than 95% linked
            health_report["issues"].append(f"Low workflow linkage rate: {workflow_linkage_rate:.2%}")
            health_report["recommendations"].append("Check workflow assignment in update_workflow_with_link")
        
        # Check 3: Verify batch tracking is working
        recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        recent_executions = mongo_db.workflow_executions.count_documents({
            "is_part_of_multi_assignment": True,
            "generated_at": {"$gte": recent_cutoff}
        })
        executions_with_batches = mongo_db.workflow_executions.count_documents({
            "is_part_of_multi_assignment": True,
            "generated_at": {"$gte": recent_cutoff},
            "multi_type_batch_id": {"$ne": None}
        })
        
        batch_tracking_rate = executions_with_batches / recent_executions if recent_executions > 0 else 0
        health_report["statistics"]["batch_tracking_rate"] = batch_tracking_rate
        
        if batch_tracking_rate < 0.9:  # Less than 90% have batch IDs
            health_report["issues"].append(f"Low batch tracking rate: {batch_tracking_rate:.2%}")
            health_report["recommendations"].append("Check create_multi_type_execution_batch function")
        
        # Check 4: Verify workflows are being reset properly
        workflows_with_content = mongo_db.automa_workflows.count_documents({"has_content": True})
        workflows_with_links = mongo_db.automa_workflows.count_documents({"has_real_link": True})
        
        health_report["statistics"]["workflows_available"] = workflows_with_content
        health_report["statistics"]["workflows_in_use"] = workflows_with_links
        
        if workflows_with_content == 0:
            health_report["issues"].append("No workflows available with has_content=True")
            health_report["recommendations"].append("Check workflow reset mechanism or create new workflows")
        
        # Overall health determination
        issue_count = len(health_report["issues"])
        if issue_count == 0:
            health_report["overall_health"] = "healthy"
        elif issue_count <= 2:
            health_report["overall_health"] = "warning"
        else:
            health_report["overall_health"] = "critical"
        
        health_report["check_timestamp"] = datetime.now(timezone.utc)
        health_report["total_issues_found"] = issue_count
        
        logger.info(f"Multi-type assignment health check complete: {health_report['overall_health']} ({issue_count} issues)")
        
        return health_report
        
    except Exception as e:
        logger.error(f"Error performing multi-type assignment health check: {e}")
        return {
            "overall_health": "error",
            "issues": [f"Health check failed: {str(e)}"],
            "recommendations": ["Check logs for health check errors"],
            "statistics": {}
        }


def create_execution_tracking_record(mongo_db, execution_id, strategy, dag_id, dag_run_id, batch_id=None):
    """Create a tracking record for workflow execution in Airflow"""
    try:
        tracking_record = {
            "execution_id": execution_id,
            "strategy": strategy,
            "batch_id": batch_id,
            "execution_timestamp": datetime.now(timezone.utc),
            "status": "initiated",
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "created_at": datetime.now(timezone.utc),
            "schema_version": "multi_type_v2_no_copies",
            "supports_multi_type": True,
            "uses_workflow_copies": False
        }
        
        result = mongo_db.workflow_execution_tracking.insert_one(tracking_record)
        logger.info(f"Created execution tracking record: {result.inserted_id}")
        
        return str(result.inserted_id)
        
    except Exception as e:
        logger.error(f"Error creating execution tracking record: {e}")
        return None


def cleanup_old_execution_records(mongo_db, days_to_keep=30):
    """Clean up old execution records and related data"""
    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        total_cleaned = 0
        
        # Clean up workflow_executions
        result1 = mongo_db.workflow_executions.delete_many({
            "generated_at": {"$lt": cutoff_date},
            "status": {"$in": ["completed", "failed"]}
        })
        total_cleaned += result1.deleted_count
        logger.info(f"Deleted {result1.deleted_count} old workflow execution records")
        
        # Clean up workflow_execution_tracking
        result2 = mongo_db.workflow_execution_tracking.delete_many({
            "execution_timestamp": {"$lt": cutoff_date}
        })
        total_cleaned += result2.deleted_count
        logger.info(f"Deleted {result2.deleted_count} old execution tracking records")
        
        # Clean up old content workflow links for completed/failed executions
        old_completed_executions = list(mongo_db.workflow_executions.find({
            "completed_at": {"$lt": cutoff_date},
            "status": {"$in": ["completed", "failed"]}
        }, {"content_link_id": 1}))
        
        old_content_link_ids = [exec_rec["content_link_id"] for exec_rec in old_completed_executions if "content_link_id" in exec_rec and exec_rec["content_link_id"]]
        
        if old_content_link_ids:
            result3 = mongo_db.content_workflow_links.delete_many({
                "_id": {"$in": old_content_link_ids}
            })
            total_cleaned += result3.deleted_count
            logger.info(f"Deleted {result3.deleted_count} old content workflow links")
        
        # Clean up old multi-type execution batches
        result4 = mongo_db.multi_type_execution_batches.delete_many({
            "created_at": {"$lt": cutoff_date},
            "batch_status": {"$in": ["all_completed"]}
        })
        total_cleaned += result4.deleted_count
        logger.info(f"Deleted {result4.deleted_count} old execution batches")
        
        logger.info(f"Cleanup complete - Total {total_cleaned} records deleted older than {days_to_keep} days")
        
        return total_cleaned
        
    except Exception as e:
        logger.error(f"Error cleaning up old execution records: {e}")
        return 0


def reset_workflows_after_execution(mongo_db, workflow_ids=None, account_id=None):
    """
    Reset workflows back to has_content=True state after execution completes
    This makes them available for new assignments
    
    Args:
        workflow_ids: List of workflow IDs to reset (optional)
        account_id: Only reset workflows for specific account (optional)
    """
    try:
        # Ensure account_id is integer if provided
        if account_id is not None:
            account_id = ensure_integer_account_id(account_id)
        
        # Build query
        query = {"has_real_link": True}
        
        if workflow_ids:
            query["_id"] = {"$in": [ObjectId(wid) for wid in workflow_ids]}
        
        if account_id is not None:
            query["account_id"] = account_id
        
        # Find workflows that need resetting
        workflows_to_reset = list(mongo_db.automa_workflows.find(query))
        
        if not workflows_to_reset:
            logger.info("No workflows found to reset")
            return 0
        
        reset_count = 0
        for workflow in workflows_to_reset:
            # Reset workflow flags
            result = mongo_db.automa_workflows.update_one(
                {"_id": workflow["_id"]},
                {
                    "$set": {
                        "has_content": True,
                        "has_real_link": False,
                        "reset_at": datetime.now(timezone.utc)
                    },
                    "$unset": {
                        "assigned_link": "",
                        "assigned_content_id": "",
                        "assignment_time": "",
                        "link_assigned": ""
                    }
                }
            )
            
            if result.modified_count > 0:
                reset_count += 1
                logger.info(f"Reset workflow {workflow['name']} (ID: {workflow['_id']})")
        
        logger.info(f"Successfully reset {reset_count} workflows back to has_content=True")
        return reset_count
        
    except Exception as e:
        logger.error(f"Error resetting workflows after execution: {e}")
        return 0


def get_workflow_execution_summary(mongo_db, postgres_content_id=None, account_id=None):
    """
    Get a comprehensive summary of workflow executions
    
    Args:
        postgres_content_id: Filter by specific content ID (optional)
        account_id: Filter by specific account (optional)
    """
    try:
        # Ensure account_id is integer if provided
        if account_id is not None:
            account_id = ensure_integer_account_id(account_id)
        
        # Build match filter
        match_filter = {}
        if postgres_content_id:
            match_filter["postgres_content_id"] = postgres_content_id
        if account_id is not None:
            match_filter["account_id"] = account_id
        
        # Get execution summary by content and workflow type
        pipeline = [
            {"$match": match_filter} if match_filter else {"$match": {}},
            {
                "$group": {
                    "_id": {
                        "postgres_content_id": "$postgres_content_id",
                        "account_id": "$account_id",
                        "workflow_type": "$workflow_type"
                    },
                    "execution_count": {"$sum": 1},
                    "successful_executions": {"$sum": {"$cond": ["$success", 1, 0]}},
                    "failed_executions": {"$sum": {"$cond": [{"$eq": ["$status", "failed"]}, 1, 0]}},
                    "pending_executions": {"$sum": {"$cond": [{"$eq": ["$status", "generated"]}, 1, 0]}},
                    "avg_execution_time": {"$avg": "$actual_execution_time"},
                    "latest_execution": {"$max": "$generated_at"}
                }
            },
            {"$sort": {"_id.postgres_content_id": 1, "_id.workflow_type": 1}}
        ]
        
        summary = list(mongo_db.workflow_executions.aggregate(pipeline))
        
        # Format the results
        formatted_summary = []
        for item in summary:
            formatted_summary.append({
                "postgres_content_id": item["_id"]["postgres_content_id"],
                "account_id": ensure_integer_account_id(item["_id"]["account_id"]),
                "workflow_type": item["_id"]["workflow_type"],
                "execution_count": item["execution_count"],
                "successful_executions": item["successful_executions"],
                "failed_executions": item["failed_executions"],
                "pending_executions": item["pending_executions"],
                "success_rate": item["successful_executions"] / item["execution_count"] if item["execution_count"] > 0 else 0,
                "avg_execution_time": item["avg_execution_time"],
                "latest_execution": item["latest_execution"]
            })
        
        logger.info(f"Generated workflow execution summary with {len(formatted_summary)} entries")
        return formatted_summary
        
    except Exception as e:
        logger.error(f"Error getting workflow execution summary: {e}")
        return []


def get_pending_executions(mongo_db, account_id=None, workflow_type=None, limit=100):
    """
    Get pending workflow executions that are ready to be executed
    
    Args:
        account_id: Filter by specific account (optional)
        workflow_type: Filter by workflow type (optional)
        limit: Maximum number of results (default: 100)
    """
    try:
        # Ensure account_id is integer if provided
        if account_id is not None:
            account_id = ensure_integer_account_id(account_id)
        
        # Build query
        query = {
            "status": {"$in": ["generated", "queued"]},
            "executed": False
        }
        
        if account_id is not None:
            query["account_id"] = account_id
        
        if workflow_type:
            query["workflow_type"] = workflow_type
        
        # Get pending executions
        pending = list(mongo_db.workflow_executions.find(
            query,
            {
                "_id": 1,
                "automa_workflow_id": 1,
                "workflow_type": 1,
                "postgres_content_id": 1,
                "account_id": 1,
                "link_url": 1,
                "generated_at": 1,
                "status": 1
            }
        ).sort("generated_at", 1).limit(limit))
        
        logger.info(f"Found {len(pending)} pending executions")
        return pending
        
    except Exception as e:
        logger.error(f"Error getting pending executions: {e}")
        return []


def mark_execution_as_queued(mongo_db, execution_id):
    """Mark an execution as queued for processing"""
    try:
        result = mongo_db.workflow_executions.update_one(
            {"_id": ObjectId(execution_id)},
            {
                "$set": {
                    "status": "queued",
                    "queued_at": datetime.now(timezone.utc)
                }
            }
        )
        
        if result.modified_count > 0:
            logger.info(f"Marked execution {execution_id} as queued")
            return True
        return False
        
    except Exception as e:
        logger.error(f"Error marking execution as queued: {e}")
        return False


def get_execution_details(mongo_db, execution_id):
    """Get detailed information about a specific execution"""
    try:
        execution = mongo_db.workflow_executions.find_one({"_id": ObjectId(execution_id)})
        
        if not execution:
            logger.warning(f"Execution {execution_id} not found")
            return None
        
        # Convert ObjectId to string for JSON serialization
        execution["_id"] = str(execution["_id"])
        if execution.get("automa_workflow_id"):
            execution["automa_workflow_id"] = str(execution["automa_workflow_id"])
        if execution.get("content_link_id"):
            execution["content_link_id"] = str(execution["content_link_id"])
        
        return execution
        
    except Exception as e:
        logger.error(f"Error getting execution details: {e}")
        return None


def get_multi_type_assignment_summary(mongo_db, postgres_content_id=None):
    """
    Get a summary of multi-type assignments for specific content or all content
    """
    try:
        match_filter = {}
        if postgres_content_id:
            match_filter["postgres_content_id"] = postgres_content_id
        
        # Get assignments by content ID and account
        pipeline = [
            {"$match": match_filter} if match_filter else {"$match": {}},
            {
                "$group": {
                    "_id": {
                        "postgres_content_id": "$postgres_content_id",
                        "account_id": "$account_id"
                    },
                    "workflow_types_assigned": {"$addToSet": "$content_type"},
                    "total_assignments": {"$sum": 1},
                    "links_assigned": {"$addToSet": "$link_url"},
                    "assignment_methods": {"$addToSet": "$assignment_method"},
                    "created_dates": {"$push": "$linked_at"}
                }
            },
            {"$sort": {"_id.postgres_content_id": 1}}
        ]
        
        assignments_summary = list(mongo_db.content_workflow_links.aggregate(pipeline))
        
        # Calculate statistics
        summary_stats = {
            'total_content_items': len(assignments_summary),
            'fully_assigned_items': 0,
            'partially_assigned_items': 0,
            'assignment_distribution': {},
            'account_distribution': {},
            'method_distribution': {}
        }
        
        for item in assignments_summary:
            account_id = ensure_integer_account_id(item['_id']['account_id'])
            types_count = len(item['workflow_types_assigned'])
            
            # Count full vs partial assignments
            if types_count == 3:
                summary_stats['fully_assigned_items'] += 1
            else:
                summary_stats['partially_assigned_items'] += 1
            
            # Distribution by number of types
            if types_count not in summary_stats['assignment_distribution']:
                summary_stats['assignment_distribution'][types_count] = 0
            summary_stats['assignment_distribution'][types_count] += 1
            
            # Distribution by account
            if account_id not in summary_stats['account_distribution']:
                summary_stats['account_distribution'][account_id] = 0
            summary_stats['account_distribution'][account_id] += 1
            
            # Distribution by method
            for method in item['assignment_methods']:
                if method not in summary_stats['method_distribution']:
                    summary_stats['method_distribution'][method] = 0
                summary_stats['method_distribution'][method] += 1
        
        result = {
            'summary_stats': summary_stats,
            'detailed_assignments': assignments_summary
        }
        
        if postgres_content_id:
            logger.info(f"Multi-type assignment summary for content {postgres_content_id}: {summary_stats}")
        else:
            logger.info(f"Overall multi-type assignment summary: {summary_stats}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting multi-type assignment summary: {e}")
        return {'summary_stats': {}, 'detailed_assignments': []}


# MongoDB URI and configuration
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://app_user:app_password@mongodb:27017/messages_db')

# Default arguments for the DAG
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}