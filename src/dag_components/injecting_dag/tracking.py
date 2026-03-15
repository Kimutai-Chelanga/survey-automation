import os
import logging
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from .db_utils import get_system_setting, get_mongo_db
from bson import ObjectId

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@retry(
    stop=stop_after_attempt(3), 
    wait=wait_fixed(2),
    retry=retry_if_exception_type((ConnectionError, TimeoutError, Exception))
)
def update_execution_tracking(workflow_data, execution_success, error_message=None):
    """Update execution tracking for individual workflow with enhanced error handling"""
    try:
        if not workflow_data or 'mongoDoc' not in workflow_data:
            logger.warning("Invalid workflow_data provided for tracking update")
            return False
        
        mongo_doc = workflow_data['mongoDoc']
        collection_name = workflow_data.get('collectionName')
        workflow_type = workflow_data.get('workflowType')
        workflow_name = mongo_doc.get('name', 'Unknown')
        
        if not collection_name:
            logger.error(f"No collection name provided for workflow: {workflow_name}")
            return False
        
        # Get MongoDB connection
        db, client = get_mongo_db()
        collection = db[collection_name]
        
        # Prepare update data
        update_data = {
            'tracking_updated_at': datetime.now(),
            'execution_success': execution_success,
            'execution_attempts': mongo_doc.get('execution_attempts', 0) + 1,
            'last_execution_status': 'success' if execution_success else 'failed'
        }
        
        if error_message:
            update_data['last_error_message'] = str(error_message)
            update_data['last_error_timestamp'] = datetime.now()
        
        if execution_success:
            update_data['execution_completed_at'] = datetime.now()
            update_data['execution_status'] = 'completed_successfully'
        else:
            update_data['execution_status'] = 'failed_injection'
        
        # Update the document
        update_result = collection.update_one(
            {'_id': ObjectId(mongo_doc['_id'])},
            {'$set': update_data}
        )
        
        if update_result.modified_count > 0:
            logger.info(f"✅ Updated tracking for workflow: {workflow_name} (Success: {execution_success})")
            client.close()
            return True
        else:
            logger.warning(f"⚠️ No document updated for workflow: {workflow_name}")
            client.close()
            return False
            
    except Exception as e:
        logger.error(f"❌ Failed to update tracking for workflow: {e}")
        if 'client' in locals():
            client.close()
        raise

@retry(
    stop=stop_after_attempt(3), 
    wait=wait_fixed(2)
)
def update_batch_execution_tracking(workflows_batch, results_batch):
    """Update execution tracking for a batch of workflows"""
    try:
        tracking_results = {
            'workflows_updated': 0, 
            'errors': [],
            'successful_updates': [],
            'failed_updates': []
        }
        
        if len(workflows_batch) != len(results_batch):
            logger.error("Mismatch between workflows and results batch sizes")
            return tracking_results
        
        for workflow_data, (success, error_msg) in zip(workflows_batch, results_batch):
            try:
                update_success = update_execution_tracking(workflow_data, success, error_msg)
                if update_success:
                    tracking_results['workflows_updated'] += 1
                    tracking_results['successful_updates'].append(workflow_data['automaWf']['name'])
                else:
                    tracking_results['failed_updates'].append(workflow_data['automaWf']['name'])
            except Exception as e:
                error_info = {
                    'workflow_name': workflow_data.get('automaWf', {}).get('name', 'Unknown'),
                    'error': str(e)
                }
                tracking_results['errors'].append(error_info)
                tracking_results['failed_updates'].append(error_info['workflow_name'])
        
        logger.info(f"📊 Batch tracking update complete: {tracking_results['workflows_updated']} successful, {len(tracking_results['errors'])} errors")
        return tracking_results
        
    except Exception as e:
        logger.error(f"❌ Failed to update batch execution tracking: {e}")
        return {'workflows_updated': 0, 'errors': [{'error': str(e)}]}

def get_workflow_execution_stats(collection_name, time_range_hours=24):
    """Get execution statistics for workflows in a collection"""
    try:
        db, client = get_mongo_db()
        collection = db[collection_name]
        
        from datetime import timedelta
        cutoff_time = datetime.now() - timedelta(hours=time_range_hours)
        
        # Get stats for recent executions
        pipeline = [
            {
                '$match': {
                    'tracking_updated_at': {'$gte': cutoff_time}
                }
            },
            {
                '$group': {
                    '_id': '$execution_success',
                    'count': {'$sum': 1},
                    'workflows': {'$push': '$name'}
                }
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        
        stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'success_rate': 0.0,
            'time_range_hours': time_range_hours,
            'collection': collection_name
        }
        
        for result in results:
            count = result['count']
            stats['total_processed'] += count
            
            if result['_id'] is True:  # execution_success = True
                stats['successful'] = count
            elif result['_id'] is False:  # execution_success = False
                stats['failed'] = count
        
        if stats['total_processed'] > 0:
            stats['success_rate'] = round((stats['successful'] / stats['total_processed']) * 100, 2)
        
        client.close()
        return stats
        
    except Exception as e:
        logger.error(f"❌ Failed to get execution stats for {collection_name}: {e}")
        if 'client' in locals():
            client.close()
        return {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'success_rate': 0.0,
            'error': str(e)
        }

def cleanup_old_tracking_data(collection_name, days_to_keep=30):
    """Clean up old tracking data to prevent database bloat"""
    try:
        db, client = get_mongo_db()
        collection = db[collection_name]
        
        from datetime import timedelta
        cutoff_time = datetime.now() - timedelta(days=days_to_keep)
        
        # Remove old tracking fields from documents
        update_result = collection.update_many(
            {'tracking_updated_at': {'$lt': cutoff_time}},
            {
                '$unset': {
                    'tracking_updated_at': '',
                    'execution_attempts': '',
                    'last_error_message': '',
                    'last_error_timestamp': '',
                    'execution_completed_at': ''
                }
            }
        )
        
        logger.info(f"🧹 Cleaned up tracking data for {update_result.modified_count} documents in {collection_name}")
        client.close()
        return update_result.modified_count
        
    except Exception as e:
        logger.error(f"❌ Failed to cleanup tracking data for {collection_name}: {e}")
        if 'client' in locals():
            client.close()
        return 0