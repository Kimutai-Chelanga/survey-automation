from bson import ObjectId
from datetime import datetime

import psycopg2
from .config import WORKFLOW_COLLECTIONS, WorkflowStrategy
from .db_utils import get_mongo_db, get_postgres_connection
from psycopg2.extras import RealDictCursor

def fetch_workflows_by_strategy(strategy, selected_types, type_order):
    """Fetch workflows based on strategy, grouping by link for multiple strategy"""
    print(f'🎯 Executing strategy: {strategy}')
    
    mongo_db, mongo_client = get_mongo_db()
    all_workflows = []
    execution_stats = {'strategy': strategy, 'collections_processed': {}}
    
    try:
        # Validate selected_types and type_order
        valid_types = list(WORKFLOW_COLLECTIONS.keys())
        selected_types = [t for t in selected_types if t in valid_types]
        type_order = [t for t in type_order if t in selected_types]
        
        if not selected_types:
            print("⚠️ No valid workflow types selected")
            return all_workflows, execution_stats
            
        print(f"📋 Processing types: {selected_types} in order: {type_order}")
        
        # Use context manager for PostgreSQL connection
        with get_postgres_connection() as pg_conn:
            if strategy == WorkflowStrategy.MULTIPLE.value and len(selected_types) > 1:
                print("🔗 Using link-based grouping for multiple workflow types")
                all_workflows, execution_stats = _fetch_workflows_by_link_groups(
                    mongo_db, pg_conn, selected_types, type_order, execution_stats
                )
            else:
                print("📋 Using single collection strategy")
                all_workflows, execution_stats = _fetch_workflows_single(
                    mongo_db, selected_types, type_order, execution_stats
                )
        
        print(f'\n📊 STRATEGY EXECUTION SUMMARY:')
        print(f'   - Strategy: {strategy}')
        print(f'   - Types processed: {selected_types}')
        print(f'   - Total workflows ready: {len(all_workflows)}')
        for workflow_type, stats in execution_stats['collections_processed'].items():
            print(f'   - {workflow_type}: {stats["succeeded"]}/{stats["found"]} succeeded')
        
    except Exception as e:
        print(f'❌ Failed to fetch workflows: {e}')
        execution_stats = {'strategy': strategy, 'error': str(e)}
    finally:
        mongo_client.close()
    
    return all_workflows, execution_stats



def _get_link_workflow_mapping(db, pg_conn, selected_types):
    """Get mapping of links to workflows from MongoDB and PostgreSQL"""
    link_groups = {}
    execution_stats = {'collections_processed': {}}  # Initialize execution_stats here
    
    try:
        pg_cursor = pg_conn.cursor(cursor_factory=RealDictCursor)
        
        for workflow_type in selected_types:
            collection_config = WORKFLOW_COLLECTIONS[workflow_type]
            collection = db[collection_config['name']]
            
            workflows = collection.find({
                '$and': [
                    {'has_link': True},
                    {'executed': {'$in': [False, None]}},
                    {'has_content': True},
                    {'postgres_content_id': {'$exists': True, '$ne': None}}
                ]
            })
            
            # Initialize stats for this workflow_type
            execution_stats['collections_processed'][workflow_type] = {
                'found': 0, 'processed': 0, 'succeeded': 0, 'failed': 0
            }
            
            # Count workflows
            workflow_count = 0
            for _ in workflows:  # Count documents
                workflow_count += 1
            workflows.rewind()  # Reset cursor for actual processing
            
            execution_stats['collections_processed'][workflow_type]['found'] = workflow_count
            
            for workflow in workflows:
                content_id = workflow.get('postgres_content_id')
                
                if not content_id:
                    print(f"Warning: Workflow {workflow.get('name')} in {collection_config['name']} missing postgres_content_id")
                    execution_stats['collections_processed'][workflow_type]['failed'] += 1
                    continue
                
                try:
                    pg_cursor.execute(
                        "SELECT link FROM links WHERE links_id = %s",
                        (content_id,)
                    )
                    result = pg_cursor.fetchone()
                    link = result['link'] if result else None
                    
                    if link:
                        if link not in link_groups:
                            link_groups[link] = {
                                'link': link,
                                'workflows': [],
                                'content_ids': set()
                            }
                        
                        link_groups[link]['workflows'].append({
                            'workflow_id': str(workflow['_id']),
                            'name': workflow.get('name'),
                            'type': workflow_type,
                            'collection': collection_config['name'],
                            'content_id': content_id,
                            'created_time': workflow.get('created_time', datetime.now())
                        })
                        link_groups[link]['content_ids'].add(content_id)
                        execution_stats['collections_processed'][workflow_type]['succeeded'] += 1
                    else:
                        print(f"Warning: No link found in PostgreSQL for links_id {content_id}")
                        execution_stats['collections_processed'][workflow_type]['failed'] += 1
                
                except psycopg2.Error as e:
                    print(f"PostgreSQL query error for content_id {content_id}: {e}")
                    execution_stats['collections_processed'][workflow_type]['failed'] += 1
                    continue
        
        return sorted(link_groups.values(), key=lambda x: x['workflows'][0]['created_time']), execution_stats
    
    finally:
        if 'pg_cursor' in locals():
            pg_cursor.close()

def _fetch_workflows_by_link_groups(db, pg_conn, selected_types, type_order, execution_stats):
    """Fetch workflows grouped by links for multiple strategy"""
    all_workflows = []
    
    try:
        link_groups, execution_stats = _get_link_workflow_mapping(db, pg_conn, selected_types)
        print(f"📊 Found {len(link_groups)} link groups")
        
        total_processed = 0
        for link_group in link_groups:
            link = link_group['link']
            workflows_for_link = link_group['workflows']
            
            print(f"🔗 Processing link group: {link} ({len(workflows_for_link)} workflows)")
            
            ordered_workflows = _order_workflows_within_link_group(workflows_for_link, type_order)
            
            for workflow_info in ordered_workflows:
                workflow_data = _fetch_and_process_workflow(
                    db, workflow_info, execution_stats, total_processed + 1, 'multiple'
                )
                
                if workflow_data:
                    workflow_data['automaWf']['_linkGroup'] = link
                    all_workflows.append(workflow_data)
                    total_processed += 1
    
    except Exception as e:
        print(f"❌ Error in link-based grouping: {e}")
        execution_stats['error'] = str(e)
    
    return all_workflows, execution_stats

def _order_workflows_within_link_group(workflows, type_order):
    """Order workflows within a link group based on user-specified type order"""
    ordered_workflows = []
    for workflow_type in type_order:
        for workflow in workflows:
            if workflow['type'] == workflow_type:
                ordered_workflows.append(workflow)
    return ordered_workflows

def _fetch_workflows_single(db, selected_types, type_order, execution_stats):
    """Fetch workflows for single strategy or when link grouping is not needed"""
    all_workflows = []
    
    for workflow_type in type_order:
        collection_config = WORKFLOW_COLLECTIONS[workflow_type]
        collection_name = collection_config['name']
        collection = db[collection_name]
        
        print(f"🔍 Processing {workflow_type} workflows from {collection_name}...")
        
        query = {
            '$and': [
                {'has_link': True},
                {'executed': {'$in': [False, None]}},
                {'has_content': True}
            ]
        }
        
        # Log total documents and matching documents
        total_docs = collection.count_documents({})
        matching_docs = collection.count_documents(query)
        print(f"📊 Total documents in {collection_name}: {total_docs}")
        print(f"📊 Matching unprocessed workflows: {matching_docs}")
        
        if matching_docs == 0:
            # Log documents that fail specific conditions
            has_link_count = collection.count_documents({'has_link': True})
            not_executed_count = collection.count_documents({'executed': {'$in': [False, None]}})
            has_content_count = collection.count_documents({'has_content': True})
            has_postgres_id_count = collection.count_documents({'postgres_content_id': {'$exists': True, '$ne': None}})
            print(f"📋 Diagnostics for {collection_name}:")
            print(f"   - Documents with has_link: True: {has_link_count}")
            print(f"   - Documents with executed: False/None: {not_executed_count}")
            print(f"   - Documents with has_content: True: {has_content_count}")
            print(f"   - Documents with valid postgres_content_id: {has_postgres_id_count}")
        
        workflows = list(collection.find(query).sort('_id', 1))
        
        execution_stats['collections_processed'][workflow_type] = {
            'found': len(workflows),
            'processed': 0,
            'succeeded': 0,
            'failed': 0
        }
        
        print(f"📊 Found {len(workflows)} unprocessed {workflow_type} workflows")
        
        for idx, wf in enumerate(workflows):
            workflow_info = {
                'workflow_id': str(wf['_id']),
                'name': wf.get('name'),
                'type': workflow_type,
                'collection': collection_name,
                'created_time': wf.get('created_time', datetime.now())
            }
            
            workflow_data = _fetch_and_process_workflow(
                db, workflow_info, execution_stats, idx + 1, strategy='single'
            )
            
            if workflow_data:
                all_workflows.append(workflow_data)
    
    return all_workflows, execution_stats

def _fetch_and_process_workflow(db, workflow_info, execution_stats, processing_order, strategy):
    """Fetch and process individual workflow"""
    try:
        collection = db[workflow_info['collection']]
        workflow = collection.find_one({'_id': ObjectId(workflow_info['workflow_id'])})
        
        if not workflow:
            print(f"⚠️ Workflow {workflow_info['workflow_id']} not found")
            return None
        
        workflow_type = workflow_info['type']
        
        update_result = collection.update_one(
            {'_id': workflow['_id']},
            {
                '$set': {
                    'executed': True,
                    'execution_timestamp': datetime.now(),
                    'execution_status': 'picked_for_processing',
                    'execution_strategy': strategy,
                    'processing_order': processing_order
                }
            }
        )
        
        if update_result.modified_count == 0:
            print(f"⚠️ Failed to mark workflow {workflow.get('name')} as executed")
            execution_stats['collections_processed'][workflow_type]['failed'] += 1
            return None
        
        workflow_data = {
            'id': workflow.get('name', f"{workflow_type}_{str(workflow['_id'])}"),
            'name': workflow.get('name', f"{workflow_type.title()} Workflow - {str(workflow['_id'])[:8]}"),
            'description': workflow.get('description', f"Auto-generated {workflow_type} workflow"),
            'version': workflow.get('version', '1.0.0'),
            'drawflow': workflow.get('drawflow', {}),
            'createdAt': int(datetime.now().timestamp() * 1000),
            'updatedAt': int(datetime.now().timestamp() * 1000),
            'isDisabled': False,
            'table': [],
            'globalData': {},
            '_workflowType': workflow_type,
            '_collectionName': workflow_info['collection'],
            '_mongoId': str(workflow['_id']),
            '_executionStrategy': strategy,
            '_processingOrder': processing_order,
            '_contentId': workflow_info.get('content_id'),
        }
        
        execution_stats['collections_processed'][workflow_type]['processed'] += 1
        execution_stats['collections_processed'][workflow_type]['succeeded'] += 1
        
        return {
            'mongoDoc': workflow,
            'automaWf': workflow_data,
            'collectionName': workflow_info['collection'],
            'workflowType': workflow_type
        }
        
    except Exception as e:
        print(f"❌ Error processing workflow {workflow_info.get('name', workflow_info['workflow_id'])}: {e}")
        execution_stats['collections_processed'][workflow_type]['failed'] += 1
        return None