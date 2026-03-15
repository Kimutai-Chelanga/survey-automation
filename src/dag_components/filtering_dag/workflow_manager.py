import datetime
import json
from datetime import timezone
from bson import ObjectId
from .config import logger
from core.database.postgres.connection import get_postgres_connection
from typing import List, Dict, Any
from datetime import datetime, timezone, date
from typing import List, Dict, Any
from bson import ObjectId
import logging

logger = logging.getLogger(__name__)
# Essential helper functions
def ensure_integer_account_id(account_id):
    """Ensure account_id is an integer"""
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
import re
from datetime import datetime, timezone
from typing import List, Dict, Any
from bson import ObjectId

# Assuming these are already imported in your file
# from .config import logger
# from core.database.mongodb.connection import get_mongo_collection
# from .utils import ensure_integer_account_id


def assign_link_to_all_workflow_types(
    mongo_db,
    link_url: str,
    postgres_link_id: int,
    account_id: int,
    available_workflows: List[Dict[str, Any]],
    content_text: str = None,
    tweeted_date=None,
) -> List[Dict[str, Any]]:
    """
    Assigns a link to multiple workflows (multi-type per link), updates drawflow with real URLs,
    sets meaningful screenshot/video filenames, and stores artifact metadata.
    """
    from datetime import date as date_type, datetime as datetime_type

    # Convert date → datetime if needed
    if tweeted_date and isinstance(tweeted_date, date_type) and not isinstance(tweeted_date, datetime_type):
        tweeted_date = datetime_type.combine(tweeted_date, datetime_type.min.time())

    results = []
    account_id = ensure_integer_account_id(account_id)

    # === Get username for artifact naming ===
    username = f"account_{account_id}"
    try:
        account_doc = mongo_db.accounts.find_one({"account_id": account_id})
        if account_doc and account_doc.get("username"):
            raw_username = account_doc["username"]
            username = re.sub(r"\W+", "_", str(raw_username))  # Sanitize for filename
    except Exception as e:
        logger.warning(f"Could not fetch username for account {account_id}: {e}")

    # Timestamp for uniqueness
    now = datetime.now(timezone.utc)
    ts_str = now.strftime("%Y%m%d_%H%M%S")

    workflow_types = {wf["workflow_type"] for wf in available_workflows}

    logger.info(
        f"Starting link assignment for postgres_link_id={postgres_link_id} "
        f"(account_id={account_id}, username={username}) to {len(available_workflows)} workflows"
    )

    for wf in available_workflows:
        wf_id = wf["workflow_id"]
        wf_type = wf["workflow_type"]
        automa_wf_id = wf.get("automa_workflow_id")

        success = False
        reason = None
        content_link_id = None
        metadata_id = None

        try:
            # === Basic validation ===
            has_link = bool(
                link_url
                and isinstance(link_url, str)
                and link_url.strip()
                and ("http://" in link_url or "https://" in link_url)
            )

            if not has_link:
                reason = "invalid_link"
                logger.warning(f"Invalid link for workflow {wf_id}: {link_url}")
                results.append(
                    {
                        "workflow_id": wf_id,
                        "workflow_type": wf_type,
                        "success": False,
                        "reason": reason,
                        "content_link_id": None,
                        "metadata_id": None,
                    }
                )
                continue

            if not automa_wf_id:
                reason = "missing_automa_workflow_id"
                logger.error(f"Missing automa_workflow_id for workflow {wf_id}")
                results.append(
                    {
                        "workflow_id": wf_id,
                        "workflow_type": wf_type,
                        "success": False,
                        "reason": reason,
                        "content_link_id": None,
                        "metadata_id": None,
                    }
                )
                continue

            # === Fetch the actual workflow document ===
            workflow_doc = mongo_db.automa_workflows.find_one({"_id": ObjectId(automa_wf_id)})
            if not workflow_doc:
                reason = "automa_workflow_not_found"
                logger.error(f"Automa workflow {automa_wf_id} not found")
                results.append(
                    {
                        "workflow_id": wf_id,
                        "workflow_type": wf_type,
                        "success": False,
                        "reason": reason,
                        "content_link_id": None,
                        "metadata_id": None,
                    }
                )
                continue

            # === Generate artifact filenames ===
            base_name = f"{username}_{wf_type}_{postgres_link_id}_{ts_str}"
            pre_screenshot_name = f"pre_{base_name}.png"
            post_screenshot_name = f"post_{base_name}.png"
            video_name = f"recording_{base_name}.webm"

            # CHANGE THIS TO YOUR ACTUAL ARTIFACT STORAGE BASE URL
            ARTIFACT_BASE_URL = "https://your-domain.com/artifacts/"  # ← UPDATE THIS

            pre_url = f"{ARTIFACT_BASE_URL}{pre_screenshot_name}"
            post_url = f"{ARTIFACT_BASE_URL}{post_screenshot_name}"
            video_url = f"{ARTIFACT_BASE_URL}{video_name}"

            # === Update take-screenshot blocks with fileName (first = pre, second = post) ===
            screenshots_updated = 0
            updated_drawflow = False

            if "drawflow" in workflow_doc and "nodes" in workflow_doc["drawflow"]:
                for node in workflow_doc["drawflow"]["nodes"]:
                    if "data" in node and "blocks" in node["data"]:
                        for block in node["data"]["blocks"]:
                            if block.get("id") == "take-screenshot" and "data" in block:
                                data = block["data"]
                                if screenshots_updated == 0:
                                    data["fileName"] = pre_screenshot_name
                                    logger.info(f"Set pre-screenshot filename: {pre_screenshot_name}")
                                elif screenshots_updated == 1:
                                    data["fileName"] = post_screenshot_name
                                    logger.info(f"Set post-screenshot filename: {post_screenshot_name}")
                                screenshots_updated += 1
                                updated_drawflow = True

            # Save updated drawflow if we modified screenshots
            if updated_drawflow:
                mongo_db.automa_workflows.update_one(
                    {"_id": ObjectId(automa_wf_id)},
                    {"$set": {"drawflow": workflow_doc["drawflow"]}},
                )
                logger.info(f"Updated drawflow with screenshot filenames for {wf_id}")

            # === Prepare metadata document with new artifacts_folder section ===
            metadata_doc = {
                "automa_workflow_id": ObjectId(automa_wf_id),
                "workflow_type": wf_type,
                "workflow_name": wf_id,
                "postgres_content_id": postgres_link_id,
                "account_id": account_id,
                "postgres_account_id": account_id,
                "has_link": True,
                "link_url": link_url.strip(),
                "link_assigned_at": datetime.now(timezone.utc),
                "has_content": True,
                "content_preview": (content_text or link_url)[:200],
                "content_hash": hash(link_url),
                "status": "ready_to_execute",
                "execute": True,
                "executed": False,
                "success": False,
                "execution_attempts": 0,
                "updated_at": datetime.now(timezone.utc),
                "tweeted_date": tweeted_date,
                "assignment_method": "multi_type_per_link",
                "assignment_source": "weekly_schedule",
                # === NEW: ARTIFACTS FOLDER SECTION ===
                "artifacts_folder": {
                    "pre_screenshot_name": pre_screenshot_name,
                    "pre_screenshot_url": pre_url,
                    "post_screenshot_name": post_screenshot_name,
                    "post_screenshot_url": post_url,
                    "video_name": video_name,
                    "video_url": video_url,
                    "generated_at": now,
                    "base_name": base_name,
                },
            }

            # Set created_at only on insert
            existing_metadata = mongo_db.workflow_metadata.find_one(
                {"automa_workflow_id": ObjectId(automa_wf_id), "account_id": account_id}
            )
            if not existing_metadata:
                metadata_doc["created_at"] = datetime.now(timezone.utc)

            # === Upsert workflow_metadata ===
            metadata_result = mongo_db.workflow_metadata.update_one(
                {"automa_workflow_id": ObjectId(automa_wf_id), "account_id": account_id},
                {"$set": metadata_doc},
                upsert=True,
            )

            if metadata_result.upserted_id:
                metadata_id = str(metadata_result.upserted_id)
            else:
                updated_doc = mongo_db.workflow_metadata.find_one(
                    {"automa_workflow_id": ObjectId(automa_wf_id), "account_id": account_id}
                )
                metadata_id = str(updated_doc["_id"]) if updated_doc else None

            # === Create content_workflow_links record ===
            link_doc = {
                "postgres_content_id": postgres_link_id,
                "automa_workflow_id": ObjectId(automa_wf_id),
                "content_type": wf_type,
                "link_url": link_url.strip(),
                "account_id": account_id,
                "linked_at": datetime.now(timezone.utc),
                "assignment_method": "multi_type_per_link",
                "assignment_source": "weekly_schedule",
                "content_preview": (content_text or "")[:200],
                "content_hash": hash(link_url),
                "has_link": True,
                "has_content": True,
            }

            try:
                link_result = mongo_db.content_workflow_links.insert_one(link_doc)
                content_link_id = str(link_result.inserted_id)
                logger.info(f"Created content_workflow_link {content_link_id}")
            except Exception as e:
                logger.warning(f"Failed to create content_workflow_link: {e}")

            success = True
            logger.info(f"SUCCESS: Assigned {wf_type} workflow {wf_id} to link {postgres_link_id}")

        except Exception as e:
            reason = str(e)
            logger.error(f"Assignment FAILED for {wf_type} workflow {wf_id}: {e}")
            import traceback

            logger.error(traceback.format_exc())

        results.append(
            {
                "workflow_id": wf_id,
                "workflow_type": wf_type,
                "success": success,
                "reason": reason,
                "content_link_id": content_link_id,
                "metadata_id": metadata_id,
            }
        )

    # === Final summary ===
    successful_by_type = {}
    failed_by_type = {}
    for r in results:
        t = r["workflow_type"]
        if r["success"]:
            successful_by_type[t] = successful_by_type.get(t, 0) + 1
        else:
            failed_by_type[t] = failed_by_type.get(t, 0) + 1

    logger.info("Assignment Summary:")
    for t in workflow_types:
        s = successful_by_type.get(t, 0)
        f = failed_by_type.get(t, 0)
        logger.info(f"   {t}: {s} successful, {f} failed")

    return results
def get_available_workflows_for_links(mongo_db, workflow_type=None):
    """Get workflows with has_content=True from workflow_metadata, optionally filtered by workflow_type"""
    try:
        # Build query - only get workflows with has_content=True
        query = {"has_content": True}
        
        # If workflow_type is specified, filter by it
        if workflow_type:
            query["workflow_type"] = workflow_type
            logger.info(f"🔍 Filtering workflows by type: {workflow_type}")
        
        workflows = list(mongo_db.workflow_metadata.find(
            query,
            {
                "automa_workflow_id": 1,
                "workflow_type": 1,
                "workflow_name": 1,
                "account_id": 1,
                "postgres_account_id": 1,
                "description": 1,
                "version": 1,
                "has_link": 1  # Also check if they already have links
            }
        ))
        
        result = []
        for doc in workflows:
            # Skip workflows that already have links assigned
            if doc.get('has_link', False):
                logger.debug(f"Skipping workflow {doc['workflow_name']} - already has link assigned")
                continue
                
            # CRITICAL FIX: Ensure account_id is properly set
            account_id = doc.get('account_id')
            if account_id is None:
                account_id = doc.get('postgres_account_id', 1)
            if account_id is None:
                account_id = 1
            
            # Ensure it's an integer
            account_id = ensure_integer_account_id(account_id)
            
            workflow = mongo_db.automa_workflows.find_one({'_id': doc['automa_workflow_id']})
            if not workflow:
                logger.warning(f"Workflow {doc['automa_workflow_id']} not found in automa_workflows")
                continue
            
            result.append({
                'workflow_id': doc['workflow_name'],
                'workflow_type': doc['workflow_type'],
                'automa_workflow_id': str(doc['automa_workflow_id']),
                'account_id': account_id,  # Use the properly set account_id
                'description': doc.get('description', ''),
                'has_content': True,
                'has_link': doc.get('has_link', False),  # Track if link is already assigned
                'available_for_links': True,
                'version': doc.get('version', '1.0')
            })
        
        logger.info(f"Found {len(result)} available workflows" + (f" for type '{workflow_type}'" if workflow_type else ""))
        
        # Log workflow type distribution
        type_counts = {}
        for wf in result:
            wf_type = wf['workflow_type']
            type_counts[wf_type] = type_counts.get(wf_type, 0) + 1
        
        logger.info(f"Workflows by type: {type_counts}")
        
        # Log account distribution
        account_counts = {}
        for wf in result:
            aid = wf['account_id']
            account_counts[aid] = account_counts.get(aid, 0) + 1
        
        logger.info(f"Workflows by account: {account_counts}")
        
        return result
    except Exception as e:
        logger.error(f"Error getting available workflows: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []
def safe_get_account_id(doc):
    """Safely get account_id from any document"""
    if not doc:
        return 1
    for field_name in ['account_id', 'postgres_account_id']:
        value = doc.get(field_name)
        if value is not None:
            return ensure_integer_account_id(value)
    name = doc.get('name', '')
    if '_account_' in name:
        try:
            parts = name.split('_account_')
            if len(parts) > 1:
                account_part = parts[1].split('_')[0]
                if account_part.isdigit():
                    return int(account_part)
        except:
            pass
    return 1

def determine_workflow_type(workflow_doc):
    """Determine workflow type from workflow document"""
    try:
        name = workflow_doc.get('name', '').lower()
        if 'reply' in name or 'replies' in name:
            return 'replies'
        elif 'message' in name or 'messages' in name:
            return 'messages'
        elif 'retweet' in name or 'retweets' in name:
            return 'retweets'
        description = workflow_doc.get('description', '').lower()
        if 'reply' in description:
            return 'replies'
        elif 'message' in description:
            return 'messages'
        elif 'retweet' in description:
            return 'retweets'
        return None
    except Exception as e:
        logger.warning(f"Error determining workflow type: {e}")
        return None






# File: dag_components/filtering_dag/workflow_manager.py
from typing import List, Dict, Any
from bson import ObjectId
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------- #
#  NO IMPORT NEEDED — function is in DAG file, passed via context
#

def update_workflow_url(
    mongo_db, workflow_id, old_url, new_url, account_id, 
    postgres_content_id, link_id, content_text=None, tweeted_date=None  # ← ADD
):
    """Update workflow in automa_workflows with new URL and store metadata in workflow_metadata"""
    try:
        account_id = ensure_integer_account_id(account_id)
        
        # Get the workflow
        workflow = mongo_db.automa_workflows.find_one({'_id': ObjectId(workflow_id)})
        if not workflow:
            logger.error(f"Workflow {workflow_id} not found in automa_workflows")
            return {
                "success": False,
                "error": f"Workflow {workflow_id} not found"
            }
        
        logger.info(f"Workflow loaded: {workflow.get('name', 'Unnamed')}")
        
        # Track replacements
        urls_replaced = 0
        blocks_checked = 0
        
        # Update URLs in drawflow.nodes structure
        if 'drawflow' in workflow and 'nodes' in workflow['drawflow']:
            for node in workflow['drawflow']['nodes']:
                if 'data' in node and 'blocks' in node['data']:
                    for block in node['data']['blocks']:
                        blocks_checked += 1
                        if 'data' in block and 'url' in block['data']:
                            if block['data']['url'] == old_url:
                                block['data']['url'] = new_url
                                urls_replaced += 1
                                logger.info(f"Updated URL in block '{block.get('id', 'unknown')}' "
                                           f"(itemId: {block.get('itemId', 'N/A')})")
        
        # Also check flat blocks structure
        elif 'blocks' in workflow:
            for block in workflow['blocks']:
                blocks_checked += 1
                if 'data' in block and 'url' in block['data']:
                    if block['data']['url'] == old_url:
                        block['data']['url'] = new_url
                        urls_replaced += 1
                        logger.info(f"Updated URL in block '{block.get('id', 'unknown')}'")
        
        # Check if any replacements were made
        if urls_replaced == 0:
            logger.warning(f"No instances of '{old_url}' found in {blocks_checked} blocks")
            return {
                "success": False,
                "error": f"No instances of '{old_url}' found",
                "blocks_checked": blocks_checked
            }
        
        # Update workflow name
        original_name = workflow.get('name', '')
        workflow['name'] = original_name.replace('kimu', 'facebook')
        logger.info(f"Updated workflow name: '{original_name}' -> '{workflow['name']}'")
        
        # Update description if it contains the old URL
        if 'description' in workflow and old_url in workflow['description']:
            workflow['description'] = workflow['description'].replace(old_url, new_url)
            logger.info(f"Updated workflow description")
        
        # Update workflow in automa_workflows
        result = mongo_db.automa_workflows.update_one(
            {'_id': ObjectId(workflow_id)},
            {'$set': {
                'drawflow': workflow.get('drawflow'),
                'blocks': workflow.get('blocks'),
                'name': workflow['name'],
                'description': workflow.get('description')
            }}
        )
        
        if result.modified_count == 0:
            logger.warning(f"No changes made to workflow {workflow_id}")
        
        # Store metadata in workflow_metadata
        workflow_type = determine_workflow_type(workflow)
        content_hash = hash(content_text or new_url) if content_text or new_url else 0
        metadata_doc = {
            'automa_workflow_id': ObjectId(workflow_id),
            'workflow_type': workflow_type,
            'workflow_name': workflow['name'],
            'postgres_content_id': postgres_content_id,
            'account_id': account_id,
            'link_id': link_id,
            'linked_at': datetime.now(timezone.utc),
            'content_preview': (content_text[:100] + '...' if content_text and len(content_text) > 100 else content_text) or new_url[:100],
            'content_length': len(content_text) if content_text else len(new_url),
            'content_hash': content_hash,
            'has_link': True,
            'link_url': new_url,
            'tweeted_date': tweeted_date,  # ← STORE IT HERE
            'content_updated_at': datetime.now(timezone.utc).isoformat(),
            'assignment_method': 'single_url_per_account',
            'status': 'link_assigned',
            'created_at': datetime.now(timezone.utc).isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat()
        }
        
        metadata_result = mongo_db.workflow_metadata.update_one(
            {'automa_workflow_id': ObjectId(workflow_id), 'account_id': account_id},
            {'$set': metadata_doc},
            upsert=True
        )
        
        # Update PostgreSQL links table
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE links 
                        SET workflow_id = %s,
                            used = TRUE,
                            used_time = %s,
                            processed_by_workflow = TRUE,
                            workflow_processed_time = %s
                        WHERE links_id = %s
                    """, (str(workflow_id), datetime.now(timezone.utc), 
                          datetime.now(timezone.utc), postgres_content_id))
                    conn.commit()
                    logger.info(f"Updated PostgreSQL links table for content {postgres_content_id}")
        except Exception as pg_error:
            logger.warning(f"Failed to update PostgreSQL links table: {pg_error}")
        
        logger.info(f"Successfully updated workflow {workflow_id} with {urls_replaced} URLs")
        return {
            "success": True,
            "workflow_id": str(workflow_id),
            "old_url": old_url,
            "new_url": new_url,
            "blocks_checked": blocks_checked,
            "urls_replaced": urls_replaced,
            "workflow_name": workflow['name'],
            "link_id": link_id
        }
    
    except Exception as e:
        logger.error(f"Error updating workflow {workflow_id}: {e}")
        return {
            "success": False,
            "error": f"Error processing workflow: {e}"
        }
def reset_workflow_links(mongo_db, workflow_type=None, account_id=None):
    """Reset workflows back to initial state"""
    try:
        account_id = ensure_integer_account_id(account_id) if account_id else None
        query = {}
        if workflow_type:
            query['workflow_type'] = workflow_type
        if account_id:
            query['account_id'] = account_id
        
        # Reset metadata
        result = mongo_db.workflow_metadata.update_many(
            query,
            {
                '$set': {
                    'has_link': False,
                    'link_url': None,
                    'link_id': None,
                    'status': 'generated',
                    'content_updated_at': datetime.datetime.now(timezone.utc).isoformat(),
                    'updated_at': datetime.datetime.now(timezone.utc).isoformat()
                }
            }
        )
        
        # Reset workflows
        workflows_to_reset = mongo_db.workflow_metadata.find(query)
        reset_count = 0
        for metadata in workflows_to_reset:
            workflow = mongo_db.automa_workflows.find_one({'_id': metadata['automa_workflow_id']})
            if not workflow:
                continue
                
            # Reset URLs to a default placeholder (e.g., 'www.placeholder.com')
            default_url = 'www.placeholder.com'
            urls_replaced = 0
            if 'drawflow' in workflow and 'nodes' in workflow['drawflow']:
                for node in workflow['drawflow']['nodes']:
                    if 'data' in node and 'blocks' in node['data']:
                        for block in node['data']['blocks']:
                            if 'data' in block and 'url' in block['data']:
                                block['data']['url'] = default_url
                                urls_replaced += 1
            elif 'blocks' in workflow:
                for block in workflow['blocks']:
                    if 'data' in block and 'url' in block['data']:
                        block['data']['url'] = default_url
                        urls_replaced += 1
            
            if urls_replaced > 0:
                mongo_db.automa_workflows.update_one(
                    {'_id': workflow['_id']},
                    {'$set': {
                        'drawflow': workflow.get('drawflow'),
                        'blocks': workflow.get('blocks')
                    }}
                )
                reset_count += 1
        
        logger.info(f"Reset {reset_count} workflows and {result.modified_count} metadata records")
        return reset_count + result.modified_count
    
    except Exception as e:
        logger.error(f"Error resetting workflows: {e}")
        return 0

def get_workflow_link_status(mongo_db):
    """Get statistics about workflows and links from workflow_metadata only"""
    try:
        stats = {}
        
        # Get counts from workflow_metadata
        total_workflows = mongo_db.workflow_metadata.count_documents({})
        
        workflows_by_account = {}
        metadata = list(mongo_db.workflow_metadata.find({}))
        
        for doc in metadata:
            account_id = ensure_integer_account_id(doc.get('account_id', 1))
            workflow_type = doc.get('workflow_type')
            if not workflow_type:
                continue
                
            if account_id not in workflows_by_account:
                workflows_by_account[account_id] = {}
            if workflow_type not in workflows_by_account[account_id]:
                workflows_by_account[account_id][workflow_type] = {
                    'with_links': 0,
                    'available_for_links': 0,
                    'executed': 0,
                    'successful': 0
                }
            
            if doc.get('has_link', False):
                workflows_by_account[account_id][workflow_type]['with_links'] += 1
            else:
                workflows_by_account[account_id][workflow_type]['available_for_links'] += 1
            
            if doc.get('executed', False):
                workflows_by_account[account_id][workflow_type]['executed'] += 1
            
            if doc.get('success', False):
                workflows_by_account[account_id][workflow_type]['successful'] += 1
        
        stats = {
            'workflow_metadata': {
                'total': total_workflows,
                'by_account': workflows_by_account
            },
            'assignment_summary': {
                'total_workflows': total_workflows,
                'assignment_method': 'single_source_truth'
            }
        }
        
        logger.info(f"Workflow stats generated from workflow_metadata")
        return stats
    
    except Exception as e:
        logger.error(f"Error getting workflow stats: {e}")
        return {
            'workflow_metadata': {'total': 0, 'by_account': {}},
            'assignment_summary': {
                'total_workflows': 0,
                'assignment_method': 'single_source_truth'
            }
        }