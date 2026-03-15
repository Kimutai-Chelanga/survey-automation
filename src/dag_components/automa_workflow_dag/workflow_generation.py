import os
import json
import re
import uuid
import time
import copy
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_fixed
from .config import logger
from .db_utils import get_mongo_db
from core.database.postgres.connection import get_postgres_connection
import random
from .timing_randomizer import finalize_workflow_timing, resolve_timing_ms


def _get_workflow_priority(workflow_type):
    """Get processing priority for workflow type"""
    return 1


def deep_remove_id_fields(obj):
    """
    Recursively remove ALL _id fields from a nested structure.
    This prevents MongoDB duplicate key errors when templates come from MongoDB.
    """
    if isinstance(obj, dict):
        if '_id' in obj:
            del obj['_id']
        for key, value in list(obj.items()):
            if isinstance(value, (dict, list)):
                deep_remove_id_fields(value)
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                deep_remove_id_fields(item)
    return obj


def create_workflow_with_multi_content(
    workflow_type,
    template_data,
    name,
    content_items_data,
    automa_config=None,
    collection_name=None,
    database_name=None
):
    """
    Create workflow in specified MongoDB collection with MULTIPLE content items.

    Handles CONTENT1, CONTENT2, CONTENT3 placeholders.

    CONTENT1 / CONTENT2:
        Replaced as character-by-character press-key blocks inside BlockGroup nodes.
        Timing comes from press_keys config (uses resolve_timing_ms — rounding intact).

    CONTENT3:
        Injected directly into the BlockBasic 'forms' (text-field) node.
        - node['data']['value']  ← content text
        - node['data']['delay']  ← raw int from typing_delay config (NO rounding,
                                   NO resolve_timing_ms — value used exactly as set)

    TIMING ORDER (important):
        1. finalize_workflow_timing() runs FIRST on the raw template.
           It applies timing to click elements, delays, and any template-level
           press-key blocks — but NOT to character blocks because they don't
           exist yet at this point.
        2. find_and_replace_multi_content_blocks() runs AFTER timing is applied.
           Character blocks are created here via generate_character_press_key_blocks()
           which calls resolve_timing_ms() directly from the config.
           Since finalize_workflow_timing() has already finished, nothing can
           overwrite the character block timings.

    Args:
        workflow_type: Type of workflow
        template_data: Template from MongoDB
        name: Base workflow name
        content_items_data: List of dicts:
            [
                {
                    'label': 'CONTENT1',
                    'content_text': '...',
                    'content_id': 123,
                    'content_type': 'replies',
                    'account_id': 5,
                    'prompt_id': 10,
                    'username': 'user1',
                    'profile_id': 'profile1'
                },
                ...
            ]
        automa_config: Configuration dict (includes press_keys/click_elements/typing_delay with mode)
        collection_name: Target collection
        database_name: Target database

    Returns:
        tuple: (success, automa_workflow_id, execution_start, execution_end,
                execution_time_ms, blocks_generated, has_link, workflow_name,
                template_name, workflow_id, content_ids_used)
    """
    start_time      = time.time()
    execution_start = datetime.now()

    # Generate unique workflow_id
    workflow_id = str(uuid.uuid4())

    # Defaults
    if not database_name:
        database_name = 'execution_workflows'
    if not collection_name:
        collection_name = f"{workflow_type}_workflows"

    try:
        # Validate inputs
        if not template_data or not isinstance(template_data, dict):
            raise ValueError(f"Invalid template_data for {workflow_type}")

        if not content_items_data or not isinstance(content_items_data, list):
            raise ValueError("content_items_data must be a non-empty list")

        press_mode  = (automa_config or {}).get('press_keys', {}).get('mode', 'range')
        click_mode  = (automa_config or {}).get('click_elements', {}).get('mode', 'range')
        typing_ms   = int((automa_config or {}).get('typing_delay', {}).get('max_milliseconds', 80))
        logger.info(
            f"🚀 Creating workflow with {len(content_items_data)} content items "
            f"(press={press_mode}, click={click_mode}, typing={typing_ms}ms)"
        )

        # Clone template
        workflow_data = copy.deepcopy(template_data.get('workflow_data', template_data))

        # Remove _id fields
        logger.info("🧹 Removing all _id fields from workflow structure...")
        workflow_data = deep_remove_id_fields(workflow_data)
        logger.info("✅ All _id fields removed")

        # ============================================================
        # STEP 1: Update workflow name/description
        # ============================================================
        content_ids_str        = "_".join([str(item['content_id']) for item in content_items_data])
        primary_account_id     = content_items_data[0].get('account_id', 'no_account')
        workflow_data['name']  = f"{name}_{content_ids_str}_{primary_account_id}"

        content_previews = []
        for item in content_items_data:
            preview = item['content_text'][:30] + "..." if len(item['content_text']) > 30 else item['content_text']
            content_previews.append(f"{item['label']}: {preview}")
        workflow_data['description'] = (
            f"Generated {workflow_type} workflow - " + "; ".join(content_previews)
        )

        # ============================================================
        # STEP 2: Apply timing FIRST — before placeholders are replaced.
        # ============================================================
        if automa_config:
            logger.info(
                f"🎭 Applying timing to template blocks BEFORE placeholder replacement "
                f"(press={press_mode}, click={click_mode})..."
            )
            workflow_data = finalize_workflow_timing(workflow_data, automa_config)
            logger.info("✅ Timing applied to template blocks")
        else:
            logger.warning("⚠️ No automa_config provided, skipping timing")

        # ============================================================
        # STEP 3: Build content lookup by label
        # ============================================================
        content_lookup = {item['label']: item for item in content_items_data}

        press_keys_config   = (automa_config or {}).get('press_keys', {})
        typing_delay_config = (automa_config or {}).get('typing_delay', {})

        # ============================================================
        # STEP 4a: Replace CONTENT1 / CONTENT2 in BlockGroup nodes
        #           (character-by-character press-key blocks)
        # ============================================================
        placeholder_replaced   = {}
        total_blocks_generated = 0

        logger.info(f"🔍 Searching for press-key placeholders in {workflow_type} template...")

        if 'drawflow' in workflow_data and 'nodes' in workflow_data['drawflow']:
            for node_idx, node in enumerate(workflow_data['drawflow']['nodes']):
                node_type  = node.get('type')
                node_label = node.get('label', 'unnamed')

                if node_type == 'BlockGroup' and 'data' in node and 'blocks' in node['data']:
                    blocks = node['data']['blocks']
                    logger.info(f"  📦 Node {node_idx} (BlockGroup '{node_label}'): {len(blocks)} blocks")

                    new_blocks, replacements_made = find_and_replace_multi_content_blocks(
                        blocks,
                        content_items_data,
                        workflow_type,
                        press_keys_config
                    )

                    if replacements_made:
                        node['data']['blocks'] = new_blocks
                        total_blocks_generated += len(new_blocks)
                        placeholder_replaced.update(replacements_made)
                        logger.info(
                            f"✅ Replaced {len(replacements_made)} placeholders "
                            f"in BlockGroup node {node_idx}"
                        )

        # ============================================================
        # STEP 4b: Inject CONTENT3 into the BlockBasic 'forms' node.
        #
        #   typing_delay_ms is read DIRECTLY from the config — no
        #   resolve_timing_ms, no rounding. Whatever the user sets
        #   (e.g. 90) is written verbatim to node['data']['delay'].
        # ============================================================
        content3_item = content_lookup.get('CONTENT3')

        if content3_item:
            # Read raw value — bypass resolve_timing_ms entirely to avoid rounding
            typing_delay_ms = int(typing_delay_config.get('max_milliseconds', 80))
            logger.info(
                f"📝 Injecting CONTENT3 into forms node "
                f"(typing_delay={typing_delay_ms}ms — raw, no rounding)"
            )
            injected = inject_content3_into_forms_node(
                workflow_data,
                content3_item['content_text'],
                typing_delay_ms
            )
            if injected:
                placeholder_replaced['CONTENT3'] = True
                logger.info("✅ CONTENT3 injected into forms node")
            else:
                logger.warning("⚠️ No forms (text-field) node found for CONTENT3 injection")
        elif 'CONTENT3' in [item['label'] for item in content_items_data]:
            logger.warning("⚠️ CONTENT3 in content_items_data but not in lookup — skipping")

        if placeholder_replaced:
            logger.info(f"✅ Successfully replaced placeholders: {list(placeholder_replaced.keys())}")
        else:
            logger.warning(f"⚠️ No placeholders found in {workflow_type} template")

        # ============================================================
        # STEP 5: Save to MongoDB
        # ============================================================
        all_content_text = " ".join([item['content_text'] for item in content_items_data])
        has_link = bool(re.search(
            r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
            all_content_text
        ))

        execution_end     = datetime.now()
        execution_time_ms = int((time.time() - start_time) * 1000)

        _, client    = get_mongo_db()
        workflow_db  = client[database_name]
        metadata_db  = client['messages_db']

        logger.info(f"📝 Connected to workflow database: {database_name}")
        logger.info(f"📝 Connected to metadata database: messages_db")

        try:
            with client.start_session() as session:
                # 1. Insert pure Automa workflow
                workflow_collection = workflow_db[collection_name]
                logger.info(f"📝 Inserting PURE workflow into {database_name}.{collection_name}")

                if '_id' in workflow_data:
                    logger.warning("⚠️ Found _id in workflow_data before insert, removing...")
                    del workflow_data['_id']

                workflow_result    = workflow_collection.insert_one(workflow_data, session=session)
                automa_workflow_id = workflow_result.inserted_id
                logger.info(f"✅ Created PURE Automa workflow with ID: {automa_workflow_id}")

                # 2. Insert metadata record
                metadata_collection = metadata_db['workflow_metadata']
                content_ids_used    = [item['content_id'] for item in content_items_data]
                primary_content     = content_items_data[0]

                def _timing_summary(cfg):
                    mode = cfg.get('mode', 'range')
                    if mode == 'fixed':
                        return {'mode': 'fixed', 'value_ms': cfg.get('max_milliseconds', 1000)}
                    return {
                        'mode':   'range',
                        'min_ms': cfg.get('min_milliseconds', 0),
                        'max_ms': cfg.get('max_milliseconds', 10000)
                    }

                def _typing_summary(cfg):
                    # Typing delay is always fixed — store raw value, no rounding
                    return {
                        'mode':     'fixed',
                        'value_ms': int(cfg.get('max_milliseconds', 80))
                    }

                metadata_record = {
                    # Core identifiers
                    "automa_workflow_id":   automa_workflow_id,
                    "workflow_id":          workflow_id,
                    "workflow_type":        workflow_type,
                    "workflow_name":        workflow_data['name'],
                    "execution_id":         str(uuid.uuid4()),

                    # Storage location
                    "database_name":        database_name,
                    "collection_name":      collection_name,
                    "category":             automa_config.get('destination_category', workflow_type) if automa_config else workflow_type,

                    # Primary PostgreSQL references (backward compatibility)
                    "postgres_content_id":  primary_content['content_id'],
                    "postgres_account_id":  primary_content.get('account_id'),
                    "postgres_prompt_id":   primary_content.get('prompt_id'),
                    "postgres_workflow_id": workflow_id,

                    # Multi-content
                    "content_items": [
                        {
                            "label":            item['label'],
                            "content_id":       item['content_id'],
                            "content_type":     item['content_type'],
                            "content_text":     item['content_text'],
                            "content_preview":  item['content_text'][:100] + "..." if len(item['content_text']) > 100 else item['content_text'],
                            "content_length":   len(item['content_text']),
                            "account_id":       item.get('account_id'),
                            "prompt_id":        item.get('prompt_id'),
                            "username":         item.get('username'),
                            "profile_id":       item.get('profile_id')
                        }
                        for item in content_items_data
                    ],
                    "content_items_count": len(content_items_data),
                    "all_content_ids":     content_ids_used,

                    # Account (from primary)
                    "username":   primary_content.get('username'),
                    "profile_id": primary_content.get('profile_id'),

                    # Content (combined)
                    "actual_content":       all_content_text,
                    "content_text_preview": all_content_text[:200] + "..." if len(all_content_text) > 200 else all_content_text,
                    "content_length":       len(all_content_text),
                    "content_hash":         hash(all_content_text),
                    "has_link":             has_link,

                    # Status tracking
                    "status":      "generated",
                    "has_content": True,
                    "execute":     False,
                    "executed":    False,
                    "success":     False,

                    # Timing
                    "execution_start":    execution_start.isoformat(),
                    "execution_end":      execution_end.isoformat(),
                    "execution_time_ms":  execution_time_ms,
                    "generated_at":       execution_start.isoformat(),

                    # Generation metadata
                    "blocks_generated":       total_blocks_generated,
                    "template_used":          template_data.get('template_name', 'unknown'),
                    "processing_priority":    _get_workflow_priority(workflow_type),
                    "placeholders_replaced":  list(placeholder_replaced.keys()),

                    # Config used
                    "automa_config":          automa_config,
                    "timing_applied":         automa_config is not None,
                    "press_keys_timing":      _timing_summary(automa_config.get('press_keys', {})) if automa_config else None,
                    "click_elements_timing":  _timing_summary(automa_config.get('click_elements', {})) if automa_config else None,
                    "typing_delay_timing":    _typing_summary(automa_config.get('typing_delay', {})) if automa_config else None,

                    # Generation context
                    "generation_context": {
                        "template_name":         template_data.get('template_name'),
                        "template_category":     template_data.get('category'),
                        "blocks_generated":      total_blocks_generated,
                        "placeholders_found":    list(placeholder_replaced.keys()),
                        "account_based":         primary_content.get('account_id') is not None,
                        "custom_content_type":   workflow_type,
                        "multi_content":         len(content_items_data) > 1,
                        "press_keys_mode":       press_mode,
                        "click_elements_mode":   click_mode,
                        "typing_delay_ms":       typing_ms,
                    },

                    # Performance metrics
                    "performance_metrics": {
                        "generation_time_ms":                      execution_time_ms,
                        "template_loading_successful":             True,
                        "placeholder_replacement_successful":      len(placeholder_replaced) > 0,
                        "blocks_generated_count":                  total_blocks_generated,
                        "timing_randomization_applied":            automa_config is not None
                    },

                    # Timestamps
                    "created_at": execution_start.isoformat(),
                    "updated_at": execution_end.isoformat()
                }

                metadata_result = metadata_collection.insert_one(metadata_record, session=session)
                logger.info(f"📊 Created metadata record with ID: {metadata_result.inserted_id}")

        except Exception as db_error:
            logger.error(f"Database transaction failed: {db_error}")
            if hasattr(db_error, 'details'):
                logger.error(f"Error details: {db_error.details}")
            raise
        finally:
            client.close()

        logger.info(f"✅ Successfully created workflow for {workflow_type}")
        logger.info(f"   Pure workflow stored in: {database_name}.{collection_name}")
        logger.info(f"   Metadata stored in: messages_db.workflow_metadata")
        logger.info(f"   Content IDs used: {content_ids_used}")

        return (True, automa_workflow_id, execution_start, execution_end,
                execution_time_ms, total_blocks_generated, has_link,
                workflow_data['name'], template_data.get('template_name'),
                workflow_id, content_ids_used)

    except Exception as e:
        execution_end     = datetime.now()
        execution_time_ms = int((time.time() - start_time) * 1000)

        logger.error(f"❌ Failed to create workflow: {e}")

        try:
            _, client   = get_mongo_db()
            metadata_db = client['messages_db']

            with client.start_session() as session:
                error_record = {
                    "automa_workflow_id": None,
                    "workflow_id":        workflow_id,
                    "workflow_type":      workflow_type,
                    "workflow_name":      f"{name}_failed",
                    "execution_id":       str(uuid.uuid4()),
                    "database_name":      database_name,
                    "collection_name":    collection_name,
                    "content_items":      [{"label": item['label'], "content_id": item['content_id']} for item in content_items_data],
                    "status":             "failed",
                    "has_content":        False,
                    "timing_applied":     False,
                    "execution_start":    execution_start.isoformat(),
                    "execution_end":      execution_end.isoformat(),
                    "execution_time_ms":  execution_time_ms,
                    "error_message":      str(e),
                    "created_at":         execution_start.isoformat(),
                    "updated_at":         execution_end.isoformat()
                }
                metadata_db.workflow_metadata.insert_one(error_record, session=session)
            client.close()
        except Exception as log_error:
            logger.error(f"Failed to log error to metadata: {log_error}")

        raise


# ============================================================================
# CONTENT3 → FORMS NODE INJECTION
# ============================================================================

def inject_content3_into_forms_node(workflow_data: dict, content_text: str, typing_delay_ms: int) -> bool:
    """
    Find the BlockBasic 'forms' node (type='text-field') and inject CONTENT3.

    Sets:
        node['data']['value'] = content_text
        node['data']['delay'] = str(typing_delay_ms)
            (this is Automa's "Typing delay (millisecond)" field — 0 disables it)

    typing_delay_ms is the raw integer from the config with NO rounding applied.
    If the user sets 90, this writes "90" exactly.

    The node is identified by label == 'forms' OR data.type == 'text-field'.
    Handles both the top-level nodes list and nested block structures.

    Returns:
        True if at least one forms node was updated, False otherwise.
    """
    updated = False

    nodes = workflow_data.get('drawflow', {}).get('nodes', [])
    for node in nodes:
        node_label = node.get('label', '')
        node_type  = node.get('type', '')

        if node_type == 'BlockBasic':
            node_data = node.get('data', {})
            if node_label == 'forms' or node_data.get('type') == 'text-field':
                old_value = node_data.get('value', '<unset>')
                old_delay = node_data.get('delay', '<unset>')

                node_data['value'] = content_text
                node_data['delay'] = str(typing_delay_ms)
                node['data'] = node_data

                logger.info(
                    f"   📝 Forms node updated: "
                    f"value='{content_text[:40]}...' (was '{str(old_value)[:40]}'), "
                    f"delay={typing_delay_ms}ms (was {old_delay}ms)"
                )
                updated = True

    # Fallback: scan inside BlockGroup blocks for embedded forms blocks
    for node in nodes:
        if node.get('type') == 'BlockGroup':
            for block in node.get('data', {}).get('blocks', []):
                if block.get('id') == 'forms' or block.get('data', {}).get('type') == 'text-field':
                    block_data = block.get('data', {})
                    block_data['value'] = content_text
                    block_data['delay'] = str(typing_delay_ms)
                    block['data'] = block_data
                    logger.info(
                        f"   📝 Embedded forms block updated in BlockGroup: "
                        f"value='{content_text[:40]}...', delay={typing_delay_ms}ms"
                    )
                    updated = True

    return updated


# ============================================================================
# CONTENT1 / CONTENT2 — BlockGroup press-key replacement
# ============================================================================

def find_and_replace_multi_content_blocks(blocks, content_items_data, workflow_type, press_keys_config):
    """
    Find and replace CONTENT1 / CONTENT2 press-key placeholders with
    character-by-character press-key blocks.

    NOTE: CONTENT3 is handled separately via inject_content3_into_forms_node().
          Any CONTENT3 press-key placeholder found here is intentionally skipped.

    Args:
        blocks: List of workflow blocks
        content_items_data: List of content item dicts
        workflow_type: Type of workflow
        press_keys_config: {mode, min_milliseconds, max_milliseconds}

    Returns:
        tuple: (new_blocks, replacements_made_dict)
    """
    new_blocks        = []
    replacements_made = {}

    content_lookup = {
        item['label']: item
        for item in content_items_data
        if item['label'] in ('CONTENT1', 'CONTENT2')
    }
    logger.info(f"Looking for press-key placeholders: {list(content_lookup.keys())}")

    press_mode = press_keys_config.get('mode', 'range')
    logger.info(f"Press keys mode for character generation: {press_mode}")

    for block in blocks:
        block_data  = block.get('data', {})
        block_id    = block.get('id', '')

        if block_id == 'press-key':
            keys_value    = block_data.get('keys', '')
            matched_label = None

            for label in content_lookup.keys():
                if keys_value == label or label in str(keys_value):
                    matched_label = label
                    break

            if matched_label:
                try:
                    content_item = content_lookup[matched_label]
                    replacements_made[matched_label] = True

                    logger.info(f"✅ Found placeholder block for {matched_label}: keys='{keys_value}'")

                    typing_blocks = generate_character_press_key_blocks(
                        content_item['content_text'],
                        block,
                        f"{workflow_type}_{content_item['content_id']}_{int(time.time())}",
                        press_keys_config
                    )
                    new_blocks.extend(typing_blocks)
                    logger.info(f"🔤 Replaced {matched_label} with {len(typing_blocks)} character blocks")

                except Exception as e:
                    logger.error(f"❌ Error replacing {matched_label}: {e}")
                    new_blocks.append(block)
                    continue
            else:
                new_blocks.append(block)
        else:
            new_blocks.append(block)

    return new_blocks, replacements_made


def generate_character_press_key_blocks(sentence, placeholder_block, base_item_id, press_keys_config):
    """
    Generate character-level press-key blocks for CONTENT1 / CONTENT2.

    Uses resolve_timing_ms() for press key timing (rounding behaviour unchanged).

    Args:
        sentence: Text to type
        placeholder_block: Original block (for selector reference)
        base_item_id: Unique base ID for generated blocks
        press_keys_config: {mode, min_milliseconds, max_milliseconds}
    """
    sentence   = sentence.strip()
    blocks     = []
    press_mode = press_keys_config.get('mode', 'range')

    min_press_ms = press_keys_config.get('min_milliseconds', 500)
    max_press_ms = press_keys_config.get('max_milliseconds', 2000)

    logger.info(
        f"Generating character blocks: mode={press_mode}, "
        f"min={min_press_ms}ms, max={max_press_ms}ms, "
        f"chars={len(sentence)}"
    )

    for i, char in enumerate(sentence):
        description = "Type space" if char == ' ' else f"Type {'character' if char.isalnum() else 'special character'}: {char}"

        press_time_ms = resolve_timing_ms(press_keys_config, default_min=min_press_ms, default_max=max_press_ms)

        char_block = {
            "id":     "press-key",
            "itemId": f"{base_item_id}_char_{i}",
            "data": {
                "disableBlock": False,
                "keys":         char,
                "selector":     placeholder_block["data"].get("selector", ""),
                "pressTime":    str(press_time_ms),
                "description":  description,
                "keysToPress":  "",
                "action":       "press-key",
                "onError": {
                    "retry":         False,
                    "enable":        True,
                    "retryTimes":    1,
                    "retryInterval": 2,
                    "toDo":          "continue",
                    "insertData":    False,
                    "dataToInsert":  []
                },
                "settings": {
                    "blockTimeout": 0,
                    "debugMode":    False
                }
            }
        }
        blocks.append(char_block)
        logger.debug(f"  char {i} '{char}': pressTime={press_time_ms}ms (mode={press_mode})")

    logger.info(f"Generated {len(blocks)} character blocks (mode={press_mode})")
    return blocks


def update_has_content(workflow_ids_by_type):
    """Update has_content=True in workflow_metadata collection."""
    _, client = get_mongo_db()
    try:
        metadata_db         = client['messages_db']
        metadata_collection = metadata_db['workflow_metadata']

        total_updated = 0
        with client.start_session() as session:
            for workflow_type, workflow_ids in workflow_ids_by_type.items():
                if not workflow_ids:
                    continue

                result = metadata_collection.update_many(
                    {
                        "automa_workflow_id": {"$in": workflow_ids},
                        "workflow_type":      workflow_type
                    },
                    {
                        "$set": {
                            "has_content":               True,
                            "status":                    "content_confirmed",
                            "content_status_updated_at": datetime.now().isoformat()
                        }
                    },
                    session=session
                )

                logger.info(f"Updated has_content=True for {workflow_type}: {result.modified_count}")
                total_updated += result.modified_count

        logger.info(f"DAG completion: Updated has_content=True for {total_updated} total workflows")
        return total_updated

    except Exception as e:
        logger.error(f"Failed updating has_content: {e}")
        raise
    finally:
        client.close()
