"""
Execution Configuration - Main Settings

UPDATED (2026-02-20):
  - Added 'Number of workflows to process' input field inside ⚙️ Execution Settings
  - FIX: Updated get_collections_for_category_and_type() to look for both
    "ready" and "ready_to_execute" status values
  - FIX: Added better debugging for workflow status in collection display

UPDATED (2026-02-19):
  - Replaced 'Total Execution Budget' with 'Wait Margin' input inside ⚙️ Execution Settings.
    Bottom-up block analysis ALWAYS runs.
    Formula: configured_wait = bottom_up_estimated + wait_margin_ms

UPDATED (2026-02-15):
  - Fixed MongoDB key: 'execution_orchestrator_template'
  - Fixed document structure: { template_name, template_data, ... }
  - Reorganised into three tabs: 📋 Configuration / 📤 Upload Template / 📊 Statistics
  - Added min/max delay range inputs inside ⚙️ Execution Settings

SAFE-MERGE UPDATE:
  - All save operations reload fresh from MongoDB before writing so no other
    tab's settings (time windows, time_filter, testing_config, etc.) are wiped.
  - Statistics tab now has ✏️ Edit buttons with inline forms for each day.

UPDATED: Added "Apply to All Week" — save same execution config to all 7 days in one click.
"""

import streamlit as st
from datetime import date, datetime, timedelta
from ...settings.settings_manager import (
    get_system_setting,
    update_system_setting
)
import json
from typing import Dict, Any, List, Optional
from bson import ObjectId
import logging

logger = logging.getLogger(__name__)

DAYS_ORDER = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_future_dates(num_dates: int = 8) -> list:
    dates = []
    today = datetime.now().date()
    for i in range(num_dates):
        future_date = today + timedelta(days=i)
        date_str    = future_date.strftime('%b %d, %Y')
        if i == 0:
            date_str += " (Today)"
        dates.append((future_date, date_str))
    return dates


def get_day_key_from_date(target_date: date) -> str:
    weekday = target_date.weekday()
    days    = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    return days[weekday]


def check_execution_config_exists(weekly_settings: dict, target_date: date) -> tuple:
    day_key    = get_day_key_from_date(target_date)
    day_config = weekly_settings.get(day_key, {})

    if not day_config or 'execution_config' not in day_config:
        return (False, day_key, {})

    exec_config     = day_config.get('execution_config', {})
    stored_date_str = exec_config.get('config_date')

    if not stored_date_str:
        return (False, day_key, {})

    try:
        stored_date = date.fromisoformat(stored_date_str)
        if stored_date == target_date:
            return (True, day_key, exec_config)
        else:
            return (False, day_key, {})
    except:
        return (False, day_key, {})


def get_saved_workflow_categories() -> List[str]:
    try:
        workflow_categories = get_system_setting('workflow_categories', {})
        if not isinstance(workflow_categories, dict):
            workflow_categories = {}
        return sorted(list(workflow_categories.keys()))
    except Exception as e:
        logger.error(f"Error loading categories: {e}")
        return []


def get_workflow_types_for_category(category: str) -> List[str]:
    try:
        workflow_categories = get_system_setting('workflow_categories', {})
        if not isinstance(workflow_categories, dict):
            return []
        return workflow_categories.get(category, [])
    except Exception as e:
        logger.error(f"Error loading types: {e}")
        return []


def get_collections_for_category_and_type(category: str, workflow_type: str) -> List[Dict[str, Any]]:
    """
    Get collections for a given category and workflow type.
    Looks for both "ready" and "ready_to_execute" status values.
    """
    try:
        from pymongo import MongoClient
        import os

        client = MongoClient(
            os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
            serverSelectionTimeoutMS=5000
        )

        messages_db         = client['messages_db']
        metadata_collection = messages_db['workflow_metadata']

        pipeline = [
            {"$match": {
                "category":      category.lower(),
                "workflow_type": workflow_type.lower()
            }},
            {"$group": {
                "_id":         "$collection_name",
                "total_count": {"$sum": 1},
                "ready_count": {"$sum": {"$cond": [
                    {"$or": [
                        {"$eq": ["$status", "ready"]},
                        {"$eq": ["$status", "ready_to_execute"]}
                    ]},
                    1, 0
                ]}}
            }},
            {"$sort": {"_id": 1}}
        ]

        results = list(metadata_collection.aggregate(pipeline))
        client.close()

        return [
            {'collection_name': r['_id'], 'total_count': r['total_count'], 'ready_count': r['ready_count']}
            for r in results
        ]

    except Exception as e:
        logger.error(f"Error loading collections: {e}")
        st.error(f"❌ Error loading collections: {e}")
        return []


# ============================================================================
# HELPER: Build a single day's execution_config dict
# ============================================================================

def _build_execution_day_config(
    existing_day_cfg: dict,
    day_key: str,
    day_name: str,
    config_date_iso: str,
    selected_category: str,
    selected_workflow_type: str,
    selected_collection: str,
    enabled: bool,
    max_workflows: int,
    delay_min: int,
    delay_max: int,
    wait_margin_ms: int,
) -> dict:
    """Build (or update) an execution_config dict for one day, preserving unrelated day keys."""
    day_cfg = dict(existing_day_cfg)
    day_cfg['day_name'] = day_name
    day_cfg['day_key']  = day_key
    day_cfg['execution_config'] = {
        'config_date':              config_date_iso,
        'day_name':                 day_name,
        'day_key':                  day_key,
        'workflow_category':        selected_category.lower(),
        'workflow_type':            selected_workflow_type.lower() if hasattr(selected_workflow_type, 'lower') else selected_workflow_type,
        'collection_name':          selected_collection,
        'enabled':                  enabled,
        'max_workflows_to_process': int(max_workflows),
        'delay_min_milliseconds':   int(delay_min),
        'delay_max_milliseconds':   int(delay_max),
        'wait_margin_ms':           int(wait_margin_ms),
        'updated_at':               datetime.now().isoformat(),
    }
    return day_cfg


# ============================================================================
# ORCHESTRATOR MANAGEMENT
# ============================================================================

ORCHESTRATOR_SETTINGS_KEY = 'execution_orchestrator_template'


def _render_upload_template_tab():
    st.markdown("### 🎼 Workflow Orchestrator Template")
    st.caption(
        "Upload or paste the orchestrator workflow template. "
        "This is used by the Airflow DAG to wrap all eligible workflows "
        "in a single master workflow."
    )

    orchestrator_template = get_system_setting(ORCHESTRATOR_SETTINGS_KEY, None)

    if orchestrator_template:
        st.success("✅ Orchestrator template configured")

        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            st.write(f"**Template:** {orchestrator_template.get('template_name', orchestrator_template.get('name', 'Unknown'))}")
            uploaded_at = orchestrator_template.get('uploaded_at', '')
            if uploaded_at:
                try:
                    date_obj = datetime.fromisoformat(uploaded_at.replace('Z', '+00:00'))
                    st.write(f"**Uploaded:** {date_obj.strftime('%Y-%m-%d %H:%M')}")
                except:
                    st.write(f"**Uploaded:** {uploaded_at[:16] if len(uploaded_at) >= 16 else 'N/A'}")
        with col2:
            template_data = orchestrator_template.get('template_data', {})
            node_count    = len(template_data.get('drawflow', {}).get('nodes', []))
            st.metric("Nodes", node_count)
        with col3:
            if st.button("🗑️ Delete Template", key="delete_orchestrator_tab", type="secondary"):
                try:
                    update_system_setting(ORCHESTRATOR_SETTINGS_KEY, None)
                    st.success("✅ Orchestrator template deleted!")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error deleting orchestrator: {e}")

        with st.expander("🔍 View Orchestrator Details"):
            st.json(orchestrator_template)
    else:
        st.info("ℹ️ No orchestrator template configured yet")

    st.markdown("---")
    st.markdown("### 📂 Upload New Template")

    upload_method = st.radio(
        "Choose input method:",
        ["Upload JSON File", "Paste JSON Data"],
        key="orchestrator_upload_method_tab",
        horizontal=True
    )

    orchestrator_data = None

    if upload_method == "Upload JSON File":
        uploaded_file = st.file_uploader(
            "Choose orchestrator JSON file",
            type=['json'],
            key="orchestrator_file_upload_tab",
            help="Upload the workflow orchestrator JSON file exported from Automa"
        )
        if uploaded_file is not None:
            try:
                orchestrator_data = json.load(uploaded_file)
                st.success(f"✅ File **'{uploaded_file.name}'** loaded successfully!")
            except json.JSONDecodeError as e:
                st.error(f"❌ Invalid JSON file: {e}")
            except Exception as e:
                st.error(f"❌ Error loading file: {e}")
    else:
        default_orchestrator = (
            '{"extVersion":"1.30.00","name":"manual","icon":"riGlobalLine","table":[],'
            '"version":"1.30.00","drawflow":{"nodes":[{"id":"d2kb6g5","type":"BlockBasic",'
            '"initialized":false,"position":{"x":-598,"y":165},"data":{"activeInInput":false,'
            '"contextMenuName":"","contextTypes":[],"date":"","days":[],"delay":5,'
            '"description":"","disableBlock":false,"interval":60,"isUrlRegex":false,'
            '"observeElement":{"baseElOptions":{"attributeFilter":[],"attributes":false,'
            '"characterData":false,"childList":true,"subtree":false},"baseSelector":"",'
            '"matchPattern":"","selector":"","targetOptions":{"attributeFilter":[],'
            '"attributes":false,"characterData":false,"childList":true,"subtree":false}},'
            '"parameters":[],"preferParamsInTab":false,"shortcut":"","time":"00:00",'
            '"type":"manual","url":""},"label":"trigger"}],"edges":[],"position":[0,0],'
            '"zoom":1,"viewport":{"x":0,"y":0,"zoom":1}},"settings":{"publicId":"",'
            '"blockDelay":0,"saveLog":true,"debugMode":false,"restartTimes":3,'
            '"notification":true,"execContext":"popup","reuseLastState":false,'
            '"inputAutocomplete":true,"onError":"stop-workflow","executedBlockOnWeb":false,'
            '"insertDefaultColumn":false,"defaultColumnName":"column"},"globalData":"{}",'
            '"description":"","includedWorkflows":{}}'
        )

        json_text = st.text_area(
            "Paste orchestrator JSON here:",
            value=default_orchestrator,
            height=200,
            key="orchestrator_json_paste_tab",
        )

        if json_text:
            try:
                orchestrator_data = json.loads(json_text.strip().lstrip('\ufeff'))
                st.success("✅ JSON parsed successfully!")
            except json.JSONDecodeError as e:
                st.error(f"❌ Invalid JSON: {e}")

    if orchestrator_data:
        st.markdown("---")
        st.markdown("### 🔍 Preview & Save")

        node_count = len(orchestrator_data.get('drawflow', {}).get('nodes', []))
        edge_count = len(orchestrator_data.get('drawflow', {}).get('edges', []))

        col1, col2, col3, col4 = st.columns(4)
        with col1: st.metric("Name",    orchestrator_data.get('name', 'Unknown'))
        with col2: st.metric("Version", orchestrator_data.get('version', 'N/A'))
        with col3: st.metric("Nodes",   node_count)
        with col4: st.metric("Edges",   edge_count)

        with st.expander("🔍 View Raw JSON"):
            st.json(orchestrator_data)

        if st.button("💾 Save Orchestrator Template", key="save_orchestrator_btn_tab", type="primary", use_container_width=True):
            try:
                orchestrator_doc = {
                    'template_name': orchestrator_data.get('name', 'orchestrator'),
                    'template_data': orchestrator_data,
                    'uploaded_at':   datetime.now().isoformat(),
                    'node_count':    node_count,
                    'version':       orchestrator_data.get('version', '1.0'),
                    'ext_version':   orchestrator_data.get('extVersion', ''),
                }
                update_system_setting(ORCHESTRATOR_SETTINGS_KEY, orchestrator_doc)
                st.success("✅ Orchestrator template saved!")
                st.info(f"📌 Saved as **{orchestrator_doc['template_name']}** with {node_count} node(s)")
                st.balloons()
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error saving orchestrator: {e}")
                import traceback
                st.error(traceback.format_exc())


def _render_statistics_tab(weekly_settings: dict):
    """Statistics tab — all configured execution settings with Edit & Delete."""
    st.markdown("### 📋 All Configured Execution Settings")

    has_any = False

    for dk in DAYS_ORDER:
        if dk in weekly_settings:
            day_config  = weekly_settings[dk]
            exec_config = day_config.get('execution_config')

            if exec_config:
                has_any      = True
                config_date  = exec_config.get('config_date', 'N/A')
                category     = exec_config.get('workflow_category', 'N/A')
                wf_type      = exec_config.get('workflow_type', 'N/A')
                collection   = exec_config.get('collection_name', 'N/A')
                enabled      = exec_config.get('enabled', True)
                max_workflows = exec_config.get('max_workflows_to_process')
                delay_min_ms  = exec_config.get('delay_min_milliseconds')
                delay_max_ms  = exec_config.get('delay_max_milliseconds')
                wait_margin   = exec_config.get('wait_margin_ms')

                with st.expander(
                    f"⚡ {day_config.get('day_name', dk.capitalize())} — {config_date}",
                    expanded=False
                ):
                    col1, col2, col3, col4 = st.columns([3, 2, 1, 1])

                    with col1:
                        st.write(f"**Category:** {category}")
                        st.write(f"**Type:** {wf_type}")
                        st.write(f"**Collection:** {collection}")
                        st.write(f"**Status:** {'✅ Enabled' if enabled else '❌ Disabled'}")
                        if max_workflows and max_workflows > 0:
                            st.write(f"**📊 Max Workflows:** {max_workflows:,}")
                        else:
                            st.write(f"**📊 Max Workflows:** All available (unlimited)")

                    with col2:
                        if delay_min_ms is not None and delay_max_ms is not None:
                            st.markdown("**⏱️ Delay Range**")
                            st.write(f"Min: {delay_min_ms / 1000:.2f}s ({delay_min_ms:,} ms)")
                            st.write(f"Max: {delay_max_ms / 1000:.2f}s ({delay_max_ms:,} ms)")
                        else:
                            st.write("**⏱️ Delay:** Not configured")

                        st.markdown("**🛡️ Wait Margin**")
                        if wait_margin is not None:
                            st.write(f"{wait_margin / 1000:.0f}s ({wait_margin:,} ms)")
                        else:
                            st.write("Not set — default 300s used")

                        if exec_config.get('updated_at'):
                            st.caption(f"Updated: {exec_config.get('updated_at')}")

                    with col3:
                        if st.button("✏️ Edit", key=f"edit_exec_stat_{dk}", use_container_width=True):
                            st.session_state[f'editing_exec_{dk}'] = True
                            st.rerun()

                    with col4:
                        if st.button("🗑️ Delete", key=f"del_exec_stat_{dk}", use_container_width=True):
                            try:
                                fresh_weekly = get_system_setting('weekly_workflow_settings', {})
                                if 'execution_config' in fresh_weekly.get(dk, {}):
                                    del fresh_weekly[dk]['execution_config']
                                    update_system_setting('weekly_workflow_settings', fresh_weekly)
                                    st.success("✅ Deleted!")
                                    st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error: {e}")

                    # ── Inline edit form ──────────────────────────────────────
                    if st.session_state.get(f'editing_exec_{dk}'):
                        st.markdown("---")
                        st.markdown(f"**✏️ Editing {dk.title()} execution configuration**")

                        available_categories = get_saved_workflow_categories()

                        with st.form(key=f"edit_exec_form_{dk}"):
                            edit_enabled = st.toggle(
                                "Enable Execution",
                                value=exec_config.get('enabled', True),
                                key=f"edit_exec_enabled_{dk}"
                            )

                            ec1, ec2 = st.columns(2)

                            with ec1:
                                curr_cat  = exec_config.get('workflow_category', '')
                                cat_idx   = 0
                                if curr_cat.title() in available_categories:
                                    try:
                                        cat_idx = available_categories.index(curr_cat.title())
                                    except ValueError:
                                        pass
                                edit_category = st.selectbox(
                                    "Workflow Category",
                                    options=available_categories,
                                    index=cat_idx,
                                    key=f"edit_exec_cat_{dk}"
                                )

                                wf_types    = get_workflow_types_for_category(edit_category)
                                curr_wf     = exec_config.get('workflow_type', '')
                                wf_idx      = 0
                                if curr_wf in wf_types:
                                    try:
                                        wf_idx = wf_types.index(curr_wf)
                                    except ValueError:
                                        pass
                                edit_wf_type = st.selectbox(
                                    "Workflow Type",
                                    options=wf_types,
                                    index=wf_idx,
                                    key=f"edit_exec_wf_{dk}"
                                ) if wf_types else st.text_input(
                                    "Workflow Type", value=curr_wf, key=f"edit_exec_wf_text_{dk}"
                                )

                                collections = get_collections_for_category_and_type(edit_category, edit_wf_type)
                                if collections:
                                    coll_options = [
                                        f"{c['collection_name']} ({c['ready_count']} ready / {c['total_count']} total)"
                                        for c in collections
                                    ]
                                    coll_values = [c['collection_name'] for c in collections]
                                    curr_coll   = exec_config.get('collection_name', '')
                                    coll_idx    = 0
                                    if curr_coll in coll_values:
                                        try:
                                            coll_idx = coll_values.index(curr_coll)
                                        except ValueError:
                                            pass
                                    edit_coll_display = st.selectbox(
                                        "Collection",
                                        options=coll_options,
                                        index=coll_idx,
                                        key=f"edit_exec_coll_{dk}"
                                    )
                                    edit_collection = coll_values[coll_options.index(edit_coll_display)]
                                else:
                                    edit_collection = st.text_input(
                                        "Collection",
                                        value=exec_config.get('collection_name', ''),
                                        key=f"edit_exec_coll_text_{dk}"
                                    )

                            with ec2:
                                edit_max_workflows = st.number_input(
                                    "Max Workflows (0 = all)",
                                    min_value=0, max_value=10000,
                                    value=int(exec_config.get('max_workflows_to_process', 0)),
                                    key=f"edit_exec_max_wf_{dk}"
                                )

                                edit_delay_min = st.number_input(
                                    "Min Delay (ms)",
                                    min_value=1000, max_value=120000,
                                    value=int(exec_config.get('delay_min_milliseconds', 10000)),
                                    step=1000, key=f"edit_exec_dmin_{dk}"
                                )

                                edit_delay_max = st.number_input(
                                    "Max Delay (ms)",
                                    min_value=1000, max_value=300000,
                                    value=int(exec_config.get('delay_max_milliseconds', 20000)),
                                    step=1000, key=f"edit_exec_dmax_{dk}"
                                )

                                edit_wait_margin = st.number_input(
                                    "Wait Margin (ms)",
                                    min_value=0, max_value=3600000,
                                    value=int(exec_config.get('wait_margin_ms', 300000)),
                                    step=30000, key=f"edit_exec_margin_{dk}"
                                )

                            save_col, cancel_col = st.columns(2)
                            with save_col:
                                save_edit = st.form_submit_button(
                                    "💾 Save Changes", type="primary", use_container_width=True
                                )
                            with cancel_col:
                                cancel_edit = st.form_submit_button(
                                    "❌ Cancel", use_container_width=True
                                )

                        if save_edit:
                            try:
                                fresh_weekly = get_system_setting('weekly_workflow_settings', {})
                                fresh_weekly[dk]['execution_config'].update({
                                    'enabled':                  edit_enabled,
                                    'workflow_category':        edit_category.lower(),
                                    'workflow_type':            edit_wf_type.lower() if hasattr(edit_wf_type, 'lower') else edit_wf_type,
                                    'collection_name':          edit_collection,
                                    'max_workflows_to_process': int(edit_max_workflows),
                                    'delay_min_milliseconds':   int(edit_delay_min),
                                    'delay_max_milliseconds':   int(edit_delay_max),
                                    'wait_margin_ms':           int(edit_wait_margin),
                                    'updated_at':               datetime.now().isoformat(),
                                })
                                update_system_setting('weekly_workflow_settings', fresh_weekly)
                                st.session_state[f'editing_exec_{dk}'] = False
                                st.success(f"✅ {dk.title()} execution config updated!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Failed to save: {e}")

                        if cancel_edit:
                            st.session_state[f'editing_exec_{dk}'] = False
                            st.rerun()

    if not has_any:
        st.info("ℹ️ No execution configurations saved yet. Use the **📋 Configuration** tab to create one.")

    st.markdown("---")
    st.markdown("### 🎼 Orchestrator Template Status")

    orch = get_system_setting(ORCHESTRATOR_SETTINGS_KEY, None)
    if orch:
        col1, col2 = st.columns(2)
        with col1:
            st.success("✅ Orchestrator template configured")
            st.write(f"**Name:** {orch.get('template_name', 'Unknown')}")
            uploaded_at = orch.get('uploaded_at', '')
            if uploaded_at:
                try:
                    dt = datetime.fromisoformat(uploaded_at.replace('Z', '+00:00'))
                    st.write(f"**Uploaded:** {dt.strftime('%Y-%m-%d %H:%M')}")
                except:
                    pass
        with col2:
            td         = orch.get('template_data', {})
            node_count = len(td.get('drawflow', {}).get('nodes', []))
            st.metric("Nodes", node_count)
    else:
        st.warning("⚠️ No orchestrator template uploaded. Go to **📤 Upload Template** to add one.")


# ============================================================================
# MAIN RENDERING FUNCTION
# ============================================================================

def render_execution_config():
    st.header("⚡ Execution Configuration")
    st.markdown("*Configure workflow execution settings*")

    tab_config, tab_upload, tab_stats = st.tabs([
        "📋 Configuration",
        "📤 Upload Template",
        "📊 Statistics",
    ])

    # =========================================================================
    # TAB 1 — Configuration
    # =========================================================================
    with tab_config:
        try:
            weekly_settings = get_system_setting('weekly_workflow_settings', {})
            if not isinstance(weekly_settings, dict):
                weekly_settings = {}

            with st.form("execution_config_form"):

                # ── Schedule Date ─────────────────────────────────────────────
                st.markdown("### 📅 Schedule Date")
                st.markdown("*Used for single-day saves. Ignored when saving to all 7 days.*")
                available_dates   = get_future_dates(8)
                date_options      = [date_str for _, date_str in available_dates]
                date_values       = [d        for d, _        in available_dates]

                selected_date_str = st.selectbox("Schedule Date", options=date_options, index=0)
                selected_index    = date_options.index(selected_date_str)
                scheduled_date    = date_values[selected_index]

                config_exists, day_key, existing_config = check_execution_config_exists(weekly_settings, scheduled_date)
                day_name = scheduled_date.strftime('%A')

                if config_exists:
                    st.success(f"✅ Configuration exists for {day_name}, {scheduled_date.strftime('%B %d, %Y')}")
                else:
                    st.info(f"ℹ️ No configuration found for {day_name}, {scheduled_date.strftime('%B %d, %Y')}")

                st.markdown("---")

                # ── Target Selection ──────────────────────────────────────────
                st.markdown("### 🎯 Target Selection")
                available_categories = get_saved_workflow_categories()

                if not available_categories:
                    st.warning("⚠️ No workflow categories found. Configure categories in 'Automa Workflow Config' first.")
                    st.form_submit_button("💾 Save Configuration", disabled=True)
                    return

                col1, col2 = st.columns(2)

                with col1:
                    current_category = existing_config.get('workflow_category', '')
                    category_index   = 0
                    if current_category and current_category.title() in available_categories:
                        try:
                            category_index = available_categories.index(current_category.title())
                        except ValueError:
                            pass
                    selected_category = st.selectbox("Workflow Category", options=available_categories, index=category_index)

                with col2:
                    workflow_types = get_workflow_types_for_category(selected_category)
                    if workflow_types:
                        current_wf_type = existing_config.get('workflow_type', '')
                        wf_type_index   = 0
                        if current_wf_type in workflow_types:
                            try:
                                wf_type_index = workflow_types.index(current_wf_type)
                            except ValueError:
                                pass
                        selected_workflow_type = st.selectbox("Workflow Type", options=workflow_types, index=wf_type_index)
                    else:
                        st.warning("⚠️ No workflow types found for this category")
                        selected_workflow_type = ""

                # ── Collection Selection ──────────────────────────────────────
                selected_collection = ""
                collections         = []

                if selected_category and selected_workflow_type:
                    st.markdown("---")
                    collections = get_collections_for_category_and_type(selected_category, selected_workflow_type)

                    if collections:
                        collection_options = [
                            f"{c['collection_name']} ({c['ready_count']} ready / {c['total_count']} total)"
                            for c in collections
                        ]
                        collection_values  = [c['collection_name'] for c in collections]

                        current_collection = existing_config.get('collection_name', '')
                        collection_index   = 0
                        if current_collection in collection_values:
                            try:
                                collection_index = collection_values.index(current_collection)
                            except ValueError:
                                pass

                        selected_collection_display = st.selectbox("Collection", options=collection_options, index=collection_index)
                        selected_collection         = collection_values[collection_options.index(selected_collection_display)]

                        selected_coll_data = collections[collection_index]
                        ready_count        = selected_coll_data['ready_count']
                        total_count        = selected_coll_data['total_count']

                        if ready_count > 0:
                            st.success(f"✅ {ready_count} workflows ready to execute out of {total_count} total")
                        else:
                            st.warning(f"⚠️ No ready workflows in this collection ({total_count} total, 0 ready).")
                            try:
                                from pymongo import MongoClient
                                import os
                                debug_client = MongoClient(
                                    os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
                                    serverSelectionTimeoutMS=5000
                                )
                                sample = list(debug_client['messages_db'].workflow_metadata.find(
                                    {"collection_name": selected_collection},
                                    {"status": 1, "workflow_name": 1, "_id": 0}
                                ).limit(3))
                                if sample:
                                    statuses = [doc.get('status', 'unknown') for doc in sample]
                                    st.caption(f"🔍 Sample status values in DB: {', '.join(set(statuses))}")
                                    st.caption(f"   (Looking for 'ready' or 'ready_to_execute')")
                                debug_client.close()
                            except Exception:
                                pass
                    else:
                        st.warning(f"⚠️ No collections found for {selected_category} → {selected_workflow_type}")

                st.markdown("---")

                # ── Execution Settings ────────────────────────────────────────
                st.markdown("### ⚙️ Execution Settings")

                enabled = st.toggle(
                    "Enable Execution",
                    value=existing_config.get('enabled', True) if existing_config else True
                )

                st.markdown("#### 📊 Workflow Limit")
                current_max_workflows = existing_config.get('max_workflows_to_process', 0) if existing_config else 0

                col_workflow_limit, col_workflow_info = st.columns([2, 3])
                with col_workflow_limit:
                    max_workflows = st.number_input(
                        "Number of workflows to process",
                        min_value=0, max_value=10000,
                        value=current_max_workflows,
                        step=1,
                        help="Set to 0 to process ALL available workflows in the collection",
                        key="exec_max_workflows"
                    )
                with col_workflow_info:
                    if selected_category and selected_workflow_type and selected_collection and collections:
                        current_collection_data = next(
                            (c for c in collections if c['collection_name'] == selected_collection), None
                        )
                        if current_collection_data:
                            rdy = current_collection_data['ready_count']
                            if max_workflows == 0:
                                st.info(f"📊 Will process ALL {rdy} ready workflows")
                            elif max_workflows > rdy:
                                st.warning(f"⚠️ Limit ({max_workflows}) exceeds available workflows ({rdy})")
                            else:
                                st.success(f"📊 Will process {max_workflows} of {rdy} ready workflows")

                st.caption("💡 **0** = process all available workflows | **1+** = process only that many (oldest first)")

                # ── Delay Range ───────────────────────────────────────────────
                st.markdown("#### ⏱️ Delay Range")
                delay_col1, delay_col2 = st.columns(2)
                with delay_col1:
                    delay_min = st.number_input(
                        "Minimum Delay (milliseconds)",
                        min_value=1_000, max_value=120_000,
                        value=int(existing_config.get('delay_min_milliseconds', 10_000)) if existing_config else 10_000,
                        step=1_000, key="exec_delay_min"
                    )
                with delay_col2:
                    delay_max = st.number_input(
                        "Maximum Delay (milliseconds)",
                        min_value=1_000, max_value=300_000,
                        value=int(existing_config.get('delay_max_milliseconds', 20_000)) if existing_config else 20_000,
                        step=1_000, key="exec_delay_max"
                    )

                if delay_min > delay_max:
                    st.error("⚠️ Minimum delay cannot be greater than maximum delay")
                st.caption(f"💡 Delay range: **{delay_min / 1000:.2f}s** — **{delay_max / 1000:.2f}s**")

                # ── Wait Margin ───────────────────────────────────────────────
                st.markdown("#### 🛡️ Wait Margin")
                st.info(
                    "**How the wait time is calculated:**\n\n"
                    "The orchestrator automatically analyses every block in every sub-workflow "
                    "to estimate how long execution will take. "
                    "The **wait margin** is a safety buffer added on top of that estimate.\n\n"
                    "**Configured wait = auto-estimated time + wait margin**"
                )

                default_margin_ms = int(existing_config.get('wait_margin_ms', 300_000)) if existing_config else 300_000

                wait_margin_ms = st.number_input(
                    "Wait Margin (milliseconds)",
                    min_value=0, max_value=3_600_000,
                    value=default_margin_ms,
                    step=30_000,
                    help="Safety buffer added on top of the auto-calculated estimate (300 000 ms = 5 min)",
                    key="exec_wait_margin_ms"
                )

                margin_minutes = wait_margin_ms / 60_000
                st.caption(f"💡 Margin: **{margin_minutes:.1f} min** ({wait_margin_ms:,} ms)")

                if selected_category and selected_workflow_type and selected_collection and collections:
                    current_collection_data = next(
                        (c for c in collections if c['collection_name'] == selected_collection), None
                    )
                    if current_collection_data:
                        rdy                   = current_collection_data['ready_count']
                        workflows_to_process  = max_workflows if max_workflows > 0 else rdy
                        workflows_to_process  = min(workflows_to_process, rdy)
                        est_delay_mid_ms      = (delay_min + delay_max) / 2
                        total_delay_time      = (workflows_to_process - 1) * est_delay_mid_ms / 1000

                        st.caption(
                            f"📐 Estimated timing for {workflows_to_process} workflow(s):\n"
                            f"• Block analysis estimate: ~X minutes (depends on workflow complexity)\n"
                            f"• Total delay between workflows: {total_delay_time:.0f}s ({workflows_to_process-1} gaps × {est_delay_mid_ms/1000:.0f}s avg)\n"
                            f"• Wait margin: {margin_minutes:.1f} min"
                        )

                st.markdown("---")

                form_invalid = (
                    not (selected_category and selected_workflow_type and selected_collection)
                    or delay_min > delay_max
                )

                # ── Dual submit buttons ───────────────────────────────────────
                st.markdown("""
                    <div style='background: linear-gradient(135deg, rgba(103,58,183,0.12), rgba(33,150,243,0.12));
                                padding: 18px 20px;
                                border-radius: 10px;
                                border-left: 5px solid #7c4dff;
                                margin: 10px 0 12px 0;'>
                        <h3 style='color: #4a148c; margin: 0 0 6px 0;'>📅 Two ways to save</h3>
                        <p style='margin: 0; color: #555; font-size: 14px;'>
                            &nbsp;&nbsp;• <strong>💾 Save to Selected Day</strong> — applies only to the date chosen above<br>
                            &nbsp;&nbsp;• <strong>🗓️ Save to ALL 7 Days</strong> — applies the exact same settings to every day of the week
                        </p>
                    </div>
                """, unsafe_allow_html=True)

                btn_col1, btn_col2 = st.columns(2)
                with btn_col1:
                    submit_single = st.form_submit_button(
                        "💾 Save to Selected Day",
                        type="primary",
                        use_container_width=True,
                        disabled=form_invalid,
                    )
                with btn_col2:
                    submit_all_week = st.form_submit_button(
                        "🗓️ Save to ALL 7 Days",
                        use_container_width=True,
                        disabled=form_invalid,
                    )

            # ── Handle submissions (outside form) ─────────────────────────────
            if submit_single or submit_all_week:
                if form_invalid:
                    st.error("❌ Please fix all validation errors before saving!")
                else:
                    try:
                        fresh_weekly = get_system_setting('weekly_workflow_settings', {})

                        if submit_single:
                            # ── Save to single day ────────────────────────────
                            fresh_weekly[day_key] = _build_execution_day_config(
                                existing_day_cfg       = fresh_weekly.get(day_key, {}),
                                day_key                = day_key,
                                day_name               = day_name,
                                config_date_iso        = scheduled_date.isoformat(),
                                selected_category      = selected_category,
                                selected_workflow_type = selected_workflow_type,
                                selected_collection    = selected_collection,
                                enabled                = enabled,
                                max_workflows          = max_workflows,
                                delay_min              = delay_min,
                                delay_max              = delay_max,
                                wait_margin_ms         = wait_margin_ms,
                            )
                            update_system_setting('weekly_workflow_settings', fresh_weekly)

                            st.success(f"✅ Configuration saved for {day_name}!")
                            st.info(f"📂 Category: {selected_category} → {selected_workflow_type}")
                            st.info(f"📁 Collection: {selected_collection}")
                            st.info(f"📊 Workflow limit: {'All available (unlimited)' if max_workflows == 0 else f'{max_workflows} workflow(s)'}")
                            st.info(f"⏱️ Delay range: {delay_min / 1000:.2f}s – {delay_max / 1000:.2f}s")
                            st.info(f"🛡️ Wait margin: {wait_margin_ms / 60_000:.1f} min")

                        elif submit_all_week:
                            # ── Save to ALL 7 days ────────────────────────────
                            today         = datetime.now().date()
                            weekday_today = today.weekday()   # 0 = Monday
                            saved_days    = []

                            for offset, dk in enumerate(DAYS_ORDER):
                                days_until = (offset - weekday_today) % 7
                                target_dt  = today + timedelta(days=days_until)
                                dn         = target_dt.strftime('%A')

                                fresh_weekly[dk] = _build_execution_day_config(
                                    existing_day_cfg       = fresh_weekly.get(dk, {}),
                                    day_key                = dk,
                                    day_name               = dn,
                                    config_date_iso        = target_dt.isoformat(),
                                    selected_category      = selected_category,
                                    selected_workflow_type = selected_workflow_type,
                                    selected_collection    = selected_collection,
                                    enabled                = enabled,
                                    max_workflows          = max_workflows,
                                    delay_min              = delay_min,
                                    delay_max              = delay_max,
                                    wait_margin_ms         = wait_margin_ms,
                                )
                                saved_days.append(f"{dn} ({target_dt.strftime('%b %d')})")

                            update_system_setting('weekly_workflow_settings', fresh_weekly)

                            st.success("🎉 Execution settings applied to **all 7 days** of the week!")
                            st.markdown("**Days updated:**")
                            for sd in saved_days:
                                st.write(f"✅ {sd}")
                            st.info(f"📂 Category: {selected_category} → {selected_workflow_type}")
                            st.info(f"📁 Collection: {selected_collection}")
                            st.info(f"📊 Workflow limit: {'All available (unlimited)' if max_workflows == 0 else f'{max_workflows} workflow(s)'}")
                            st.info(f"⏱️ Delay range: {delay_min / 1000:.2f}s – {delay_max / 1000:.2f}s")
                            st.info(f"🛡️ Wait margin: {wait_margin_ms / 60_000:.1f} min")

                        st.balloons()
                        st.rerun()

                    except Exception as e:
                        st.error(f"❌ Error: {e}")
                        import traceback
                        st.error(traceback.format_exc())

        except Exception as e:
            st.error(f"❌ Error loading execution configuration: {e}")
            import traceback
            st.error(traceback.format_exc())

    # =========================================================================
    # TAB 2 — Upload Template
    # =========================================================================
    with tab_upload:
        _render_upload_template_tab()

    # =========================================================================
    # TAB 3 — Statistics (with Edit & Delete)
    # =========================================================================
    with tab_stats:
        try:
            weekly_settings = get_system_setting('weekly_workflow_settings', {})
            if not isinstance(weekly_settings, dict):
                weekly_settings = {}
            _render_statistics_tab(weekly_settings)
        except Exception as e:
            st.error(f"❌ Error loading statistics: {e}")
            import traceback
            st.error(traceback.format_exc())
