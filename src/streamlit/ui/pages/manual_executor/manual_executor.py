"""
Manual Executor Page
Handles manual template upload and workflow package download with single-button execution.
Replaces the ReverseDAGsPage in the navigation.
"""
import os
import streamlit as st
from datetime import datetime, date
import json
import zipfile
import io
import random
from typing import Dict, Any, List
from bson import ObjectId
import logging

logger = logging.getLogger(__name__)


class ManualExecutor:
    """Manual workflow execution and package management page."""

    def __init__(self, db_manager):
        """Initialize the ManualExecutor page."""
        self.db_manager = db_manager

    MANUAL_TEMPLATE_FILE_PATHS = [
        os.path.join(os.getcwd(), 'src', 'templates', 'manual_orchestrator_template.automa.json'),
        os.path.join(os.getcwd(), 'templates', 'manual_orchestrator_template.automa.json'),
        '/opt/airflow/src/templates/manual_orchestrator_template.automa.json',
        '/app/src/templates/manual_orchestrator_template.automa.json',
        os.path.join(os.path.dirname(__file__), '..', '..', '..', 'templates', 'manual_orchestrator_template.automa.json'),
    ]

    def render(self):
        """Render the Manual Executor page."""
        st.title("🚀 Manual Executor")
        st.markdown("*Upload templates and download workflow packages for manual execution*")
        st.markdown("---")

        tab1, tab2, tab3 = st.tabs([
            "📤 Upload Template",
            "📥 Download Workflows",
            "⚙️ Manage Template"
        ])

        with tab1:
            self._render_template_upload()

        with tab2:
            self._render_download_workflows()

        with tab3:
            self._render_manage_template()

    # ============================================================================
    # TEMPLATE FILE HELPERS
    # ============================================================================

    def _load_manual_template_from_file(self) -> dict | None:
        """Load the manual orchestrator template from src/templates/."""
        for path in self.MANUAL_TEMPLATE_FILE_PATHS:
            normalized = os.path.normpath(path)
            if os.path.isfile(normalized):
                try:
                    with open(normalized, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    logger.info(f"Loaded manual template from {normalized}")
                    return data
                except Exception as e:
                    logger.warning(f"Found file at {normalized} but failed to parse: {e}")
        return None

    def _get_or_auto_save_manual_template(self) -> dict | None:
        """
        Return the saved manual_execution_template setting.
        If none is saved yet, auto-load from file, save it, and return it.
        """
        saved = self._get_system_setting('manual_execution_template', None)
        if saved:
            return saved

        file_data = self._load_manual_template_from_file()
        if not file_data:
            return None

        template_to_save = {
            'template_data':        file_data,
            'uploaded_at':          datetime.now().isoformat(),
            'template_name':        file_data.get('name', 'manual_orchestrator_template'),
            'source':               'auto_loaded_from_file',
            'gap_between_workflows': {
                'min_milliseconds': 500,
                'max_milliseconds': 5000
            }
        }

        try:
            self._update_system_setting('manual_execution_template', template_to_save)
            logger.info("Auto-saved manual template from file to system settings")
        except Exception as e:
            logger.warning(f"Could not auto-save manual template: {e}")

        return template_to_save

    # ============================================================================
    # TABS
    # ============================================================================

    def _render_download_workflows(self):
        """Render download filtered workflows section - SINGLE BUTTON VERSION."""
        st.subheader("📥 Download Filtered Workflows")
        st.caption("Download workflows that were filtered and assigned links")

        manual_template = self._get_or_auto_save_manual_template()
        if not manual_template:
            st.warning(
                "⚠️ No manual execution template found and could not auto-load from "
                "`src/templates/manual_orchestrator_template.automa.json`. "
                "Please upload one in the **Upload Template** tab."
            )
            return

        source_label = " *(auto-loaded from file)*" if manual_template.get('source') == 'auto_loaded_from_file' else ""
        st.success(f"✅ Template loaded: **{manual_template.get('template_name', 'Unknown')}**{source_label}")

        gap_config = manual_template.get('gap_between_workflows', {})
        if gap_config:
            min_gap = gap_config.get('min_milliseconds', 500)
            max_gap = gap_config.get('max_milliseconds', 5000)
            st.info(f"⏱️ Gap between workflows: {min_gap/1000:.2f}s - {max_gap/1000:.2f}s")
        else:
            st.warning("⚠️ No gap configuration found. Using defaults: 0.5s - 5.0s")
            gap_config = {'min_milliseconds': 500, 'max_milliseconds': 5000}

        weekly_settings = self._get_system_setting('weekly_workflow_settings', {})
        current_day = datetime.now().strftime('%A').lower()
        day_config = weekly_settings.get(current_day, {})
        filtering_config = day_config.get('filtering_config', {})

        st.markdown("---")

        filter_date = st.date_input(
            "Filter by date:",
            value=datetime.now().date(),
            key="download_filter_date"
        )

        try:
            category = filtering_config.get('destination_category')
            workflow_type = filtering_config.get('workflow_type_name')
            collection_name = filtering_config.get('collection_name')

            filtered_workflows = self._get_filtered_workflows_with_links(
                category=category,
                workflow_type=workflow_type,
                collection_name=collection_name,
                date_filter=filter_date
            )

            if not filtered_workflows:
                st.warning("⚠️ No workflows found")
                return

            st.success(f"✅ Found **{len(filtered_workflows)}** workflows")

            delay_count = len(filtered_workflows) - 1
            if delay_count > 0:
                st.info(f"🔄 Will add **{delay_count}** delay blocks between workflows")

            st.markdown("---")

            if st.button("🚀 Generate & Download ZIP Package", type="primary", use_container_width=True, key="generate_download_single_btn"):
                with st.spinner("Generating workflows with delays..."):
                    try:
                        template_data = manual_template.get('template_data', {})

                        master_workflow = self._create_master_execution_workflow_with_delays(
                            filtered_workflows,
                            template_data,
                            gap_config
                        )

                        if not master_workflow:
                            st.error("❌ Failed to create master workflow")
                            return

                        zip_buffer = self._create_zip_package(filtered_workflows, master_workflow)

                        if not zip_buffer:
                            st.error("❌ Failed to create ZIP")
                            return

                        success_update = self._update_links_success_status(filtered_workflows)

                        if success_update.get('success'):
                            st.success(f"✅ Marked {success_update.get('updated_count', 0)} links as SUCCESS")
                        else:
                            st.warning(f"⚠️ Could not update success status: {success_update.get('error', 'Unknown error')}")

                        mark_success = self._mark_links_as_executed(filtered_workflows)

                        if mark_success.get('success'):
                            st.success(f"✅ Marked {mark_success.get('postgres_updated', 0)} links as executed")

                        now = datetime.now()
                        date_str = now.strftime("%Y-%m-%d")
                        time_str = now.strftime("%H-%M-%S")
                        day_str = now.strftime("%A")
                        filename = f"workflows_{date_str}_{time_str}_{day_str}.zip"

                        st.info(f"📦 Package name: `{filename}`")

                        st.download_button(
                            label="📥 Download ZIP Package",
                            data=zip_buffer,
                            file_name=filename,
                            mime="application/zip",
                            use_container_width=True,
                            key="download_zip_btn_after_generation"
                        )

                        st.success("✅ Package generated successfully and ready to download!")
                        st.balloons()

                    except Exception as e:
                        st.error(f"❌ Error: {e}")
                        import traceback
                        st.code(traceback.format_exc())

        except Exception as e:
            st.error(f"❌ Error: {e}")
            import traceback
            st.code(traceback.format_exc())

    def _render_template_upload(self):
        """Render manual template upload section."""
        st.subheader("📤 Manual Template Upload")
        st.caption("Upload a workflow template for manual execution with configurable delays")

        st.markdown("---")

        file_template_data = self._load_manual_template_from_file()

        if file_template_data:
            default_manual_template = json.dumps(file_template_data, indent=2)
            st.info("📂 Default template auto-loaded from `src/templates/manual_orchestrator_template.automa.json`")
        else:
            default_manual_template = r'''{"extVersion":"1.30.00","name":"manual", ... }'''

        input_method = st.radio(
            "Choose input method:",
            ["Upload JSON File", "Paste JSON Data"],
            key="manual_template_input_method",
            horizontal=True
        )

        template_data = None

        if input_method == "Upload JSON File":
            uploaded_file = st.file_uploader(
                "Choose a JSON file",
                type=['json'],
                key="manual_template_file_upload"
            )

            if uploaded_file is not None:
                try:
                    template_data = json.load(uploaded_file)
                    st.success(f"✅ File '{uploaded_file.name}' loaded successfully!")
                except Exception as e:
                    st.error(f"❌ Error: {e}")
        else:
            json_text = st.text_area(
                "Paste your workflow JSON here:",
                value=default_manual_template,
                height=300,
                key="manual_template_json_paste",
                help="Default template loaded from file. Clear and paste your own JSON if needed."
            )

            if json_text:
                try:
                    template_data = json.loads(json_text)
                    st.success("✅ JSON loaded successfully!")
                except json.JSONDecodeError as e:
                    st.error(f"❌ Invalid JSON: {e}")
                except Exception as e:
                    st.error(f"❌ Error: {e}")

        if template_data:
            st.markdown("---")
            st.markdown("### ⏱️ Gap Between Workflows Configuration")
            st.caption("Configure the delay range between workflow executions in milliseconds")

            existing_template   = self._get_system_setting('manual_execution_template', None)
            existing_gap_config = {}

            if existing_template:
                existing_gap_config = existing_template.get('gap_between_workflows', {
                    'min_milliseconds': 500,
                    'max_milliseconds': 5000
                })

            gap_col1, gap_col2 = st.columns(2)

            with gap_col1:
                gap_min = st.number_input(
                    "Minimum Gap (milliseconds)",
                    min_value=100, max_value=60000,
                    value=int(existing_gap_config.get('min_milliseconds', 500)),
                    step=100,
                    key="gap_min_input"
                )

            with gap_col2:
                gap_max = st.number_input(
                    "Maximum Gap (milliseconds)",
                    min_value=100, max_value=120000,
                    value=int(existing_gap_config.get('max_milliseconds', 5000)),
                    step=100,
                    key="gap_max_input"
                )

            if gap_min > gap_max:
                st.error("⚠️ Minimum gap cannot be greater than maximum gap")

            st.caption(f"💡 Current range: {gap_min/1000:.2f}s - {gap_max/1000:.2f}s")

            st.info("""
            **How it works:**
            - A delay block will be inserted between each workflow execution
            - The delay duration will be randomly selected between the minimum and maximum values
            - This helps prevent rate limiting and makes automation look more natural
            """)

            st.markdown("---")
            st.markdown("### 📋 Template Preview")

            template_name = template_data.get('name', 'Unknown')
            st.write(f"**Name:** {template_name}")

            drawflow = template_data.get('drawflow', {})
            nodes    = drawflow.get('nodes', [])
            edges    = drawflow.get('edges', [])

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Nodes", len(nodes))
            with col2:
                st.metric("Edges", len(edges))
            with col3:
                included_workflows = template_data.get('includedWorkflows', {})
                st.metric("Included Workflows", len(included_workflows))

            st.write(f"**Gap Between Workflows:** {gap_min/1000:.2f}s - {gap_max/1000:.2f}s")

            with st.expander("🔍 View Raw Template JSON"):
                st.json(template_data)

            st.markdown("---")

            if st.button(
                "💾 Save Template with Gap Configuration",
                type="primary",
                use_container_width=True,
                disabled=(gap_min > gap_max),
                key="save_template_button"
            ):
                try:
                    template_to_save = {
                        'template_data':        template_data,
                        'uploaded_at':          datetime.now().isoformat(),
                        'template_name':        template_name,
                        'gap_between_workflows': {
                            'min_milliseconds': int(gap_min),
                            'max_milliseconds': int(gap_max)
                        }
                    }
                    self._update_system_setting('manual_execution_template', template_to_save)
                    st.success("✅ Template saved with gap configuration!")
                    st.info(f"📊 Gap range: {gap_min/1000:.2f}s - {gap_max/1000:.2f}s")
                    st.balloons()
                except Exception as e:
                    st.error(f"❌ Error saving template: {e}")
                    import traceback
                    st.code(traceback.format_exc())
        else:
            st.info("👆 Upload or paste a template to configure gap settings")

    def _render_manage_template(self):
        """Render template management — update gap config, delete saved template, or reset executed links."""
        st.subheader("⚙️ Manage Saved Template")

        existing = self._get_system_setting('manual_execution_template', None)

        if not existing:
            st.info("ℹ️ No saved template found. Upload one in the **Upload Template** tab.")
            return

        st.success(f"✅ Saved template: **{existing.get('template_name', 'Unknown')}**")
        st.caption(f"Saved at: {existing.get('uploaded_at', 'Unknown')}")
        if existing.get('updated_at'):
            st.caption(f"Last updated: {existing.get('updated_at')}")

        # ── Update gap config ─────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("### ✏️ Update Gap Configuration")
        st.caption("Change the min/max delay between workflows without re-uploading the template JSON")

        gap = existing.get('gap_between_workflows', {'min_milliseconds': 500, 'max_milliseconds': 5000})

        col1, col2 = st.columns(2)
        with col1:
            new_min = st.number_input(
                "Min Gap (ms)",
                min_value=100, max_value=60000,
                value=int(gap.get('min_milliseconds', 500)),
                step=100,
                key="manage_gap_min"
            )
        with col2:
            new_max = st.number_input(
                "Max Gap (ms)",
                min_value=100, max_value=120000,
                value=int(gap.get('max_milliseconds', 5000)),
                step=100,
                key="manage_gap_max"
            )

        if new_min > new_max:
            st.error("⚠️ Min gap cannot be greater than max gap")
        else:
            st.caption(f"Range: {new_min/1000:.2f}s – {new_max/1000:.2f}s")
            if st.button("💾 Update Gap Config", type="primary", key="update_gap_btn"):
                try:
                    existing['gap_between_workflows'] = {
                        'min_milliseconds': int(new_min),
                        'max_milliseconds': int(new_max)
                    }
                    existing['updated_at'] = datetime.now().isoformat()
                    self._update_system_setting('manual_execution_template', existing)
                    st.success(f"✅ Gap configuration updated to {new_min/1000:.2f}s – {new_max/1000:.2f}s!")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Failed to update: {e}")

        # ── Delete saved template ─────────────────────────────────────────────
        st.markdown("---")
        st.markdown("### 🗑️ Delete Saved Template")
        st.warning(
            "⚠️ This removes the saved template from MongoDB system_settings. "
            "On the next visit the system will attempt to auto-reload from "
            "`src/templates/manual_orchestrator_template.automa.json`."
        )

        confirm_delete = st.checkbox(
            "I confirm I want to delete the saved template",
            key="confirm_delete_template"
        )

        if st.button(
            "🗑️ Delete Template",
            type="secondary",
            disabled=not confirm_delete,
            key="delete_template_btn"
        ):
            try:
                self._delete_system_setting('manual_execution_template')
                st.success("✅ Template deleted successfully. Reload the page to see changes.")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Failed to delete: {e}")

        # ── Reset executed links ──────────────────────────────────────────────
        st.markdown("---")
        st.markdown("### 🔄 Reset Executed Links")
        st.caption(
            "Undo a download — marks links as un-executed in PostgreSQL and MongoDB "
            "so they reappear in the next download run."
        )

        raw_ids = st.text_input(
            "Paste comma-separated link IDs to reset:",
            placeholder="e.g. 101, 102, 103",
            key="reset_link_ids_input"
        )

        if st.button("🔄 Reset Links", type="secondary", key="reset_links_btn"):
            if not raw_ids.strip():
                st.error("❌ Please enter at least one link ID.")
            else:
                try:
                    link_ids = [int(x.strip()) for x in raw_ids.split(',') if x.strip()]
                    result = self._reset_executed_links(link_ids)
                    if result['success']:
                        st.success(f"✅ Reset {result['reset_count']} link(s) successfully.")
                    else:
                        st.error(f"❌ Reset failed: {result.get('error')}")
                except ValueError:
                    st.error("❌ Invalid input — all IDs must be integers.")

    # ============================================================================
    # DATABASE HELPERS
    # ============================================================================

    def _get_mongo_connection(self):
        """Get MongoDB connection."""
        try:
            from pymongo import MongoClient
            client = MongoClient(
                os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
                serverSelectionTimeoutMS=5000
            )
            return client['messages_db']
        except Exception as e:
            st.error(f"Failed to connect to MongoDB: {e}")
            raise

    def _get_system_setting(self, key: str, default=None):
        """Get system setting from MongoDB."""
        try:
            mongo_db = self._get_mongo_connection()
            settings_collection = mongo_db['system_settings']
            result = settings_collection.find_one({'key': key})
            return result.get('value', default) if result else default
        except Exception as e:
            logger.error(f"Error getting system setting {key}: {e}")
            return default

    def _update_system_setting(self, key: str, value: Any):
        """Update system setting in MongoDB."""
        try:
            mongo_db = self._get_mongo_connection()
            settings_collection = mongo_db['system_settings']
            settings_collection.update_one(
                {'key': key},
                {'$set': {'value': value, 'updated_at': datetime.now().isoformat()}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error updating system setting {key}: {e}")
            raise

    def _delete_system_setting(self, key: str):
        """Delete a system setting from MongoDB."""
        try:
            mongo_db = self._get_mongo_connection()
            settings_collection = mongo_db['system_settings']
            settings_collection.delete_one({'key': key})
            logger.info(f"Deleted system setting: {key}")
        except Exception as e:
            logger.error(f"Error deleting system setting {key}: {e}")
            raise

    def _reset_executed_links(self, link_ids: list) -> dict:
        """Reset executed status on links in PostgreSQL and MongoDB (undo a download)."""
        try:
            from src.core.database.postgres.connection import get_postgres_connection
            from psycopg2.extras import RealDictCursor

            mongo_db = self._get_mongo_connection()

            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        UPDATE links
                        SET
                            executed = FALSE,
                            processed_by_workflow = FALSE,
                            workflow_status = NULL,
                            workflow_processed_time = NULL,
                            success = FALSE
                        WHERE links_id = ANY(%s)
                        RETURNING links_id
                    """, (link_ids,))
                    updated = cursor.fetchall()
                    conn.commit()

            mongo_db.workflow_metadata.update_many(
                {'postgres_content_id': {'$in': link_ids}},
                {'$set': {
                    'executed': False,
                    'success': False,
                    'status': 'ready_to_execute',
                    'updated_at': datetime.now().isoformat()
                }}
            )

            return {'success': True, 'reset_count': len(updated)}

        except Exception as e:
            logger.error(f"Error resetting executed links: {e}")
            return {'success': False, 'error': str(e)}

    # ============================================================================
    # WORKFLOW HELPERS
    # ============================================================================

    def _get_filtered_workflows_with_links(self, category=None, workflow_type=None, collection_name=None, date_filter=None):
        """Get workflows that match filtering logic."""
        try:
            from src.core.database.postgres.connection import get_postgres_connection
            from psycopg2.extras import RealDictCursor

            mongo_db = self._get_mongo_connection()

            if date_filter is None:
                date_filter = datetime.now().date()

            logger.info("Step 1: Fetching eligible links from PostgreSQL...")

            eligible_links_query = """
                SELECT
                    l.links_id,
                    l.link,
                    l.tweet_id,
                    l.tweeted_date,
                    l.tweeted_time,
                    l.workflow_type,
                    l.within_limit,
                    l.account_id,
                    l.used,
                    l.processed_by_workflow,
                    l.executed,
                    l.workflow_status
                FROM links l
                WHERE l.within_limit = TRUE
                    AND l.filtered = TRUE
                    AND l.used = TRUE
                    AND COALESCE(l.executed, FALSE) = FALSE
                ORDER BY l.tweeted_date DESC, l.links_id ASC
            """

            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(eligible_links_query)
                    eligible_links = cursor.fetchall()

            if not eligible_links:
                logger.info("No eligible links found in PostgreSQL")
                return []

            logger.info(f"✓ Found {len(eligible_links)} eligible links in PostgreSQL")

            mongo_filter = {
                "has_link": True,
                "has_content": True,
                "status": "ready_to_execute",
                "executed": False
            }

            if category:
                mongo_filter["category"] = category.lower()
            if workflow_type:
                mongo_filter["workflow_type"] = workflow_type
            if collection_name:
                mongo_filter["collection_name"] = collection_name

            link_ids = [link['links_id'] for link in eligible_links]
            mongo_filter["postgres_content_id"] = {"$in": link_ids}

            workflow_assignments = list(
                mongo_db.workflow_metadata.find(mongo_filter).sort("link_assigned_at", -1)
            )

            if not workflow_assignments:
                logger.info("No workflow assignments found matching filters")
                return []

            logger.info(f"✓ Found {len(workflow_assignments)} matching workflow assignments")

            enriched_workflows = []

            for link in eligible_links:
                assignments = [
                    a for a in workflow_assignments
                    if a.get('postgres_content_id') == link['links_id']
                ]

                for assignment in assignments:
                    try:
                        database_name = assignment.get('database_name', 'execution_workflows')
                        actual_collection = assignment.get('collection_name')
                        automa_wf_id = assignment.get('automa_workflow_id')

                        if not actual_collection or not automa_wf_id:
                            continue

                        workflow_db = mongo_db.client[database_name]
                        workflow_collection = workflow_db[actual_collection]
                        workflow_doc = workflow_collection.find_one({'_id': automa_wf_id})

                        if workflow_doc:
                            enriched_workflows.append({
                                'metadata': assignment,
                                'workflow': workflow_doc,
                                'workflow_id': str(automa_wf_id),
                                'workflow_name': workflow_doc.get('name', assignment.get('workflow_name', 'Unknown')),
                                'link_url': link['link'],
                                'link_id': link['links_id'],
                                'assigned_at': assignment.get('link_assigned_at'),
                                'category': assignment.get('category', ''),
                                'workflow_type': assignment.get('workflow_type', ''),
                                'collection_name': actual_collection,
                                'database_name': database_name,
                                'account_id': assignment.get('account_id'),
                                'tweet_id': link['tweet_id'],
                                'tweeted_date': link['tweeted_date']
                            })

                    except Exception as e:
                        logger.error(f"Error enriching workflow: {e}")
                        continue

            logger.info(f"✓ Created {len(enriched_workflows)} link-workflow combinations")
            return enriched_workflows

        except Exception as e:
            logger.error(f"Error fetching filtered workflows: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    def _create_master_execution_workflow_with_delays(self, filtered_workflows, template_data, gap_config):
        """Create a master workflow that executes all filtered workflows sequentially with delays."""
        try:
            master_workflow = {
                "extVersion": template_data.get("extVersion", "1.30.00"),
                "name": "master_execution",
                "icon": "riGlobalLine",
                "table": [],
                "version": template_data.get("version", "1.30.00"),
                "settings": template_data.get("settings", {}),
                "globalData": template_data.get("globalData", '{\n\t"key": "value"\n}'),
                "description": f"Master workflow to execute {len(filtered_workflows)} filtered workflows with delays",
                "includedWorkflows": {}
            }

            nodes = []
            edges = []

            trigger_node = {
                "id": "master_trigger",
                "type": "BlockBasic",
                "initialized": False,
                "position": {"x": 50, "y": 200},
                "data": {
                    "disableBlock": False,
                    "description": "",
                    "type": "manual",
                    "interval": 60,
                    "delay": 5,
                    "date": "",
                    "time": "00:00",
                    "url": "",
                    "shortcut": "",
                    "activeInInput": False,
                    "isUrlRegex": False,
                    "days": [],
                    "contextMenuName": "",
                    "contextTypes": [],
                    "parameters": [],
                    "preferParamsInTab": False,
                    "observeElement": {
                        "selector": "",
                        "baseSelector": "",
                        "matchPattern": "",
                        "targetOptions": {
                            "subtree": False,
                            "childList": True,
                            "attributes": False,
                            "attributeFilter": [],
                            "characterData": False
                        },
                        "baseElOptions": {
                            "subtree": False,
                            "childList": True,
                            "attributes": False,
                            "attributeFilter": [],
                            "characterData": False
                        }
                    }
                },
                "label": "trigger"
            }
            nodes.append(trigger_node)

            x_position = 350
            y_position = 200
            previous_node_id = "master_trigger"

            min_gap = gap_config.get('min_milliseconds', 500)
            max_gap = gap_config.get('max_milliseconds', 5000)

            for idx, wf in enumerate(filtered_workflows):
                exec_node_id = f"exec_{idx}"
                workflow_id = wf['workflow_id']
                workflow_name = wf['workflow_name']

                exec_node = {
                    "id": exec_node_id,
                    "type": "BlockBasic",
                    "initialized": False,
                    "position": {"x": x_position, "y": y_position},
                    "data": {
                        "disableBlock": False,
                        "executeId": "",
                        "workflowId": workflow_id,
                        "globalData": "",
                        "description": f"Execute {workflow_name}",
                        "insertAllVars": False,
                        "insertAllGlobalData": False
                    },
                    "label": "execute-workflow"
                }
                nodes.append(exec_node)

                edge = {
                    "id": f"edge_{previous_node_id}_to_{exec_node_id}",
                    "type": "custom",
                    "source": previous_node_id,
                    "target": exec_node_id,
                    "sourceHandle": f"{previous_node_id}-output-1",
                    "targetHandle": f"{exec_node_id}-input-1",
                    "updatable": True,
                    "selectable": True,
                    "data": {},
                    "label": "",
                    "markerEnd": "arrowclosed",
                    "class": "connected-edges"
                }
                edges.append(edge)

                master_workflow["includedWorkflows"][workflow_id] = wf['workflow']

                if idx < len(filtered_workflows) - 1:
                    delay_node_id = f"delay_{idx}"
                    delay_time = random.randint(min_gap, max_gap)

                    delay_node = {
                        "id": delay_node_id,
                        "type": "BlockDelay",
                        "initialized": False,
                        "position": {"x": x_position + 300, "y": y_position - 25},
                        "data": {
                            "disableBlock": False,
                            "time": delay_time
                        },
                        "label": "delay"
                    }
                    nodes.append(delay_node)

                    delay_edge = {
                        "id": f"edge_{exec_node_id}_to_{delay_node_id}",
                        "type": "custom",
                        "source": exec_node_id,
                        "target": delay_node_id,
                        "sourceHandle": f"{exec_node_id}-output-1",
                        "targetHandle": f"{delay_node_id}-input-1",
                        "updatable": True,
                        "selectable": True,
                        "data": {},
                        "label": "",
                        "markerEnd": "arrowclosed",
                        "class": f"source-{exec_node_id}-output-1 target-{delay_node_id}-input-1"
                    }
                    edges.append(delay_edge)

                    x_position += 600
                    previous_node_id = delay_node_id
                else:
                    previous_node_id = exec_node_id

                y_position += 100

            master_workflow["drawflow"] = {
                "nodes": nodes,
                "edges": edges,
                "position": [0, 0],
                "zoom": 1.0,
                "viewport": {"x": 0, "y": 0, "zoom": 1.0}
            }

            logger.info(f"✅ Created master workflow with {len(filtered_workflows)} workflows and {len(filtered_workflows)-1} delays")
            return master_workflow

        except Exception as e:
            logger.error(f"Error creating master workflow with delays: {e}")
            import traceback
            logger.error(traceback.format_exc())
            st.error(f"❌ Error creating master workflow: {e}")
            return None

    def _create_zip_package(self, filtered_workflows, master_workflow):
        """Create a ZIP package with all workflows at root level."""
        try:
            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                master_json = json.dumps(master_workflow, indent=2, default=str)
                zip_file.writestr('master_execution.json', master_json)

                for idx, wf in enumerate(filtered_workflows):
                    workflow_name = wf['workflow_name'].replace(' ', '_').replace('/', '_')
                    workflow_json = json.dumps(wf['workflow'], indent=2, default=str)
                    zip_file.writestr(f'{workflow_name}.json', workflow_json)

            zip_buffer.seek(0)
            return zip_buffer

        except Exception as e:
            st.error(f"Error creating ZIP package: {e}")
            import traceback
            st.code(traceback.format_exc())
            return None

    def _update_links_success_status(self, filtered_workflows):
        """Update success and failure status in PostgreSQL links table."""
        try:
            from src.core.database.postgres.connection import get_postgres_connection
            from psycopg2.extras import RealDictCursor

            link_ids = []
            for wf in filtered_workflows:
                postgres_content_id = wf['metadata'].get('postgres_content_id')
                if postgres_content_id:
                    link_ids.append(postgres_content_id)

            if not link_ids:
                return {
                    'success': False,
                    'updated_count': 0,
                    'error': 'No link IDs found'
                }

            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'links'
                        AND column_name IN ('success', 'failure')
                    """)
                    existing_columns = [row['column_name'] for row in cursor.fetchall()]

                    if 'success' not in existing_columns:
                        cursor.execute("ALTER TABLE links ADD COLUMN success BOOLEAN DEFAULT FALSE")
                        st.info("✅ Added 'success' column to links table")

                    if 'failure' not in existing_columns:
                        cursor.execute("ALTER TABLE links ADD COLUMN failure BOOLEAN DEFAULT FALSE")
                        st.info("✅ Added 'failure' column to links table")

                    update_query = """
                        UPDATE links
                        SET
                            success = TRUE,
                            failure = FALSE
                        WHERE links_id = ANY(%s)
                        RETURNING links_id, success, failure
                    """

                    cursor.execute(update_query, (link_ids,))
                    updated_rows = cursor.fetchall()
                    updated_count = len(updated_rows)

                    conn.commit()

                    return {
                        'success': True,
                        'updated_count': updated_count,
                        'link_ids': [row['links_id'] for row in updated_rows],
                        'timestamp': datetime.now().isoformat()
                    }

        except Exception as e:
            logger.error(f"❌ Error updating links success status: {e}")
            return {
                'success': False,
                'updated_count': 0,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    def _mark_links_as_executed(self, filtered_workflows):
        """Mark links as executed in BOTH PostgreSQL AND MongoDB with success tracking."""
        try:
            from src.core.database.postgres.connection import get_postgres_connection
            from psycopg2.extras import RealDictCursor

            mongo_db = self._get_mongo_connection()

            link_ids = []
            metadata_ids = []

            for wf in filtered_workflows:
                postgres_content_id = wf['metadata'].get('postgres_content_id')
                if postgres_content_id:
                    link_ids.append(postgres_content_id)

                metadata_id = wf['metadata'].get('_id')
                if metadata_id:
                    metadata_ids.append(ObjectId(metadata_id))

            if not link_ids:
                return {
                    'success': False,
                    'postgres_updated': 0,
                    'mongo_updated': 0,
                    'error': 'No link IDs found'
                }

            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    update_query = """
                        UPDATE links
                        SET
                            executed = TRUE,
                            processed_by_workflow = TRUE,
                            workflow_status = 'completed',
                            workflow_processed_time = CURRENT_TIMESTAMP,
                            success = TRUE,
                            failure = FALSE
                        WHERE links_id = ANY(%s)
                        RETURNING links_id, workflow_status, executed, success, failure
                    """

                    cursor.execute(update_query, (link_ids,))
                    updated_rows = cursor.fetchall()
                    postgres_updated = len(updated_rows)

                    conn.commit()

            if metadata_ids:
                mongo_update = {
                    '$set': {
                        'executed': True,
                        'success': True,
                        'executed_at': datetime.now().isoformat(),
                        'status': 'completed',
                        'updated_at': datetime.now().isoformat(),
                        'execution_mode': 'manual_download',
                        'execution_source': 'streamlit_ui'
                    }
                }

                result = mongo_db.workflow_metadata.update_many(
                    {'_id': {'$in': metadata_ids}},
                    mongo_update
                )
                mongo_updated = result.modified_count
            else:
                mongo_updated = 0

            return {
                'success': True,
                'postgres_updated': postgres_updated,
                'mongo_updated': mongo_updated,
                'link_ids': [row['links_id'] for row in updated_rows],
                'timestamp': datetime.now().isoformat(),
                'success_status_updated': True
            }

        except Exception as e:
            logger.error(f"❌ Error marking links as executed: {e}")
            return {
                'success': False,
                'postgres_updated': 0,
                'mongo_updated': 0,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
