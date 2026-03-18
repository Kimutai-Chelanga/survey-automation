"""
Manual Executor Page
Handles manual template upload and workflow package download with single-button execution.
Now integrated with survey sites - each survey site has its own workflows.
"""
import os
import streamlit as st
from datetime import datetime, date
import json
import zipfile
import io
import random
from typing import Dict, Any, List, Optional
from bson import ObjectId
import logging

logger = logging.getLogger(__name__)


class ManualExecutor:
    """Manual workflow execution and package management page - Integrated with survey sites."""

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

        # Load survey sites for dropdowns
        survey_sites = self._load_survey_sites()

        tab1, tab2, tab3, tab4 = st.tabs([
            "📤 Upload Template",
            "📥 Download Workflows",
            "⚙️ Manage Template",
            "🌐 Survey Site Workflows"
        ])

        with tab1:
            self._render_template_upload()

        with tab2:
            self._render_download_workflows(survey_sites)

        with tab3:
            self._render_manage_template()

        with tab4:
            self._render_survey_site_workflows(survey_sites)

    # ============================================================================
    # SURVEY SITE HELPERS
    # ============================================================================

    def _load_survey_sites(self) -> List[Dict[str, Any]]:
        """Load all survey sites from database."""
        try:
            from src.core.database.postgres.connection import get_postgres_connection
            from psycopg2.extras import RealDictCursor

            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT site_id, country, url, description, created_at
                        FROM survey_sites
                        WHERE is_active = TRUE OR is_active IS NULL
                        ORDER BY country
                    """)
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error loading survey sites: {e}")
            return []

    def _get_site_workflows(self, site_id: int) -> List[Dict[str, Any]]:
        """Get workflows for a specific survey site."""
        try:
            from src.core.database.postgres.connection import get_postgres_connection
            from psycopg2.extras import RealDictCursor

            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT 
                            w.workflow_id,
                            w.workflow_name,
                            w.workflow_type,
                            w.created_time,
                            w.updated_time,
                            w.is_active,
                            COUNT(DISTINCT q.question_id) as question_count,
                            COUNT(DISTINCT a.answer_id) as answer_count
                        FROM workflows w
                        LEFT JOIN questions q ON w.workflow_id = q.workflow_id
                        LEFT JOIN answers a ON w.workflow_id = a.workflow_id
                        WHERE w.site_id = %s
                        GROUP BY w.workflow_id
                        ORDER BY w.created_time DESC
                    """, (site_id,))
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error loading site workflows: {e}")
            return []

    def _save_workflow_for_site(self, site_id: int, workflow_name: str, workflow_data: Dict, workflow_type: str = "extraction") -> Optional[int]:
        """Save a workflow for a specific survey site."""
        try:
            from src.core.database.postgres.connection import get_postgres_connection
            from psycopg2.extras import RealDictCursor

            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        INSERT INTO workflows (
                            site_id, workflow_name, workflow_type, workflow_data,
                            created_time, updated_time, is_active
                        ) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE)
                        RETURNING workflow_id
                    """, (site_id, workflow_name, workflow_type, json.dumps(workflow_data)))
                    
                    result = cursor.fetchone()
                    conn.commit()
                    
                    return result['workflow_id'] if result else None
        except Exception as e:
            logger.error(f"Error saving workflow for site: {e}")
            return None

    def _get_workflows_for_download(self, site_id: Optional[int] = None, workflow_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get workflows ready for download, optionally filtered by site."""
        try:
            from src.core.database.postgres.connection import get_postgres_connection
            from psycopg2.extras import RealDictCursor

            mongo_db = self._get_mongo_connection()

            query = """
                SELECT 
                    w.workflow_id,
                    w.site_id,
                    w.workflow_name,
                    w.workflow_type,
                    w.workflow_data,
                    w.created_time,
                    w.updated_time,
                    s.country as site_country,
                    s.url as site_url,
                    COUNT(DISTINCT q.question_id) as question_count
                FROM workflows w
                LEFT JOIN survey_sites s ON w.site_id = s.site_id
                LEFT JOIN questions q ON w.workflow_id = q.workflow_id
                WHERE w.is_active = TRUE
            """
            params = []

            if site_id:
                query += " AND w.site_id = %s"
                params.append(site_id)

            if workflow_type:
                query += " AND w.workflow_type = %s"
                params.append(workflow_type)

            query += " GROUP BY w.workflow_id, s.country, s.url ORDER BY w.created_time DESC"

            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query, params)
                    workflows = cursor.fetchall()

            # Enrich with MongoDB workflow data if available
            enriched = []
            for wf in workflows:
                workflow_data = wf.get('workflow_data')
                if workflow_data and isinstance(workflow_data, str):
                    try:
                        workflow_data = json.loads(workflow_data)
                    except:
                        workflow_data = {}

                enriched.append({
                    'workflow_id': wf['workflow_id'],
                    'site_id': wf['site_id'],
                    'site_country': wf.get('site_country', 'Unknown'),
                    'site_url': wf.get('site_url', ''),
                    'workflow_name': wf['workflow_name'],
                    'workflow_type': wf['workflow_type'],
                    'workflow_data': workflow_data,
                    'question_count': wf.get('question_count', 0),
                    'created_time': wf['created_time']
                })

            return enriched

        except Exception as e:
            logger.error(f"Error getting workflows for download: {e}")
            return []

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

    def _render_download_workflows(self, survey_sites: List[Dict[str, Any]]):
        """Render download filtered workflows section - NOW WITH SURVEY SITE SELECTION."""
        st.subheader("📥 Download Workflows by Survey Site")
        st.caption("Download workflows for specific survey sites")

        manual_template = self._get_or_auto_save_manual_template()
        if not manual_template:
            st.warning(
                "⚠️ No manual execution template found. "
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

        st.markdown("---")

        # Survey site selection
        col1, col2 = st.columns(2)

        with col1:
            site_options = {f"{s['country']}": s['site_id'] for s in survey_sites}
            selected_site = st.selectbox(
                "Select Survey Site:",
                options=["All Sites"] + list(site_options.keys()),
                key="download_site_select"
            )

        with col2:
            workflow_type = st.selectbox(
                "Workflow Type:",
                options=["All Types", "extraction", "submission", "validation"],
                key="download_type_select"
            )

        filter_date = st.date_input(
            "Filter by date:",
            value=datetime.now().date(),
            key="download_filter_date"
        )

        # Get workflows based on selection
        site_id = None if selected_site == "All Sites" else site_options[selected_site]
        wf_type = None if workflow_type == "All Types" else workflow_type

        workflows = self._get_workflows_for_download(
            site_id=site_id,
            workflow_type=wf_type
        )

        if not workflows:
            st.warning("⚠️ No workflows found for the selected criteria")
            return

        st.success(f"✅ Found **{len(workflows)}** workflows")

        # Show workflow details
        with st.expander("📋 Workflow Details", expanded=False):
            for wf in workflows:
                st.markdown(f"""
                - **{wf['workflow_name']}** ({wf['workflow_type']})
                  - Site: {wf['site_country']}
                  - Questions: {wf['question_count']}
                  - Created: {wf['created_time'].strftime('%Y-%m-%d %H:%M') if wf.get('created_time') else 'Unknown'}
                """)

        delay_count = len(workflows) - 1
        if delay_count > 0:
            st.info(f"🔄 Will add **{delay_count}** delay blocks between workflows")

        st.markdown("---")

        if st.button("🚀 Generate & Download ZIP Package", type="primary", use_container_width=True, key="generate_download_site_btn"):
            with st.spinner("Generating workflows with delays..."):
                try:
                    template_data = manual_template.get('template_data', {})

                    master_workflow = self._create_master_execution_workflow_with_delays(
                        workflows,
                        template_data,
                        gap_config
                    )

                    if not master_workflow:
                        st.error("❌ Failed to create master workflow")
                        return

                    zip_buffer = self._create_zip_package(workflows, master_workflow)

                    if not zip_buffer:
                        st.error("❌ Failed to create ZIP")
                        return

                    now = datetime.now()
                    date_str = now.strftime("%Y-%m-%d")
                    time_str = now.strftime("%H-%M-%S")
                    site_str = selected_site.replace(" ", "_") if selected_site != "All Sites" else "all_sites"
                    filename = f"workflows_{site_str}_{date_str}_{time_str}.zip"

                    st.info(f"📦 Package name: `{filename}`")

                    st.download_button(
                        label="📥 Download ZIP Package",
                        data=zip_buffer,
                        file_name=filename,
                        mime="application/zip",
                        use_container_width=True,
                        key="download_zip_btn_site"
                    )

                    st.success("✅ Package generated successfully and ready to download!")
                    st.balloons()

                except Exception as e:
                    st.error(f"❌ Error: {e}")
                    import traceback
                    st.code(traceback.format_exc())

    def _render_survey_site_workflows(self, survey_sites: List[Dict[str, Any]]):
        """Render survey site workflows management tab."""
        st.subheader("🌐 Survey Site Workflows")
        st.caption("View and manage workflows for each survey site")

        if not survey_sites:
            st.warning("No survey sites found. Add them in the Accounts page first.")
            return

        # Site selector
        site_options = {f"{s['country']} - {s['url']}": s for s in survey_sites}
        selected_site_name = st.selectbox(
            "Select Survey Site:",
            options=list(site_options.keys()),
            key="site_workflow_select"
        )
        selected_site = site_options[selected_site_name]

        st.markdown("---")

        # Show site details
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"**Country:** {selected_site['country']}")
        with col2:
            st.info(f"**URL:** {selected_site['url']}")

        # Get workflows for this site
        site_workflows = self._get_site_workflows(selected_site['site_id'])

        if site_workflows:
            st.success(f"✅ Found {len(site_workflows)} workflows for this site")

            for wf in site_workflows:
                with st.expander(f"📋 {wf['workflow_name']} ({wf['workflow_type']})", expanded=False):
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.metric("Questions", wf.get('question_count', 0))
                    with col2:
                        st.metric("Answers", wf.get('answer_count', 0))
                    with col3:
                        status = "✅ Active" if wf.get('is_active') else "❌ Inactive"
                        st.metric("Status", status)

                    st.caption(f"Created: {wf['created_time'].strftime('%Y-%m-%d %H:%M') if wf.get('created_time') else 'Unknown'}")
        else:
            st.info("No workflows found for this site yet.")

            # Form to add new workflow
            with st.form("add_site_workflow_form"):
                st.subheader("➕ Add New Workflow")

                workflow_name = st.text_input("Workflow Name *", placeholder="e.g., survey_extraction_v1")
                workflow_type = st.selectbox(
                    "Workflow Type *",
                    options=["extraction", "submission", "validation"]
                )

                workflow_json = st.text_area(
                    "Workflow JSON *",
                    height=300,
                    placeholder='{\n  "extVersion": "1.30.00",\n  "name": "workflow_name",\n  ...\n}'
                )

                if st.form_submit_button("✅ Save Workflow", type="primary"):
                    if not workflow_name.strip() or not workflow_json.strip():
                        st.error("Workflow name and JSON are required!")
                    else:
                        try:
                            workflow_data = json.loads(workflow_json)
                            wf_id = self._save_workflow_for_site(
                                site_id=selected_site['site_id'],
                                workflow_name=workflow_name.strip(),
                                workflow_data=workflow_data,
                                workflow_type=workflow_type
                            )
                            if wf_id:
                                st.success(f"✅ Workflow saved with ID: {wf_id}")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("❌ Failed to save workflow")
                        except json.JSONDecodeError as e:
                            st.error(f"❌ Invalid JSON: {e}")

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

    # ============================================================================
    # WORKFLOW HELPERS
    # ============================================================================

    def _create_master_execution_workflow_with_delays(self, workflows, template_data, gap_config):
        """Create a master workflow that executes all workflows sequentially with delays."""
        try:
            master_workflow = {
                "extVersion": template_data.get("extVersion", "1.30.00"),
                "name": "master_execution",
                "icon": "riGlobalLine",
                "table": [],
                "version": template_data.get("version", "1.30.00"),
                "settings": template_data.get("settings", {}),
                "globalData": template_data.get("globalData", '{\n\t"key": "value"\n}'),
                "description": f"Master workflow to execute {len(workflows)} workflows with delays",
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

            for idx, wf in enumerate(workflows):
                exec_node_id = f"exec_{idx}"
                workflow_id = str(wf.get('workflow_id', idx))
                workflow_name = wf.get('workflow_name', f'workflow_{idx}')

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
                        "description": f"Execute {workflow_name} for {wf.get('site_country', 'Unknown')}",
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

                # Add workflow data to includedWorkflows
                if wf.get('workflow_data'):
                    master_workflow["includedWorkflows"][workflow_id] = wf['workflow_data']

                if idx < len(workflows) - 1:
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

            logger.info(f"✅ Created master workflow with {len(workflows)} workflows and {len(workflows)-1} delays")
            return master_workflow

        except Exception as e:
            logger.error(f"Error creating master workflow with delays: {e}")
            import traceback
            logger.error(traceback.format_exc())
            st.error(f"❌ Error creating master workflow: {e}")
            return None

    def _create_zip_package(self, workflows, master_workflow):
        """Create a ZIP package with all workflows at root level."""
        try:
            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                master_json = json.dumps(master_workflow, indent=2, default=str)
                zip_file.writestr('master_execution.json', master_json)

                for idx, wf in enumerate(workflows):
                    workflow_name = wf.get('workflow_name', f'workflow_{idx}').replace(' ', '_').replace('/', '_')
                    site_country = wf.get('site_country', 'unknown').replace(' ', '_')
                    
                    if wf.get('workflow_data'):
                        workflow_json = json.dumps(wf['workflow_data'], indent=2, default=str)
                        zip_file.writestr(f'{site_country}_{workflow_name}.json', workflow_json)

            zip_buffer.seek(0)
            return zip_buffer

        except Exception as e:
            st.error(f"Error creating ZIP package: {e}")
            import traceback
            st.code(traceback.format_exc())
            return None