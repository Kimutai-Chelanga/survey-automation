"""
Manual Executor Page
Handles workflow upload per survey site and workflow package download.
"""
import os
import streamlit as st
from datetime import datetime
import json
import zipfile
import io
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class ManualExecutor:
    """Manual workflow execution and package management page."""

    def __init__(self, db_manager):
        """Initialize the ManualExecutor page."""
        self.db_manager = db_manager

    def render(self):
        """Render the Manual Executor page."""
        st.title("🚀 Manual Executor")
        st.markdown("*Upload and download workflows for survey sites*")
        st.markdown("---")

        # Load survey sites for dropdowns
        survey_sites = self._load_survey_sites()

        if not survey_sites:
            st.warning("No survey sites found. Please add them in the Accounts page first.")
            return

        tab1, tab2 = st.tabs([
            "🌐 Survey Site Workflows",
            "📥 Download Workflows"
        ])

        with tab1:
            self._render_survey_site_workflows(survey_sites)

        with tab2:
            self._render_download_workflows(survey_sites)

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
                            w.workflow_data,
                            w.created_time,
                            w.updated_time,
                            w.is_active,
                            COUNT(DISTINCT q.question_id) as question_count,
                            COUNT(DISTINCT a.answer_id) as answer_count
                        FROM workflows w
                        LEFT JOIN questions q ON w.workflow_id = q.workflow_id
                        LEFT JOIN answers a ON w.workflow_id = a.workflow_id
                        WHERE w.site_id = %s
                        GROUP BY w.workflow_id, w.workflow_name, w.workflow_data, 
                                 w.created_time, w.updated_time, w.is_active
                        ORDER BY w.created_time DESC
                    """, (site_id,))
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error loading site workflows: {e}")
            return []

    def _save_workflow_for_site(self, site_id: int, workflow_data: Dict) -> Optional[int]:
        """Save a workflow for a specific survey site."""
        try:
            from src.core.database.postgres.connection import get_postgres_connection
            from psycopg2.extras import RealDictCursor

            # Extract workflow name from the JSON data
            workflow_name = workflow_data.get('name', f'workflow_{datetime.now().strftime("%Y%m%d_%H%M%S")}')

            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        INSERT INTO workflows (
                            site_id, workflow_name, workflow_data,
                            created_time, updated_time, is_active
                        ) VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE)
                        RETURNING workflow_id
                    """, (site_id, workflow_name, json.dumps(workflow_data)))
                    
                    result = cursor.fetchone()
                    conn.commit()
                    
                    return result['workflow_id'] if result else None
        except Exception as e:
            logger.error(f"Error saving workflow for site: {e}")
            return None

    def _delete_workflow(self, workflow_id: int) -> bool:
        """Delete a workflow."""
        try:
            from src.core.database.postgres.connection import get_postgres_connection

            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM workflows WHERE workflow_id = %s", (workflow_id,))
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error deleting workflow: {e}")
            return False

    def _toggle_workflow_status(self, workflow_id: int, is_active: bool) -> bool:
        """Toggle workflow active status."""
        try:
            from src.core.database.postgres.connection import get_postgres_connection

            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE workflows 
                        SET is_active = %s, updated_time = CURRENT_TIMESTAMP 
                        WHERE workflow_id = %s
                    """, (is_active, workflow_id))
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error toggling workflow status: {e}")
            return False

    def _get_workflows_for_download(self, site_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get workflows ready for download, optionally filtered by site."""
        try:
            from src.core.database.postgres.connection import get_postgres_connection
            from psycopg2.extras import RealDictCursor

            query = """
                SELECT 
                    w.workflow_id,
                    w.site_id,
                    w.workflow_name,
                    w.workflow_data,
                    w.created_time,
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

            query += " GROUP BY w.workflow_id, s.country, s.url ORDER BY w.created_time DESC"

            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query, params)
                    workflows = cursor.fetchall()

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
                    'workflow_data': workflow_data,
                    'question_count': wf.get('question_count', 0),
                    'created_time': wf['created_time']
                })

            return enriched

        except Exception as e:
            logger.error(f"Error getting workflows for download: {e}")
            return []

    # ============================================================================
    # TAB 1: SURVEY SITE WORKFLOWS (Upload & Manage)
    # ============================================================================

    def _render_survey_site_workflows(self, survey_sites: List[Dict[str, Any]]):
        """Render survey site workflows management tab - Upload and manage workflows per site."""
        st.subheader("🌐 Upload & Manage Workflows")
        st.caption("Upload workflows for each survey site")

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

        # Upload new workflow section
        with st.expander("📤 Upload New Workflow", expanded=True):
            st.markdown("Upload a workflow JSON file for this survey site")

            uploaded_file = st.file_uploader(
                "Choose workflow JSON file",
                type=['json'],
                key="workflow_file_upload"
            )

            if uploaded_file is not None:
                try:
                    workflow_data = json.load(uploaded_file)
                    
                    # Preview
                    st.success(f"✅ File '{uploaded_file.name}' loaded successfully!")
                    
                    with st.expander("🔍 Preview Workflow"):
                        st.json(workflow_data)
                    
                    if st.button("💾 Save Workflow", type="primary", use_container_width=True):
                        wf_id = self._save_workflow_for_site(
                            site_id=selected_site['site_id'],
                            workflow_data=workflow_data
                        )
                        if wf_id:
                            st.success(f"✅ Workflow saved successfully with ID: {wf_id}")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("❌ Failed to save workflow")
                            
                except Exception as e:
                    st.error(f"❌ Error loading file: {e}")

        # Show existing workflows
        st.markdown("---")
        st.subheader("📋 Existing Workflows")

        site_workflows = self._get_site_workflows(selected_site['site_id'])

        if site_workflows:
            st.success(f"✅ Found {len(site_workflows)} workflows for this site")

            for wf in site_workflows:
                with st.expander(f"📋 {wf['workflow_name']}", expanded=False):
                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        st.metric("Questions", wf.get('question_count', 0))
                    with col2:
                        st.metric("Answers", wf.get('answer_count', 0))
                    with col3:
                        status = "✅ Active" if wf.get('is_active') else "❌ Inactive"
                        st.metric("Status", status)
                    with col4:
                        st.metric("Workflow ID", wf['workflow_id'])

                    st.caption(f"Created: {wf['created_time'].strftime('%Y-%m-%d %H:%M') if wf.get('created_time') else 'Unknown'}")
                    
                    # Preview workflow data
                    if wf.get('workflow_data'):
                        with st.expander("🔍 View Workflow JSON"):
                            st.json(wf['workflow_data'])
                    
                    # Actions
                    col_a, col_b, col_c = st.columns(3)
                    
                    with col_a:
                        if wf.get('is_active'):
                            if st.button("⭕ Deactivate", key=f"deact_{wf['workflow_id']}", use_container_width=True):
                                if self._toggle_workflow_status(wf['workflow_id'], False):
                                    st.success("Workflow deactivated")
                                    st.rerun()
                        else:
                            if st.button("✅ Activate", key=f"act_{wf['workflow_id']}", use_container_width=True):
                                if self._toggle_workflow_status(wf['workflow_id'], True):
                                    st.success("Workflow activated")
                                    st.rerun()
                    
                    with col_b:
                        if st.button("📥 Download JSON", key=f"dl_{wf['workflow_id']}", use_container_width=True):
                            wf_json = json.dumps(wf['workflow_data'], indent=2)
                            st.download_button(
                                label="📥 Click to Download",
                                data=wf_json,
                                file_name=f"{wf['workflow_name']}.json",
                                mime="application/json",
                                key=f"download_{wf['workflow_id']}"
                            )
                    
                    with col_c:
                        if st.button("🗑️ Delete", key=f"del_{wf['workflow_id']}", use_container_width=True):
                            if self._delete_workflow(wf['workflow_id']):
                                st.success("Workflow deleted")
                                st.rerun()
                            else:
                                st.error("Failed to delete workflow")
        else:
            st.info("No workflows found for this site yet. Upload one above.")

    # ============================================================================
    # TAB 2: DOWNLOAD WORKFLOWS
    # ============================================================================

    def _render_download_workflows(self, survey_sites: List[Dict[str, Any]]):
        """Render download workflows section."""
        st.subheader("📥 Download Workflows")
        st.caption("Select workflows to download as a ZIP package")

        # Survey site selection
        site_options = {f"{s['country']}": s['site_id'] for s in survey_sites}
        selected_site = st.selectbox(
            "Select Survey Site:",
            options=["All Sites"] + list(site_options.keys()),
            key="download_site_select"
        )

        # Get workflows based on selection
        site_id = None if selected_site == "All Sites" else site_options[selected_site]

        workflows = self._get_workflows_for_download(site_id=site_id)

        if not workflows:
            st.warning("⚠️ No workflows found for the selected criteria")
            return

        st.success(f"✅ Found **{len(workflows)}** workflows ready for download")

        # Show workflow details with checkboxes for selection
        st.markdown("### Select Workflows to Download")
        
        selected_workflows = []
        for wf in workflows:
            col1, col2, col3 = st.columns([1, 3, 2])
            with col1:
                select = st.checkbox("", key=f"select_{wf['workflow_id']}")
                if select:
                    selected_workflows.append(wf)
            with col2:
                st.write(f"**{wf['workflow_name']}**")
            with col3:
                st.write(f"{wf['site_country']} - {wf.get('question_count', 0)} questions")

        st.markdown("---")

        if selected_workflows:
            st.info(f"✅ Selected {len(selected_workflows)} workflows for download")

            if st.button("📦 Generate & Download ZIP Package", type="primary", use_container_width=True):
                with st.spinner("Creating ZIP package..."):
                    try:
                        # Create a simple manifest
                        manifest = {
                            "name": "workflow_package",
                            "created": datetime.now().isoformat(),
                            "workflow_count": len(selected_workflows),
                            "workflows": [{
                                "id": wf['workflow_id'],
                                "name": wf['workflow_name'],
                                "site": wf['site_country']
                            } for wf in selected_workflows]
                        }

                        zip_buffer = self._create_download_package(selected_workflows, manifest)

                        if zip_buffer:
                            now = datetime.now()
                            date_str = now.strftime("%Y-%m-%d")
                            site_str = selected_site.replace(" ", "_") if selected_site != "All Sites" else "all_sites"
                            filename = f"workflows_{site_str}_{date_str}.zip"

                            st.download_button(
                                label="📥 Download ZIP Package",
                                data=zip_buffer,
                                file_name=filename,
                                mime="application/zip",
                                use_container_width=True
                            )
                            st.success("✅ Package generated successfully!")
                        else:
                            st.error("❌ Failed to create ZIP package")

                    except Exception as e:
                        st.error(f"❌ Error: {e}")
                        import traceback
                        st.code(traceback.format_exc())
        else:
            st.info("👆 Select workflows above to download")

    def _create_download_package(self, workflows, manifest):
        """Create a ZIP package with selected workflows."""
        try:
            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                # Add manifest
                manifest_json = json.dumps(manifest, indent=2)
                zip_file.writestr('manifest.json', manifest_json)

                # Add each workflow
                for wf in workflows:
                    if wf.get('workflow_data'):
                        filename = f"{wf['site_country']}_{wf['workflow_name']}.json".replace(' ', '_')
                        workflow_json = json.dumps(wf['workflow_data'], indent=2)
                        zip_file.writestr(filename, workflow_json)

            zip_buffer.seek(0)
            return zip_buffer

        except Exception as e:
            logger.error(f"Error creating ZIP package: {e}")
            return None