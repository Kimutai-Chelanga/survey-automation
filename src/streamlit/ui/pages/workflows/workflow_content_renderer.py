"""
FILE: ui/components/workflow_content_renderer.py
Updated WorkflowContentRenderer with horizontal tab layout and workflow analyzer integration
"""

import streamlit as st
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional
from .workflow_filters import WorkflowFilters
from .data_formatter import format_workflows_dataframe, format_executions_dataframe
from src.core.database.mongodb.connection import get_mongo_collection
from bson import ObjectId
from pymongo import MongoClient
import os


class WorkflowContentRenderer:
    """Simplified class to render workflow content focusing on actual workflow definitions."""

    def __init__(self, workflow_type: str, db_module):
        self.workflow_type = workflow_type
        self.db_module = db_module
        self.workflow_type_lower = workflow_type.lower()

    @staticmethod
    def _get_mongo_client():
        """Get MongoDB client connection."""
        return MongoClient(
            os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
            serverSelectionTimeoutMS=5000
        )

    @staticmethod
    def _get_metadata_collection(client):
        """Get workflow_metadata collection."""
        messages_db = client['messages_db']
        return messages_db['workflow_metadata']

    def _export_workflow_with_content(self, workflow: Dict[str, Any], include_content: bool = True):
        """Export workflow with optional content inclusion."""
        try:
            # Base export data structure
            export_data = {
                "export_info": {
                    "exported_at": datetime.now().isoformat(),
                    "workflow_type": self.workflow_type,
                    "export_version": "2.0"
                },
                "workflow_identifiers": {
                    "automa_workflow_id": str(workflow.get('automa_workflow_id', workflow.get('_id'))),
                    "workflow_id": workflow.get('workflow_id'),
                    "execution_id": workflow.get('execution_id'),
                    "workflow_name": workflow.get('workflow_name')
                },
                "account_info": {
                    "postgres_account_id": workflow.get('postgres_account_id'),
                    "mongo_account_id": str(workflow.get('mongo_account_id')) if workflow.get('mongo_account_id') else None,
                    "username": workflow.get('username'),
                    "profile_id": workflow.get('profile_id')
                },
                "content_info": {
                    "postgres_content_id": workflow.get('postgres_content_id'),
                    "content_type": workflow.get('workflow_type'),
                    "has_content": workflow.get('has_content'),
                    "content_length": workflow.get('content_length'),
                    "content_hash": workflow.get('content_hash'),
                    "has_link": workflow.get('has_link'),
                    "link_url": workflow.get('link_url')
                },
                "execution_info": {
                    "status": workflow.get('status'),
                    "executed": workflow.get('executed'),
                    "success": workflow.get('success'),
                    "execute": workflow.get('execute'),
                    "execution_attempts": workflow.get('execution_attempts', 0),
                    "retry_count": workflow.get('retry_count', 0),
                    "generated_at": workflow.get('generated_at'),
                    "executed_at": workflow.get('executed_at'),
                    "execution_time_ms": workflow.get('execution_time_ms')
                },
                "generation_metadata": {
                    "blocks_generated": workflow.get('blocks_generated'),
                    "template_used": workflow.get('template_used'),
                    "processing_priority": workflow.get('processing_priority'),
                    "generation_context": workflow.get('generation_context'),
                    "performance_metrics": workflow.get('performance_metrics')
                },
                "error_tracking": {
                    "error_message": workflow.get('error_message'),
                    "last_error_message": workflow.get('last_error_message'),
                    "last_error_timestamp": workflow.get('last_error_timestamp')
                }
            }

            # Include actual content if requested
            if include_content and workflow.get('actual_content'):
                export_data["actual_content"] = {
                    "full_content": workflow.get('actual_content'),
                    "content_preview": workflow.get('content_text_preview'),
                    "content_length": len(workflow.get('actual_content', '')),
                    "word_count": len(workflow.get('actual_content', '').split()),
                    "line_count": len(workflow.get('actual_content', '').split('\n'))
                }
            elif not include_content:
                export_data["actual_content"] = {
                    "included": False,
                    "note": "Content excluded from export by user choice"
                }
            else:
                export_data["actual_content"] = {
                    "included": False,
                    "note": "No content available for this workflow"
                }

            # Include automa workflow data if available
            if workflow.get('drawflow'):
                export_data["automa_workflow_data"] = {
                    "name": workflow.get('name'),
                    "description": workflow.get('description'),
                    "version": workflow.get('version'),
                    "drawflow": workflow.get('drawflow'),
                    "settings": workflow.get('settings'),
                    "globalData": workflow.get('globalData')
                }

            # Convert to JSON
            json_data = json.dumps(export_data, indent=2, default=str)

            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            workflow_name = workflow.get('workflow_name', 'unknown').replace(' ', '_')
            filename = f"workflow_complete_{workflow_name}_{timestamp}.json"

            # Create download button
            st.download_button(
                label="📥 Download Complete Workflow Data (with Content)" if include_content else "📥 Download Workflow Data (without Content)",
                data=json_data,
                file_name=filename,
                mime="application/json",
                key=f"download_complete_{workflow.get('automa_workflow_id', workflow.get('_id'))}_{timestamp}"
            )

        except Exception as e:
            st.error(f"❌ Error exporting workflow: {e}")

    def render_workflow_details(self, workflow: Dict[str, Any]):
        """Render complete workflow details with horizontal tab layout."""

        # Create horizontal tabs for organized view
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📋 Workflow Details",
            "📄 Content",
            "📁 Artifacts",
            "🔗 Links",
            "📊 Analysis"
        ])

        with tab1:
            self._render_workflow_details_tab(workflow)

        with tab2:
            self._render_content_tab(workflow)

        with tab3:
            self._render_artifacts_tab(workflow)

        with tab4:
            self._render_links_tab(workflow)

        with tab5:
            self._render_analysis_tab(workflow)

        # Export options at the bottom
        st.markdown("---")
        st.subheader("📤 Export Options")
        col1, col2 = st.columns(2)

        with col1:
            if st.button(
                "Export with Content",
                key=f"export_with_content_{workflow.get('automa_workflow_id', workflow.get('_id'))}",
                help="Export workflow data including full content text"
            ):
                self._export_workflow_with_content(workflow, include_content=True)

        with col2:
            if st.button(
                "Export without Content",
                key=f"export_without_content_{workflow.get('automa_workflow_id', workflow.get('_id'))}",
                help="Export workflow data excluding content text"
            ):
                self._export_workflow_with_content(workflow, include_content=False)

    def _render_workflow_details_tab(self, workflow: Dict[str, Any]):
        """Render workflow metadata details."""
        st.subheader("Workflow Metadata")

        # Create 4 columns for organized display
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.write("**Content Information:**")
            st.write(f"• **Content ID:** {workflow.get('postgres_content_id', 'N/A')}")
            st.write(f"• **Content Type:** {workflow.get('workflow_type', 'N/A')}")
            st.write(f"• **Has Content:** {'✅ Yes' if workflow.get('has_content') else '❌ No'}")
            st.write(f"• **Has Link:** {'✅ Yes' if workflow.get('has_link') else '❌ No'}")
            st.write(f"• **Content Length:** {workflow.get('content_length', 0)} chars")

        with col2:
            st.write("**Account Information:**")
            st.write(f"• **Account ID:** {workflow.get('postgres_account_id', 'N/A')}")
            st.write(f"• **Username:** {workflow.get('username', 'N/A')}")
            st.write(f"• **Profile ID:** {workflow.get('profile_id', 'N/A')}")
            st.write(f"• **Mongo Account ID:** {workflow.get('mongo_account_id', 'N/A')}")

        with col3:
            st.write("**Execution Status:**")
            st.write(f"• **Status:** {workflow.get('status', 'unknown')}")
            st.write(f"• **Executed:** {'✅ Yes' if workflow.get('executed') else '❌ No'}")
            st.write(f"• **Success:** {'✅ Yes' if workflow.get('success') else '❌ No'}")
            st.write(f"• **Blocks Generated:** {workflow.get('blocks_generated', 0)}")
            st.write(f"• **Priority:** {workflow.get('processing_priority', 'N/A')}")

        with col4:
            st.write("**Workflow Info:**")
            st.write(f"• **Name:** {workflow.get('name', 'N/A')}")
            st.write(f"• **Version:** {workflow.get('version', 'N/A')}")
            st.write(f"• **Template:** {workflow.get('template_used', 'N/A')}")

        # Display timestamps
        st.markdown("---")
        st.subheader("Timestamps")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.write(f"**Generated At:**")
            st.write(workflow.get('generated_at', 'N/A'))

        with col2:
            st.write(f"**Executed At:**")
            st.write(workflow.get('executed_at', 'N/A'))

        with col3:
            st.write(f"**Created At:**")
            st.write(workflow.get('created_at', 'N/A'))

        with col4:
            st.write(f"**Updated At:**")
            st.write(workflow.get('updated_at', 'N/A'))

        # Show errors if any
        if workflow.get('error_message'):
            st.markdown("---")
            st.error(f"**Error Message:** {workflow.get('error_message')}")

            if workflow.get('last_error_message'):
                st.error(f"**Last Error:** {workflow.get('last_error_message')}")

            if workflow.get('last_error_timestamp'):
                st.write(f"**Error Timestamp:** {workflow.get('last_error_timestamp')}")

        # Raw workflow data
        with st.expander("🔍 View Raw Workflow Data", expanded=False):
            st.json(workflow)

    def _render_content_tab(self, workflow: Dict[str, Any]):
        """Render content information tab."""
        st.subheader("📄 Content Details")

        if workflow.get('actual_content'):
            # Display content in expandable section
            with st.expander("View Full Content", expanded=True):
                st.text_area(
                    "Content Text:",
                    value=workflow.get('actual_content'),
                    height=400,
                    disabled=True,
                    key=f"actual_content_display_{workflow.get('automa_workflow_id', workflow.get('_id'))}"
                )

                # Display content statistics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Character Count", len(workflow.get('actual_content', '')))
                with col2:
                    word_count = len(workflow.get('actual_content', '').split())
                    st.metric("Word Count", word_count)
                with col3:
                    line_count = len(workflow.get('actual_content', '').split('\n'))
                    st.metric("Line Count", line_count)
                with col4:
                    st.metric("Content Hash", workflow.get('content_hash', 'N/A')[:8] + "...")
        else:
            st.info("ℹ️ No actual content available for this workflow")

            # Show content preview if available but full content is not
            if workflow.get('content_text_preview'):
                st.markdown("---")
                st.subheader("📝 Content Preview")
                st.info(workflow.get('content_text_preview'))

    def _render_artifacts_tab(self, workflow: Dict[str, Any]):
        """Render artifacts folder tab showing workflow structure."""
        st.subheader("📁 Workflow Artifacts")

        # Show drawflow structure
        if workflow.get('drawflow'):
            drawflow = workflow.get('drawflow', {})
            nodes = drawflow.get('nodes', {})

            # Handle both dict and list formats
            if isinstance(nodes, dict):
                total_blocks = len(nodes)
                nodes_list = list(nodes.values())
            elif isinstance(nodes, list):
                total_blocks = len(nodes)
                nodes_list = nodes
            else:
                total_blocks = 0
                nodes_list = []

            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Blocks", total_blocks)
            with col2:
                st.metric("Has Settings", "✅ Yes" if workflow.get('settings') else "❌ No")
            with col3:
                st.metric("Has Global Data", "✅ Yes" if workflow.get('globalData') else "❌ No")
            with col4:
                st.metric("Has Table Data", "✅ Yes" if workflow.get('table') else "❌ No")

            # Block types distribution
            if nodes_list:
                st.markdown("---")
                st.subheader("📊 Block Types Distribution")
                block_types = {}
                for node in nodes_list:
                    if isinstance(node, dict):
                        block_type = node.get('data', {}).get('name', 'Unknown')
                        block_types[block_type] = block_types.get(block_type, 0) + 1

                # Create a nice display
                block_df = pd.DataFrame([
                    {"Block Type": block_type, "Count": count}
                    for block_type, count in sorted(block_types.items())
                ])
                st.dataframe(block_df, use_container_width=True, hide_index=True)

            # Show complete drawflow structure
            with st.expander("🔍 View Complete Drawflow Structure", expanded=False):
                st.json(drawflow)

            # Settings if available
            if workflow.get('settings'):
                with st.expander("⚙️ Workflow Settings", expanded=False):
                    st.json(workflow.get('settings'))

            # Global data if available
            if workflow.get('globalData'):
                with st.expander("🌐 Global Data", expanded=False):
                    st.json(workflow.get('globalData'))

        else:
            st.info("ℹ️ No workflow artifacts available")

    def _render_links_tab(self, workflow: Dict[str, Any]):
        """Render links information tab."""
        st.subheader("🔗 Link Information")

        # Display link status
        col1, col2 = st.columns(2)

        with col1:
            st.write("**Link Status:**")
            st.write(f"• **Has Link:** {'✅ Yes' if workflow.get('has_link') else '❌ No'}")

            if workflow.get('link_url'):
                st.write(f"• **Link URL:**")
                st.markdown(f"[{workflow.get('link_url')}]({workflow.get('link_url')})")
            else:
                st.write(f"• **Link URL:** N/A")

        with col2:
            st.write("**Link Metadata:**")
            st.write(f"• **Linked At:** {workflow.get('link_assigned_at', 'N/A')}")
            st.write(f"• **Link ID:** {workflow.get('link_id', 'N/A')}")

        # Show automa execution logs for this workflow
        st.markdown("---")
        st.subheader("📋 Automa Execution Logs")

        try:
            from ui.components.automa_logs_viewer import AutomaLogsViewer
            logs_viewer = AutomaLogsViewer()

            # Get workflow metadata ID
            metadata_id = workflow.get('metadata_id')
            if metadata_id:
                logs_viewer.render_logs_for_workflow(metadata_id)
            else:
                # Try to find logs by workflow ID
                metadata_collection = get_mongo_collection("workflow_metadata")
                if metadata_collection:
                    workflow_id = workflow.get('automa_workflow_id', workflow.get('_id'))
                    if workflow_id:
                        try:
                            metadata_records = list(metadata_collection.find({
                                "automa_workflow_id": ObjectId(workflow_id) if isinstance(workflow_id, str) else workflow_id
                            }).sort("executed_at", -1).limit(5))

                            if metadata_records:
                                st.info(f"Found {len(metadata_records)} execution(s)")
                                for record in metadata_records:
                                    logs_viewer.render_logs_for_workflow(str(record['_id']))
                            else:
                                st.info("No execution logs available")
                        except:
                            st.info("No execution logs available")
                else:
                    st.info("No execution logs available")

        except ImportError:
            st.warning("AutomaLogsViewer not available")

    def _render_analysis_tab(self, workflow: Dict[str, Any]):
        """Render workflow analysis tab with pre-analyzed data."""
        st.subheader("📊 Workflow Analysis")

        try:
            # Import the analyzer UI
            from src.utils.workflow_analyzer import WorkflowAnalyzer
            from ui.components.workflow_analyzer_ui import WorkflowAnalyzerUI

            # Check if workflow has drawflow data
            if not workflow.get('drawflow'):
                st.warning("⚠️ No workflow structure data available for analysis")
                return

            # Create analyzer and perform analysis
            with st.spinner("Analyzing workflow..."):
                analyzer = WorkflowAnalyzer(workflow)
                analysis = analyzer.analyze()

            # Display analysis using the UI component
            WorkflowAnalyzerUI.render_analysis(workflow, show_raw=True)

        except ImportError as e:
            st.error(f"❌ Workflow analyzer not available: {e}")
            st.info("💡 Make sure workflow_analyzer.py and workflow_analyzer_ui.py are in the correct locations")
        except Exception as e:
            st.error(f"❌ Error analyzing workflow: {e}")
            import traceback
            with st.expander("View Error Details"):
                st.code(traceback.format_exc())

    def render_workflow_list(self, workflows: list, show_details: bool = True):
        """Render a list of workflows with optional detail view."""
        if not workflows:
            st.info("No workflows found matching the current filters.")
            return

        st.write(f"**Total Workflows:** {len(workflows)}")

        for idx, workflow in enumerate(workflows):
            # Show content preview in expander title if available
            content_preview = ""
            if workflow.get('actual_content'):
                content_preview = f" - {workflow.get('actual_content')[:50]}..."

            with st.expander(
                f"{'✅' if workflow.get('success') else '❌'} {workflow.get('workflow_name', 'Unnamed')} - "
                f"ID: {workflow.get('postgres_content_id', 'N/A')} - "
                f"Account: {workflow.get('username', 'N/A')}{content_preview}",
                expanded=False
            ):
                if show_details:
                    self.render_workflow_details(workflow)
                else:
                    # Quick summary view
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.write(f"**Status:** {workflow.get('status', 'unknown')}")
                        st.write(f"**Has Content:** {'✅' if workflow.get('has_content') else '❌'}")
                    with col2:
                        st.write(f"**Executed:** {'✅' if workflow.get('executed') else '❌'}")
                        st.write(f"**Success:** {'✅' if workflow.get('success') else '❌'}")
                    with col3:
                        st.write(f"**Account:** {workflow.get('username', 'N/A')}")
                        st.write(f"**Content ID:** {workflow.get('postgres_content_id', 'N/A')}")

                    if st.button("View Full Details", key=f"view_details_{idx}_{workflow.get('_id')}"):
                        self.render_workflow_details(workflow)

    def render(self):
        """Render simplified workflow content focused on actual workflows."""
        col1, col2 = st.columns([3, 1])
        with col1:
            st.subheader(f"{self.workflow_type} Workflows")
        with col2:
            if st.button(f"Refresh {self.workflow_type}", key=f"refresh_all_{self.workflow_type_lower}"):
                self._refresh_all_data()
                st.success(f"{self.workflow_type} data refreshed!")

        workflow_filters = WorkflowFilters(self.workflow_type)
        workflow_filters.render_filters()
        filters = workflow_filters.build_filters()

        self._render_actual_workflows_section(filters)

    def _render_actual_workflows_section(self, filters: Dict[str, Any]):
        """Render the main section showing actual workflows with filters applied."""
        metadata_collection = get_mongo_collection("workflow_metadata")
        if metadata_collection is None:
            st.error("Cannot access workflow_metadata collection")
            return

        try:
            query = {"workflow_type": self.workflow_type.lower()}

            if "_metadata_filters" in filters:
                query.update(filters["_metadata_filters"])

            cursor = metadata_collection.find(query).sort("generated_at", -1)
            metadata_records = list(cursor)

            if not metadata_records:
                st.info(f"No {self.workflow_type} workflows found matching the filters.")
                return

            workflow_ids = [str(record["automa_workflow_id"]) for record in metadata_records if "automa_workflow_id" in record]

            cache_key = f"actual_workflows_{self.workflow_type_lower}"
            if cache_key not in st.session_state:
                st.session_state[cache_key] = self._get_actual_workflows(workflow_ids)
            actual_workflows = st.session_state[cache_key]

            metadata_map = {}
            for record in metadata_records:
                wf_id = str(record["automa_workflow_id"])
                if wf_id in metadata_map:
                    continue
                metadata_map[wf_id] = {
                    "metadata_id": str(record["_id"]),
                    "content_id": record.get("postgres_content_id"),
                    "account_id": record.get("postgres_account_id"),
                    "username": record.get("username"),
                    "has_link": record.get("has_link", False),
                    "has_content": record.get("has_content", False),
                    "executed": record.get("executed", False),
                    "success": record.get("success", False),
                    "status": record.get("status", "unknown"),
                    "generated_at": record.get("generated_at"),
                    "executed_at": record.get("executed_at"),
                    "blocks_generated": record.get("blocks_generated", 0),
                    "template_used": record.get("template_used"),
                    "error_message": record.get("error_message"),
                    "actual_content": record.get("actual_content"),
                    "content_length": record.get("content_length"),
                    "link_url": record.get("link_url")
                }

            for wf in actual_workflows:
                wf_id = wf["_id"]
                if wf_id in metadata_map:
                    wf.update(metadata_map[wf_id])

            sort_key = filters.get("sort_by", "Name (A-Z)")
            if sort_key == "Name (A-Z)":
                actual_workflows.sort(key=lambda x: x.get("name", "").lower())
            elif sort_key == "Name (Z-A)":
                actual_workflows.sort(key=lambda x: x.get("name", "").lower(), reverse=True)

            st.success(f"✅ {len(actual_workflows)} {self.workflow_type.lower()} workflows found")

            self._render_simplified_workflows_table(actual_workflows)
            self.render_workflow_list(actual_workflows, show_details=True)

        except Exception as e:
            st.error(f"Error rendering workflows: {e}")
            import traceback
            st.code(traceback.format_exc())

    def _get_actual_workflows(self, workflow_ids: List[str]) -> List[Dict[str, Any]]:
        """Fetch actual workflow definitions from their storage locations."""
        try:
            client = self._get_mongo_client()
            metadata_collection = self._get_metadata_collection(client)

            object_ids = []
            for wf_id in workflow_ids:
                try:
                    object_ids.append(ObjectId(wf_id) if isinstance(wf_id, str) else wf_id)
                except:
                    continue

            if not object_ids:
                client.close()
                return []

            metadata_records = list(metadata_collection.find({
                "automa_workflow_id": {"$in": object_ids}
            }))

            location_map = {}
            for meta in metadata_records:
                db_name = meta.get('database_name', 'execution_workflows')
                coll_name = meta.get('collection_name', 'unknown')
                wf_id = meta.get('automa_workflow_id')

                key = (db_name, coll_name)
                if key not in location_map:
                    location_map[key] = []
                location_map[key].append(wf_id)

            all_workflows = []
            for (db_name, coll_name), wf_ids in location_map.items():
                try:
                    target_db = client[db_name]
                    target_collection = target_db[coll_name]

                    workflows = list(target_collection.find({"_id": {"$in": wf_ids}}))

                    for wf in workflows:
                        wf['_id'] = str(wf['_id'])
                        wf['storage_database'] = db_name
                        wf['storage_collection'] = coll_name

                    all_workflows.extend(workflows)

                except Exception as e:
                    st.error(f"Error fetching from {db_name}.{coll_name}: {e}")
                    continue

            client.close()
            return all_workflows

        except Exception as e:
            st.error(f"Error fetching actual workflows: {e}")
            return []

    def _render_simplified_workflows_table(self, workflows: List[Dict[str, Any]]):
        """Render simplified table with key status indicators."""
        if not workflows:
            return

        st.subheader("📋 Workflows Summary Table")

        workflow_summary = []
        for wf in workflows:
            total_blocks = 0
            drawflow = wf.get('drawflow', {})
            if drawflow:
                nodes = drawflow.get('nodes', {})
                if isinstance(nodes, dict):
                    total_blocks = len(nodes)
                elif isinstance(nodes, list):
                    total_blocks = len(nodes)

            workflow_summary.append({
                'ID': wf.get('_id', 'Unknown')[:12] + '...',
                'Name': wf.get('name', 'Unknown'),
                'Description': self._truncate_text(wf.get('description', ''), 60),
                'Version': wf.get('version', 'Unknown'),
                'Total Blocks': total_blocks,
                '✅ Has Content': '✅ Yes' if wf.get('has_content') else '❌ No',
                '🔗 Has Link': '✅ Yes' if wf.get('has_link') else '❌ No',
                '⚙️ Executed': '✅ Yes' if wf.get('executed') else '❌ No',
                '✔️ Success': '✅ Yes' if wf.get('success') else '❌ No',
                '📊 Status': wf.get('status', 'unknown'),
                '📄 Content ID': wf.get('content_id', 'N/A'),
                '👤 Account ID': wf.get('account_id', 'N/A')
            })

        df_workflows = pd.DataFrame(workflow_summary)
        st.dataframe(df_workflows, use_container_width=True, height=400)

    def _refresh_all_data(self):
        """Refresh all cached data for this workflow type."""
        keys_to_clear = [
            f'actual_workflows_{self.workflow_type_lower}',
            f'workflow_details_{self.workflow_type_lower}',
        ]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]

    def _truncate_text(self, text: str, max_length: int) -> str:
        """Truncate text to specified length with ellipsis."""
        if not text:
            return ''
        return text[:max_length] + '...' if len(text) > max_length else text


