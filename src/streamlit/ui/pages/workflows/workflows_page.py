import streamlit as st
from typing import Dict, Any, List
import json
from datetime import datetime

from ..base_page import BasePage
from src.core.database.mongodb.workflow_stats import WorkflowStatsManager
from src.core.database.mongodb import (
    replies_workflows,
    messages_workflows,
    retweets_workflows,
)
from .stats_dashboard import render_stats_dashboard
from .data_tables import render_workflow_tables
from .template_workflows_manager import TemplateWorkflowsManager
from ...settings.settings_manager import get_system_setting

# ✅ NEW: Import orchestrator monitor
from ui.components.orchestrator_monitor import render_orchestrator_monitor


class WorkflowsPage(BasePage):
    """Enhanced Workflows page with Template Management and Automa Logs integration."""

    def __init__(self, db_manager):
        super().__init__(db_manager)
        self.template_manager = TemplateWorkflowsManager()
        self.available_categories, self.available_workflow_types = self._get_available_categories_and_types()
        self.stats_manager = WorkflowStatsManager(self.available_categories)

    def _get_available_workflow_types_list(self):
        """Get flattened list of all workflow types across all categories."""
        all_types = []
        for category, types in self.available_workflow_types.items():
            all_types.extend(types)
        return sorted(list(set(all_types)))

    def _get_mongo_client():
        """Get MongoDB client connection."""
        return MongoClient(
            os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
            serverSelectionTimeoutMS=5000
        )

    # ============================================================================
    # HELPER: Get Metadata Collection
    # ============================================================================
    def _get_metadata_collection(client):
        """Get workflow_metadata collection."""
        messages_db = client['messages_db']
        return messages_db['workflow_metadata']

    def _get_available_categories_and_types(self):
        """Get all available workflow categories and types from settings."""
        try:
            workflow_categories = get_system_setting('workflow_categories', {})
            if not workflow_categories or not isinstance(workflow_categories, dict):
                st.warning("No workflow categories found. Configure in Automa Workflow Config first.")
                return [], {}

            categories = sorted(list(workflow_categories.keys()))
            types_by_category = {}

            for category, types in workflow_categories.items():
                if isinstance(types, list):
                    types_by_category[category] = sorted(types)

            return categories, types_by_category

        except Exception as e:
            st.error(f"❌ Error fetching workflow categories: {e}")
            return [], {}

    @st.cache_data(ttl=300, show_spinner=False)
    def _get_cached_workflow_summary(_self):
        """Get cached workflow summary with automatic refresh."""
        return _self.stats_manager.get_workflow_type_summary()

    def render(self):
        """Main render method with Template Management, Workflows Overview,
        Automa Logs, Orchestrator Monitor, and Link Assignments."""

        col1, col2 = st.columns([3, 1])
        with col1:
            st.header("🚀 Enhanced Workflows Dashboard")
        with col2:
            if st.button("🔄 Refresh All", key="refresh_all_workflows"):
                self._refresh_all_workflow_data()
                st.success("✅ All workflow data refreshed!")

        if not self.available_categories:
            st.warning("""
            ⚠️ No workflow categories found.

            **To use the workflows dashboard:**
            1. Go to **Settings → 🤖 Automa Workflow Config**
            2. Create at least one workflow category and type
            3. Return here to view and manage workflows
            """)
            self.template_manager.render()
            return

        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 Workflows Overview",
            "📋 Automa Logs",
            "📝 Create Template Workflows",
            "🎼 Orchestrator Monitor",
            "🔗 Link Assignments",
        ])

        with tab1:
            render_stats_dashboard(
                self.stats_manager,
                self._get_cached_workflow_summary,
                self.available_categories,
                self.available_workflow_types
            )

        with tab2:
            self._render_automa_logs_section()

        with tab3:
            self.template_manager.render()

        with tab4:
            render_orchestrator_monitor()

        with tab5:
            self._render_link_assignments_section()

    def _render_link_assignments_section(self):
        """
        Render the Link Assignments tab.
        Shows each account's workflow assignments — every account gets
        one workflow per filtered link.
        """
        st.subheader("🔗 Link Assignments")
        st.caption(
            "Every filtered link is assigned to **one workflow per account**. "
            "With 3 accounts and 5 links, each account gets 5 workflows assigned."
        )

        # ── Controls ──────────────────────────────────────────────────────────
        ctrl1, ctrl2, ctrl3 = st.columns([2, 2, 1])

        with ctrl1:
            account_options = {"All Accounts": None}
            try:
                from src.core.database.mongodb.connection import get_mongo_collection
                meta = get_mongo_collection("workflow_metadata")
                if meta:
                    pipeline = [
                        {"$match": {"has_link": True, "postgres_account_id": {"$exists": True}}},
                        {"$group": {
                            "_id": "$postgres_account_id",
                            "username": {"$first": "$username"}
                        }},
                        {"$sort": {"username": 1}}
                    ]
                    for r in meta.aggregate(pipeline):
                        aid   = r["_id"]
                        uname = r.get("username") or f"account_{aid}"
                        account_options[f"{uname} (ID: {aid})"] = aid
            except Exception:
                pass

            selected_account_label = st.selectbox(
                "Filter by Account",
                list(account_options.keys()),
                key="la_account_filter",
            )
            selected_account_id = account_options[selected_account_label]

        with ctrl2:
            limit = st.number_input(
                "Show last N assignments",
                min_value=5, max_value=200, value=25,
                key="la_limit",
            )

        with ctrl3:
            st.write("")
            st.write("")
            if st.button("🔄 Refresh", key="la_refresh"):
                st.rerun()

        # ── Fetch records ──────────────────────────────────────────────────────
        try:
            from src.core.database.mongodb.connection import get_mongo_collection
            meta = get_mongo_collection("workflow_metadata")
            if meta is None:
                st.error("Cannot connect to workflow_metadata")
                return

            query: dict = {"has_link": True, "link_assigned_at": {"$exists": True}}
            if selected_account_id is not None:
                query["postgres_account_id"] = selected_account_id

            records = list(
                meta.find(query)
                    .sort("link_assigned_at", -1)
                    .limit(int(limit))
            )
            for r in records:
                r['_id'] = str(r['_id'])
                if r.get('automa_workflow_id'):
                    r['automa_workflow_id'] = str(r['automa_workflow_id'])

        except Exception as e:
            st.error(f"Error fetching assignments: {e}")
            return

        if not records:
            st.info("No link assignments found. Run the filter_links DAG first.")
            return

        # ── Top-level summary metrics ──────────────────────────────────────────
        total_wf       = len(records)
        injected_tweet = sum(1 for r in records if r.get('injection_status', {}).get('tweet_url_injected'))
        injected_chat  = sum(1 for r in records if r.get('injection_status', {}).get('chat_link_injected'))
        direct_chat    = sum(1 for r in records
                            if r.get('chat_link') and 'messages' not in r.get('chat_link', ''))
        accounts_seen  = len(set(r.get('postgres_account_id') for r in records))

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Total Assignments",  total_wf)
        m2.metric("Accounts",           accounts_seen)
        m3.metric("Tweet URL Injected", injected_tweet)
        m4.metric("Chat Link Injected", injected_chat)
        m5.metric("Direct Chat Links",  direct_chat)

        st.markdown("---")

        # ── Fetch x_account_id map from PostgreSQL ─────────────────────────────
        x_account_map: dict = {}
        try:
            from src.core.database.postgres.connection import get_postgres_connection
            with get_postgres_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT account_id, username, x_account_id FROM accounts")
                    for row in cur.fetchall():
                        if hasattr(row, 'get'):
                            aid, uname, xid = row.get('account_id'), row.get('username'), row.get('x_account_id')
                        else:
                            aid, uname, xid = row[0], row[1], row[2] if len(row) > 2 else None
                        x_account_map[aid] = {"username": uname, "x_account_id": xid}
        except Exception as e:
            st.warning(f"Could not load account x_account_ids: {e}")

        # ── Group by account ───────────────────────────────────────────────────
        by_account: dict = {}
        for r in records:
            aid = r.get('postgres_account_id') or 0
            by_account.setdefault(aid, []).append(r)

        for account_id, acct_records in sorted(by_account.items()):
            info      = x_account_map.get(account_id, {})
            username  = info.get('username') or f"account_{account_id}"
            x_acct_id = info.get('x_account_id')

            acct_tweet_ok = sum(1 for r in acct_records
                                if r.get('injection_status', {}).get('tweet_url_injected'))
            acct_chat_ok  = sum(1 for r in acct_records
                                if r.get('chat_link') and 'messages' not in r.get('chat_link', ''))

            with st.expander(
                f"👤 **{username}** (ID: {account_id})  —  "
                f"{len(acct_records)} workflow(s)  |  "
                f"✅ {acct_tweet_ok} tweet injected  |  "
                f"💬 {acct_chat_ok} direct chat",
                expanded=(len(by_account) == 1),
            ):
                # x_account_id health check
                xcol1, xcol2 = st.columns(2)
                with xcol1:
                    if x_acct_id:
                        st.success(f"✅ x_account_id (DM sender): `{x_acct_id}`")
                    else:
                        st.error(
                            "❌ `x_account_id` not set — chat links will fall back to "
                            "`x.com/messages`. Set it in **Settings → Accounts**."
                        )
                with xcol2:
                    st.caption(
                        f"Links assigned: {len(acct_records)} "
                        f"(one workflow per link)"
                    )

                for rec in acct_records:
                    self._render_assignment_card(rec)

    def _render_assignment_card(self, rec: dict):
        """Render a single workflow→link assignment card."""
        wf_id       = rec.get('automa_workflow_id', 'unknown')
        assigned_at = rec.get('link_assigned_at', 'unknown')
        tweet_url   = rec.get('link_url') or ''
        chat_link   = rec.get('chat_link') or ''
        injection   = rec.get('injection_status', {})
        category    = rec.get('category', '')
        wf_type     = rec.get('workflow_type', '')

        tweet_ok       = injection.get('tweet_url_injected', False)
        chat_ok        = injection.get('chat_link_injected', False)
        is_direct_chat = bool(chat_link and 'messages' not in chat_link)
        status_icon    = "✅" if (tweet_ok and chat_ok) else "⚠️"

        st.markdown(f"**{status_icon} Workflow** `{str(wf_id)[:24]}...`")

        col_a, col_b, col_c = st.columns(3)

        with col_a:
            st.caption("**Assignment**")
            st.caption(f"At: {assigned_at}")
            st.caption(f"`{category}` / `{wf_type}`")

        with col_b:
            st.caption("**Injection**")
            st.caption(
                f"{'✅' if tweet_ok else '❌'} Tweet URL  \n"
                f"{'✅' if chat_ok  else '❌'} Chat link  \n"
                f"new-tab blocks: {injection.get('new_tab_count', 0)}"
            )

        with col_c:
            st.caption("**Chat Link**")
            if is_direct_chat:
                st.success("💬 Direct DM")
                try:
                    parts = chat_link.rstrip('/').split('/')[-1].split('-')
                    if len(parts) == 2:
                        st.caption(f"Sender: `{parts[0]}`  \nRecipient: `{parts[1]}`")
                except Exception:
                    pass
            else:
                st.warning("⚠️ Fallback")

        url1, url2 = st.columns(2)
        with url1:
            if tweet_url:
                short = tweet_url[:55] + "..." if len(tweet_url) > 55 else tweet_url
                st.markdown(f"🐦 [{short}]({tweet_url})")
            else:
                st.caption("🐦 No tweet URL")
        with url2:
            if chat_link:
                short = chat_link[:55] + "..." if len(chat_link) > 55 else chat_link
                st.markdown(f"💬 [{short}]({chat_link})")
            else:
                st.caption("💬 No chat link")

        st.markdown("---")

    def _bulk_export_workflows(selected_categories: List[str]):
        """Export all workflows from selected categories."""
        st.info(f"Starting bulk export for categories: {', '.join(selected_categories)}")

        try:
            client = _get_mongo_client()
            metadata_collection = _get_metadata_collection(client)

            for category in selected_categories:
                try:
                    # Find all database/collection locations for this category
                    pipeline = [
                        {"$match": {"category": category.lower()}},
                        {
                            "$group": {
                                "_id": {
                                    "database": "$database_name",
                                    "collection": "$collection_name",
                                    "workflow_type": "$workflow_type"
                                },
                                "count": {"$sum": 1},
                                "workflow_ids": {"$push": "$automa_workflow_id"}
                            }
                        },
                        {"$sort": {"_id.workflow_type": 1}}
                    ]

                    locations = list(metadata_collection.aggregate(pipeline))

                    if locations:
                        # Export each collection
                        for location in locations:
                            db_name = location['_id']['database']
                            coll_name = location['_id']['collection']
                            workflow_type = location['_id']['workflow_type']
                            workflow_ids = location['workflow_ids']

                            # Fetch workflows from their location
                            target_db = client[db_name]
                            target_collection = target_db[coll_name]

                            object_ids = [ObjectId(wf_id) if isinstance(wf_id, str) else wf_id
                                        for wf_id in workflow_ids]

                            workflows = list(target_collection.find({"_id": {"$in": object_ids}}))

                            if workflows:
                                # Convert ObjectId to string
                                for wf in workflows:
                                    wf['_id'] = str(wf['_id'])

                                import json
                                from datetime import datetime
                                json_data = json.dumps(workflows, indent=2, default=str)
                                ts = datetime.now().strftime("%Y%m%d_%H%M%S")

                                st.download_button(
                                    label=f"Download {category}/{workflow_type}/{coll_name}",
                                    data=json_data,
                                    file_name=f"{category}_{workflow_type}_{coll_name}_{ts}.json",
                                    mime="application/json",
                                    key=f"download_{category}_{workflow_type}_{coll_name}_{ts}"
                                )
                                st.success(f"✅ {category}/{workflow_type}/{coll_name}: {len(workflows)} workflows")
                            else:
                                st.warning(f"⚠️ No workflows in {db_name}.{coll_name}")
                    else:
                        st.warning(f"⚠️ No collections found for category '{category}'")

                except Exception as e:
                    st.error(f"Export error for category '{category}': {e}")

            client.close()

        except Exception as e:
            st.error(f"❌ Database connection error: {e}")

    def _render_automa_logs_section(self):
        """Render Automa execution logs section."""
        st.subheader("📋 Automa Execution Logs")
        st.caption("View detailed execution logs from all workflow runs")

        # Import the logs viewer
        try:
            from ui.components.automa_logs_viewer import AutomaLogsViewer

            viewer = AutomaLogsViewer()

            # Add filter options - use categories and workflow types
            col1, col2, col3 = st.columns(3)

            with col1:
                # Category filter
                category_options = ["All Categories"] + self.available_categories
                selected_category = st.selectbox(
                    "Filter by Category:",
                    options=category_options,
                    key="logs_category_filter"
                )

            with col2:
                # Workflow type filter (dynamic based on selected category)
                if selected_category == "All Categories":
                    # Show all types across all categories
                    all_types = self._get_available_workflow_types_list()
                    type_options = ["All Types"] + all_types
                else:
                    # Show types for selected category
                    types_in_category = self.available_workflow_types.get(selected_category, [])
                    type_options = ["All Types"] + types_in_category

                selected_type = st.selectbox(
                    "Filter by Type:",
                    options=type_options,
                    key="logs_type_filter"
                )

            with col3:
                status_filter = st.selectbox(
                    "Filter by Status:",
                    ["All", "Success", "Failed"],
                    key="logs_status_filter"
                )

            # Limit selector
            limit = st.number_input(
                "Show Last N Logs:",
                min_value=5,
                max_value=100,
                value=20,
                key="logs_limit"
            )

            # Build filters
            filters = {}
            if selected_category != "All Categories":
                filters['category'] = selected_category.lower()
            if selected_type != "All Types":
                filters['workflow_type'] = selected_type.lower()
            if status_filter == "Failed":
                filters['has_errors'] = True
            elif status_filter == "Success":
                filters['has_errors'] = False

            # Render logs
            st.markdown("---")
            viewer.render_logs_list(filters=filters, limit=limit)

        except ImportError:
            st.error("❌ AutomaLogsViewer component not found. Please ensure it's properly installed.")
            st.info("💡 Create the component at: ui/components/automa_logs_viewer.py")

            # Show basic logs fallback
            self._render_basic_logs_fallback()

    def _render_basic_logs_fallback(self):
        """Fallback method to display basic logs if AutomaLogsViewer is not available."""
        from src.core.database.mongodb.connection import get_mongo_collection

        logs_collection = get_mongo_collection("automa_execution_logs")
        if logs_collection is None:
            st.warning("Cannot connect to automa_execution_logs collection")
            return

        try:
            # Fetch recent logs
            logs = list(logs_collection.find().sort("created_at", -1).limit(20))

            if not logs:
                st.info("No execution logs found")
                return

            st.success(f"Found {len(logs)} recent execution logs")

            # Display each log
            for log in logs:
                with st.expander(
                    f"{'❌' if log.get('has_errors') else '✅'} "
                    f"{log.get('workflow_name', 'Unknown')} - "
                    f"{log.get('execution_id', 'Unknown')[:20]}...",
                    expanded=False
                ):
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.write(f"**Workflow Type:** {log.get('workflow_type', 'Unknown')}")
                        st.write(f"**Log Count:** {log.get('log_count', 0)}")

                    with col2:
                        st.write(f"**Status:** {'❌ Failed' if log.get('has_errors') else '✅ Success'}")
                        st.write(f"**Created:** {log.get('created_at', 'Unknown')}")

                    with col3:
                        st.write(f"**Session ID:** {log.get('session_id', 'N/A')[:20]}...")

                    # Show logs
                    if log.get('logs'):
                        st.markdown("**Execution Steps:**")
                        for idx, step in enumerate(log.get('logs', [])[:10]):
                            st.caption(f"{idx + 1}. [{step.get('name', 'Unknown')}] {step.get('description', 'No description')}")

                    # Raw data
                    with st.expander("View Raw Log Data"):
                        st.json(log)

        except Exception as e:
            st.error(f"Error loading logs: {e}")

    def _get_workflow_filters(self, workflow_type: str) -> Dict[str, Any]:
        """Use the enhanced WorkflowFilters class for hierarchical filtering."""
        from .workflow_filters import WorkflowFilters

        filter_key = f"workflow_filters_{workflow_type.lower()}"

        if filter_key not in st.session_state:
            st.session_state[filter_key] = WorkflowFilters(workflow_type)

        workflow_filters = st.session_state[filter_key]
        workflow_filters.render_filters()

        return workflow_filters.build_filters()

    def _show_quick_stats(self):
        """Show quick statistics for all workflow categories."""
        with st.expander("Quick Workflow Statistics", expanded=True):
            if not self.available_categories:
                st.info("No workflow categories configured yet.")
                return

            stats: Dict[str, Dict] = {}

            # Collect stats for each category
            for category in self.available_categories:
                try:
                    # Get stats for all types in this category
                    category_stats = self.stats_manager.get_category_stats(category)
                    stats[category] = {
                        "total": category_stats.get('total_workflows', 0),
                        "executed": category_stats.get('executed_workflows', 0),
                        "successful": category_stats.get('successful_executions', 0),
                        "failed": category_stats.get('failed_executions', 0),
                        "types": len(self.available_workflow_types.get(category, [])),
                        "collections": category_stats.get('total_collections', 0)
                    }
                except Exception as e:
                    st.error(f"Error getting stats for category '{category}': {e}")
                    stats[category] = {
                        "total": 0,
                        "executed": 0,
                        "successful": 0,
                        "failed": 0,
                        "types": 0,
                        "collections": 0
                    }

            # Display in a grid
            cols = st.columns(min(3, len(self.available_categories)))

            for idx, category in enumerate(self.available_categories):
                col_idx = idx % 3
                with cols[col_idx]:
                    s = stats.get(category, {})
                    st.subheader(f"📁 {category}")
                    st.metric("Total Workflows", s.get("total", 0))
                    st.metric("Workflow Types", s.get("types", 0))
                    st.metric("Collections", s.get("collections", 0))
                    st.metric("Executed", s.get("executed", 0))
                    st.metric("Successful", s.get("successful", 0))
                    st.metric("Failed", s.get("failed", 0))

    def render_workflow_actions_sidebar(self):
        """Render workflow actions in sidebar for quick access."""
        with st.sidebar:
            st.subheader("Workflow Actions")

            if st.button("Quick Stats", key="quick_stats"):
                self._show_quick_stats()

            st.subheader("Bulk Operations")
            selected_categories = st.multiselect(
                "Select categories:",
                self.available_categories,
                key="bulk_categories",
            )

            if selected_categories:
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Export All", key="bulk_export"):
                        self._bulk_export_workflows(selected_categories)
                with col2:
                    if st.button("Refresh All", key="bulk_refresh"):
                        self._bulk_refresh_workflows(selected_categories)

    def _refresh_all_workflow_data(self):
        """Clear all workflow-related session-state keys."""
        keys_to_clear = []

        # Clear category-based cache
        for category in self.available_categories:
            types = self.available_workflow_types.get(category, [])
            for wf_type in types:
                keys_to_clear.extend([
                    f"workflows_{category}_{wf_type}",
                    f"stats_{category}_{wf_type}",
                    f"logs_{category}_{wf_type}",
                ])

        # Clear template cache
        keys_to_clear.append('templates_cache')

        # Clear category cache
        keys_to_clear.extend([
            'categories_cache',
            'workflow_types_cache',
            'collections_cache'
        ])

        for k in keys_to_clear:
            st.session_state.pop(k, None)

        if hasattr(self._get_cached_workflow_summary, "clear"):
            self._get_cached_workflow_summary.clear()

        # Refresh categories and types
        self.available_categories, self.available_workflow_types = self._get_available_categories_and_types()

    def _bulk_refresh_workflows(self, selected_categories: List[str]):
        """Clear cache for selected categories."""
        for category in selected_categories:
            types = self.available_workflow_types.get(category, [])
            for wf_type in types:
                keys = [
                    f"workflows_{category}_{wf_type}",
                    f"stats_{category}_{wf_type}",
                    f"logs_{category}_{wf_type}",
                ]
                for k in keys:
                    st.session_state.pop(k, None)

        st.success(f"Cache cleared for categories: {', '.join(selected_categories)}")
