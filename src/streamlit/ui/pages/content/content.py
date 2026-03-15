# File: streamlit/ui/pages/actions_page.py
# CONTENT-ONLY - Dynamic tabs based on custom prompt types
# ENHANCED: Integrated link-content sync functionality + Same Link Filter
# UPDATED: Delete buttons for individual rows and bulk-by-name

import logging
import pandas as pd
import streamlit as st
from src.core.database.postgres import accounts as pg_utils
from src.core.database.postgres.content_handler import get_content_handler, get_all_content_types
from src.core.database.postgres.link_content_sync import (
    sync_all_workflow_connections,
    verify_link_content_connections,
    repair_broken_connections
)
from ..base_page import BasePage
from datetime import datetime, date
from src.core.database.postgres.connection import get_postgres_connection
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class ContentPage(BasePage):
    """Actions page with DYNAMIC tabs based on custom prompt types ONLY."""

    def render(self):
        """Render the Actions main page with dynamic tabs."""
        st.header("⚡ Content Management")

        header_col1, header_col2 = st.columns([3, 2])

        with header_col1:
            self._render_account_selector()

        with header_col2:
            st.markdown("<div style='margin-bottom: 0.5rem;'></div>", unsafe_allow_html=True)
            btn_col1, btn_col2, btn_col3, btn_col4 = st.columns(4)

            with btn_col1:
                if st.button("🔗 **Sync**", key="sync_links", use_container_width=True,
                             help="Sync link-content connections for all workflows"):
                    self._run_sync()

            with btn_col2:
                if st.button("🔍 **Verify**", key="verify_links", use_container_width=True,
                             help="Verify link-content connection status"):
                    self._verify_connections()

            with btn_col3:
                if st.button("🗑️ **Clear**", key="clear_cache", use_container_width=True,
                             help="Clear all cached data"):
                    st.cache_data.clear()
                    st.success("✅ Cache cleared!")
                    st.rerun()

            with btn_col4:
                if st.button("🔄 **Refresh**", key="refresh_actions", use_container_width=True,
                             help="Refresh page and clear session state"):
                    keys_to_clear = [k for k in st.session_state.keys() if k.endswith('_export_trigger')]
                    for key in keys_to_clear:
                        del st.session_state[key]
                    if 'sync_status' in st.session_state:
                        del st.session_state.sync_status
                    st.success("✅ Content page refreshed!")
                    st.rerun()

        st.markdown("<div style='margin-bottom: 1rem;'></div>", unsafe_allow_html=True)

        if 'sync_status' in st.session_state:
            status = st.session_state.sync_status
            with st.container():
                if status['type'] == 'success':
                    st.success(f"✅ {status['message']}")
                elif status['type'] == 'info':
                    st.info(f"ℹ️ {status['message']}")
                elif status['type'] == 'warning':
                    st.warning(f"⚠️ {status['message']}")
                elif status['type'] == 'error':
                    st.error(f"❌ {status['message']}")

            dismiss_col1, dismiss_col2, dismiss_col3 = st.columns([0.7, 0.15, 0.15])
            with dismiss_col2:
                if st.button("Dismiss", key="dismiss_status", use_container_width=True):
                    del st.session_state.sync_status
                    st.rerun()
            st.markdown("---")

        prompt_types = get_all_content_types()

        if not prompt_types:
            st.error("❌ No custom prompt types found. Please create prompts first.")
            st.info("💡 **To get started:**\n"
                    "1. Go to **Prompts** page\n"
                    "2. Click **Create New Prompt**\n"
                    "3. Enter your custom prompt type (e.g., 'testimonials', 'product_launches')\n"
                    "4. Your custom types will appear here as tabs")
            return

        st.success(f"✅ Found **{len(prompt_types)} content types**: {', '.join(prompt_types)}")

        tab_labels = [ptype.replace('_', ' ').title() for ptype in prompt_types] + ["📊 Analytics"]
        tabs = st.tabs(tab_labels)

        selected_account_id = st.session_state.get('selected_account_id', None)

        for i, prompt_type in enumerate(prompt_types):
            with tabs[i]:
                self._render_content_type_section(prompt_type, selected_account_id)

        with tabs[-1]:
            self._render_analytics_section(selected_account_id, prompt_types)

    # =========================================================================
    # SYNC / VERIFY
    # =========================================================================

    def _run_sync(self):
        with st.spinner("🔄 Syncing link-content connections..."):
            try:
                result = sync_all_workflow_connections()
                if 'error' in result:
                    st.session_state.sync_status = {'type': 'error', 'message': f"❌ Sync failed: {result['error']}"}
                else:
                    total = result.get('total_found', 0)
                    synced = result.get('synced', 0)
                    failed = result.get('failed', 0)
                    if total == 0:
                        st.session_state.sync_status = {'type': 'info', 'message': "ℹ️ No unconnected workflow matches found."}
                    elif failed > 0:
                        st.session_state.sync_status = {'type': 'warning', 'message': f"✓ Synced {synced} (Found: {total}, Failed: {failed})"}
                    else:
                        st.session_state.sync_status = {'type': 'success', 'message': f"✓ Successfully synced {synced} link-content connections!"}
            except Exception as e:
                logger.error(f"Error during sync: {e}")
                st.session_state.sync_status = {'type': 'error', 'message': f"❌ Sync error: {str(e)}"}
        st.rerun()

    def _verify_connections(self):
        with st.spinner("🔍 Verifying connections..."):
            try:
                stats = verify_link_content_connections()
                if 'error' in stats:
                    st.session_state.sync_status = {'type': 'error', 'message': f"❌ Verification failed: {stats['error']}"}
                else:
                    bidirectional = stats.get('bidirectional_valid', 0)
                    broken_links = stats.get('broken_from_links', 0)
                    broken_content = stats.get('broken_from_content', 0)
                    total_broken = broken_links + broken_content
                    if total_broken > 0:
                        st.session_state.sync_status = {
                            'type': 'warning',
                            'message': (f"⚠️ {bidirectional} valid bidirectional | "
                                        f"{total_broken} broken. Click 'Sync' to repair.")
                        }
                    else:
                        st.session_state.sync_status = {
                            'type': 'success',
                            'message': (f"✓ All connections healthy! "
                                        f"Links with content: {stats.get('links_with_content', 0)} | "
                                        f"Bidirectional: {bidirectional}")
                        }
            except Exception as e:
                logger.error(f"Error during verification: {e}")
                st.session_state.sync_status = {'type': 'error', 'message': f"❌ Verification error: {str(e)}"}
        st.rerun()

    # =========================================================================
    # CONTENT TYPE SECTION (top-level)
    # =========================================================================

    def _render_content_type_section(self, content_type: str, account_id: int = None):
        display_name = content_type.replace('_', ' ').title()
        st.subheader(f"📋 {display_name}")

        content_tab1, content_tab2, content_tab3 = st.tabs([
            "📋 Content List",
            "🔗 Connections",
            "📊 Statistics"
        ])

        with content_tab1:
            self._render_content_list_with_filters(content_type, account_id)

        with content_tab2:
            self._render_content_connections_view(content_type, account_id)

        with content_tab3:
            self._render_content_statistics(content_type, account_id)

    # =========================================================================
    # CONTENT LIST WITH FILTERS (outer shell)
    # =========================================================================

    def _render_content_list_with_filters(self, content_type: str, account_id: int = None):
        try:
            self._render_content_stats(content_type, account_id)
            st.divider()
            self._render_content_name_breakdown(content_type, account_id)
            st.divider()
            with st.expander("🔍 Drill-down: filter & view individual rows", expanded=False):
                self._render_content_filters(content_type, account_id)
                st.divider()
                self._render_content_table(content_type, account_id)
        except Exception as e:
            logger.error(f"Error rendering content list for {content_type}: {e}")
            st.error(f"Error loading content list: {str(e)}")

    # =========================================================================
    # DELETE HELPERS
    # =========================================================================

    def _delete_single_content(self, content_type: str, content_id: int, invalidate_key: str):
        """
        Execute a single-item delete and store the result in session state
        so the UI can show feedback after rerun.
        """
        handler = get_content_handler(content_type)
        result = handler.delete_content(content_id, cleanup_links=True)

        if result["deleted"]:
            msg = f"🗑️ Deleted content ID {content_id}"
            if result["link_ids_cleaned"]:
                msg += f" (also cleaned {len(result['link_ids_cleaned'])} link reference(s))"
            st.session_state[f"delete_feedback_{invalidate_key}"] = ("success", msg)
        else:
            st.session_state[f"delete_feedback_{invalidate_key}"] = (
                "error", f"❌ Delete failed: {result.get('error', 'unknown error')}"
            )

        # Invalidate cached table data so it reloads
        filter_prefix = f"{content_type}_filter"
        if f"{filter_prefix}_data" in st.session_state:
            del st.session_state[f"{filter_prefix}_data"]

    def _delete_by_name(self, content_type: str, content_name: str,
                        account_id: int = None, invalidate_key: str = ""):
        """
        Execute a bulk delete for all items under content_name and store feedback.
        """
        handler = get_content_handler(content_type)
        result = handler.delete_content_by_name(content_name, account_id=account_id, cleanup_links=True)

        if result.get("error"):
            st.session_state[f"delete_feedback_{invalidate_key}"] = (
                "error", f"❌ Bulk delete failed: {result['error']}"
            )
        else:
            msg = (f"🗑️ Deleted **{result['deleted_count']}** items "
                   f"under '{content_name}'")
            if result["link_ids_cleaned"]:
                msg += f" (cleaned {len(result['link_ids_cleaned'])} link reference(s))"
            st.session_state[f"delete_feedback_{invalidate_key}"] = ("success", msg)

        filter_prefix = f"{content_type}_filter"
        if f"{filter_prefix}_data" in st.session_state:
            del st.session_state[f"{filter_prefix}_data"]

    def _show_delete_feedback(self, key: str):
        """Display any pending delete feedback stored in session state."""
        fb_key = f"delete_feedback_{key}"
        if fb_key in st.session_state:
            level, msg = st.session_state.pop(fb_key)
            if level == "success":
                st.success(msg)
            else:
                st.error(msg)

    # =========================================================================
    # CONTENT NAME BREAKDOWN (with bulk-delete per name)
    # =========================================================================

    def _render_content_name_breakdown(self, content_type: str, account_id: int = None):
        st.markdown("### 📊 Breakdown by Content Name")

        try:
            handler = get_content_handler(content_type)
            name_stats = handler.get_content_name_statistics(account_id)

            if not name_stats:
                st.info(f"No content found for **{content_type.replace('_', ' ').title()}**.")
                return

            all_data = handler.get_comprehensive_data(account_id=account_id, limit=None)

            name_link_map: dict = {}
            for item in all_data:
                cn = item.get("content_name") or "—"
                if cn not in name_link_map:
                    name_link_map[cn] = {"has_link": 0, "no_link": 0, "items": []}
                if item.get("has_link"):
                    name_link_map[cn]["has_link"] += 1
                else:
                    name_link_map[cn]["no_link"] += 1
                name_link_map[cn]["items"].append(item)

            # Summary table
            rows = []
            for name, stats in sorted(name_stats.items()):
                total = stats.get("total", 0)
                used  = stats.get("used",  0)
                unused = stats.get("unused", 0)
                usage_pct = round(used / total * 100, 1) if total else 0.0
                link_info = name_link_map.get(name, {})
                rows.append({
                    "Content Name": name,
                    "Total":        total,
                    "Used ✅":      used,
                    "Unused ❌":    unused,
                    "Usage %":      usage_pct,
                    "Has Link 🔗":  link_info.get("has_link", 0),
                    "No Link ⛔":   link_info.get("no_link", 0),
                })

            summary_df = pd.DataFrame(rows)

            def colour_usage(val):
                if val >= 75:
                    return "background-color: #1a5c2e; color: #7defa1"
                elif val >= 40:
                    return "background-color: #5c4a00; color: #ffd166"
                else:
                    return "background-color: #5c1a1a; color: #ff6b6b"

            styled = (
                summary_df.style
                .applymap(colour_usage, subset=["Usage %"])
                .format({"Usage %": "{:.1f}%"})
            )
            st.dataframe(styled, use_container_width=True, hide_index=True,
                         height=min(45 * len(rows) + 60, 520))

            # Footer metrics
            t_total  = summary_df["Total"].sum()
            t_used   = summary_df["Used ✅"].sum()
            t_unused = summary_df["Unused ❌"].sum()
            t_link   = summary_df["Has Link 🔗"].sum()
            t_nolink = summary_df["No Link ⛔"].sum()
            overall_pct = round(t_used / t_total * 100, 1) if t_total else 0.0

            fc = st.columns(6)
            fc[0].metric("Names",    len(rows))
            fc[1].metric("Total",    int(t_total))
            fc[2].metric("Used",     int(t_used))
            fc[3].metric("Unused",   int(t_unused))
            fc[4].metric("Has Link", int(t_link))
            fc[5].metric("Usage %",  f"{overall_pct}%")

            # Per-name drill-down with individual + bulk delete
            st.markdown("---")
            st.markdown("#### 🔎 Drill down by Content Name")

            for name in sorted(name_link_map.keys()):
                items = name_link_map[name]["items"]
                used_cnt   = sum(1 for i in items if i.get("used"))
                unused_cnt = len(items) - used_cnt
                link_cnt   = sum(1 for i in items if i.get("has_link"))

                label = (
                    f"**{name}** — "
                    f"Total: {len(items)} | "
                    f"Used: {used_cnt} | Unused: {unused_cnt} | Links: {link_cnt}"
                )

                with st.expander(label, expanded=False):
                    # ── Bulk delete for this content name ──────────────────
                    bulk_key = f"bulk_delete_{content_type}_{name}"
                    self._show_delete_feedback(bulk_key)

                    bulk_col1, bulk_col2, bulk_col3 = st.columns([3, 1, 1])
                    with bulk_col1:
                        st.markdown(
                            f"<span style='color:#ff6b6b'>⚠️ Bulk delete will remove "
                            f"**all {len(items)} items** under this content name "
                            + (f"for the selected account" if account_id else "across ALL accounts")
                            + ".</span>",
                            unsafe_allow_html=True,
                        )
                    with bulk_col2:
                        # Two-click confirmation: first press arms, second press fires
                        arm_key = f"arm_{bulk_key}"
                        if not st.session_state.get(arm_key, False):
                            if st.button(
                                "🗑️ Delete All",
                                key=f"btn_arm_{bulk_key}",
                                use_container_width=True,
                                help=f"Delete all {len(items)} items under '{name}'"
                            ):
                                st.session_state[arm_key] = True
                                st.rerun()
                        else:
                            if st.button(
                                "⚠️ Confirm Delete",
                                key=f"btn_confirm_{bulk_key}",
                                use_container_width=True,
                                type="primary",
                            ):
                                st.session_state[arm_key] = False
                                self._delete_by_name(
                                    content_type, name,
                                    account_id=account_id,
                                    invalidate_key=bulk_key
                                )
                                st.rerun()
                    with bulk_col3:
                        if st.session_state.get(arm_key, False):
                            if st.button("Cancel", key=f"btn_cancel_{bulk_key}",
                                         use_container_width=True):
                                st.session_state[arm_key] = False
                                st.rerun()

                    st.divider()
                    self._render_content_name_detail(name, items, content_type)

        except Exception as e:
            logger.error(f"Error in content name breakdown for {content_type}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            st.error(f"Error loading breakdown: {str(e)}")

    # =========================================================================
    # PER-NAME DETAIL (with per-row delete)
    # =========================================================================

    def _render_content_name_detail(self, content_name: str, items: list, content_type: str):
        """
        Render the individual rows for one content_name inside an expander.
        Includes a per-row 🗑️ delete button in the "All" and "Unused" sub-tabs.
        """
        if not items:
            st.info("No items.")
            return

        sub_all, sub_used, sub_unused, sub_link, sub_nolink = st.tabs([
            f"All ({len(items)})",
            f"Used ✅ ({sum(1 for i in items if i.get('used'))})",
            f"Unused ❌ ({sum(1 for i in items if not i.get('used'))})",
            f"Has Link 🔗 ({sum(1 for i in items if i.get('has_link'))})",
            f"No Link ⛔ ({sum(1 for i in items if not i.get('has_link'))})",
        ])

        def build_detail_df(subset):
            rows = []
            for item in subset:
                rows.append({
                    "ID":           item.get("content_id"),
                    "Account":      item.get("username", "—"),
                    "Used":         "✅" if item.get("used") else "❌",
                    "WF Status":    item.get("workflow_status", "—"),
                    "Workflow":     (
                        (item.get("workflow_name") or "")[:35] + "…"
                        if item.get("workflow_name") and len(item.get("workflow_name", "")) > 35
                        else item.get("workflow_name") or "—"
                    ),
                    "Has Link":     "🔗" if item.get("has_link") else "⛔",
                    "Link URL":     (
                        (item.get("link_url") or "")[:55] + "…"
                        if item.get("link_url") and len(item.get("link_url", "")) > 55
                        else item.get("link_url") or "—"
                    ),
                    "Shared Links": len(item.get("shared_link_ids") or []),
                    "Created":      (
                        item["created_time"].strftime("%Y-%m-%d %H:%M")
                        if item.get("created_time") and hasattr(item["created_time"], "strftime")
                        else str(item.get("created_time") or "—")
                    ),
                    "Content":      (
                        (str(item.get("content") or ""))[:90] + "…"
                        if item.get("content") and len(str(item.get("content", ""))) > 90
                        else str(item.get("content") or "")
                    ),
                })
            return pd.DataFrame(rows)

        def render_with_delete(subset, tab_key_suffix: str):
            """Render a dataframe + per-row delete buttons below it."""
            if not subset:
                st.info("No items.")
                return

            st.dataframe(build_detail_df(subset), use_container_width=True, hide_index=True)

            st.markdown("**Delete individual items:**")
            # Render delete buttons in rows of 4
            cols_per_row = 4
            for chunk_start in range(0, len(subset), cols_per_row):
                chunk = subset[chunk_start: chunk_start + cols_per_row]
                cols = st.columns(len(chunk))
                for col, item in zip(cols, chunk):
                    cid = item.get("content_id")
                    row_key = f"del_{content_type}_{cid}_{tab_key_suffix}"
                    arm_key = f"arm_{row_key}"
                    with col:
                        self._show_delete_feedback(row_key)
                        if not st.session_state.get(arm_key, False):
                            if st.button(
                                f"🗑️ #{cid}",
                                key=f"btn_arm_{row_key}",
                                use_container_width=True,
                                help=f"Delete content ID {cid}",
                            ):
                                st.session_state[arm_key] = True
                                st.rerun()
                        else:
                            if st.button(
                                f"⚠️ Confirm #{cid}",
                                key=f"btn_confirm_{row_key}",
                                use_container_width=True,
                                type="primary",
                            ):
                                st.session_state[arm_key] = False
                                self._delete_single_content(content_type, cid, row_key)
                                st.rerun()
                            if st.button(
                                "Cancel",
                                key=f"btn_cancel_{row_key}",
                                use_container_width=True,
                            ):
                                st.session_state[arm_key] = False
                                st.rerun()

        with sub_all:
            render_with_delete(items, "all")

        with sub_used:
            subset = [i for i in items if i.get("used")]
            # Used items — show table only (deleting used content is riskier;
            # users can still delete from the "All" tab if needed)
            st.dataframe(build_detail_df(subset) if subset else pd.DataFrame(),
                         use_container_width=True, hide_index=True)

        with sub_unused:
            subset = [i for i in items if not i.get("used")]
            render_with_delete(subset, "unused")

        with sub_link:
            subset = [i for i in items if i.get("has_link")]
            if subset:
                st.dataframe(build_detail_df(subset), use_container_width=True, hide_index=True)
                st.markdown("**Link URLs in this group:**")
                seen = set()
                for item in subset:
                    url = item.get("link_url")
                    if url and url not in seen:
                        seen.add(url)
                        st.markdown(f"- 🔗 [{url[:80]}]({url})")
            else:
                st.info("No items with links.")

        with sub_nolink:
            subset = [i for i in items if not i.get("has_link")]
            st.dataframe(build_detail_df(subset) if subset else pd.DataFrame(),
                         use_container_width=True, hide_index=True)

    # =========================================================================
    # CONTENT TABLE (drill-down)
    # =========================================================================

    def _render_content_table(self, content_type: str, account_id: int = None):
        st.markdown("### 📋 Content Data")

        filter_prefix = f"{content_type}_filter"

        used_filter = st.session_state.get(f"{filter_prefix}_applied_used", "All")
        content_name_filter = st.session_state.get(f"{filter_prefix}_applied_content_name", "All")
        created_date = st.session_state.get(f"{filter_prefix}_applied_date")
        limit = st.session_state.get(f"{filter_prefix}_limit", 100)

        used = None if used_filter == 'All' else (used_filter == 'Yes')
        content_name = None if content_name_filter == 'All' else content_name_filter

        auto_load = st.session_state.get(f"{filter_prefix}_auto_load", False)
        data_exists = f"{filter_prefix}_data" in st.session_state
        should_load = auto_load or not data_exists

        if should_load:
            if f"{filter_prefix}_auto_load" in st.session_state:
                del st.session_state[f"{filter_prefix}_auto_load"]

            with st.spinner(f"Loading {content_type} data..."):
                content_data = self._fetch_filtered_content(
                    content_type=content_type,
                    account_id=account_id,
                    used=used,
                    content_name=content_name,
                    created_date=created_date,
                    limit=limit
                )
                st.session_state[f"{filter_prefix}_data"] = content_data

        content_data = st.session_state.get(f"{filter_prefix}_data", [])

        if content_data:
            df = pd.DataFrame(content_data)
            display_df = self._simplify_content_df(df)
            st.dataframe(display_df, use_container_width=True, hide_index=True, height=500)

            csv = display_df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"{content_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key=f"{filter_prefix}_download"
            )

            stats = self._get_content_stats(content_type, account_id)
            total_in_db = stats.get('total', 0)
            st.caption(f"📊 Showing {len(df)} of {total_in_db} total content items")

            used_count = len([item for item in content_data if item.get('used')])
            with_workflow = len([item for item in content_data if item.get('automa_workflow_id')])
            st.caption(f"✅ Used: {used_count} | 🔄 With Workflow: {with_workflow}")
        else:
            st.warning("⚠️ No content found matching the current filters.")
            st.info("💡 **Tip**: Data loads automatically. If empty, create content using the DAG or check filters.")

            if st.checkbox("🔧 Show Debug Info", key=f"{filter_prefix}_debug"):
                st.write(f"**Content Type:** {content_type}")
                st.write(f"**Account ID:** {account_id}")
                st.write(f"**Filters:** Used={used}, Name={content_name}, Date={created_date}, Limit={limit}")
                try:
                    handler = get_content_handler(content_type)
                    all_data = handler.get_comprehensive_data(account_id=account_id, limit=5)
                    st.write(f"**Sample data from database:** {len(all_data)} items found")
                    if all_data:
                        st.json(all_data[0])
                except Exception as e:
                    st.error(f"Error fetching sample data: {e}")

    # =========================================================================
    # CONTENT FILTERS
    # =========================================================================

    def _render_content_filters(self, content_type: str, account_id: int = None):
        st.markdown("### 🔍 Filters")

        filter_prefix = f"{content_type}_filter"
        col1, col2, col3, col4, _ = st.columns(5)

        with col1:
            used_filter = st.selectbox("Used:", ["All", "Yes", "No"], key=f"{filter_prefix}_used")

        with col2:
            content_name_options = self._get_content_name_options(content_type, account_id)
            content_name_filter = st.selectbox("Content Name:", content_name_options,
                                               key=f"{filter_prefix}_content_name")

        with col3:
            created_date = st.date_input("Created Date:", value=None,
                                         key=f"{filter_prefix}_created_date")

        with col4:
            limit = st.number_input("Show rows:", min_value=10, max_value=1000,
                                    value=100, step=10, key=f"{filter_prefix}_limit")

        col_btn1, col_btn2, col_spacer = st.columns([1, 1, 3])

        with col_btn1:
            if st.button("🔍 Apply Filters", key=f"{filter_prefix}_apply",
                         type="primary", use_container_width=True):
                st.session_state[f"{filter_prefix}_applied_used"] = used_filter
                st.session_state[f"{filter_prefix}_applied_content_name"] = content_name_filter
                st.session_state[f"{filter_prefix}_applied_date"] = created_date
                st.session_state[f"{filter_prefix}_applied_limit"] = limit
                st.session_state[f"{filter_prefix}_data"] = None
                st.session_state[f"{filter_prefix}_auto_load"] = True
                st.rerun()

        with col_btn2:
            if st.button("🔄 Clear Filters", key=f"{filter_prefix}_clear",
                         use_container_width=True):
                keys_to_clear = [k for k in st.session_state.keys() if k.startswith(filter_prefix)]
                for key in keys_to_clear:
                    del st.session_state[key]
                st.rerun()

        active_filters = []
        if used_filter != "All":
            active_filters.append(f"Used: {used_filter}")
        if content_name_filter != "All":
            active_filters.append(f"Name: {content_name_filter}")
        if created_date:
            active_filters.append(f"Date: {created_date}")
        if active_filters:
            st.info(f"**Active Filters:** {' | '.join(active_filters)}")

    # =========================================================================
    # STATS BAR
    # =========================================================================

    def _render_content_stats(self, content_type: str, account_id: int = None):
        st.markdown("### 📊 Content Statistics")
        try:
            account_filter = "AND c.account_id = %s" if account_id else ""
            params = [content_type]
            if account_id:
                params.append(account_id)

            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(
                        f"SELECT COUNT(*) as count FROM content c WHERE c.content_type = %s {account_filter}",
                        params
                    )
                    total = cursor.fetchone()['count']

                    cursor.execute(
                        f"SELECT COUNT(*) as count FROM content c WHERE c.content_type = %s AND c.used = TRUE {account_filter}",
                        params
                    )
                    used = cursor.fetchone()['count']

                    cursor.execute(
                        f"SELECT COUNT(*) as count FROM content c WHERE c.content_type = %s AND c.automa_workflow_id IS NOT NULL {account_filter}",
                        params
                    )
                    with_workflow = cursor.fetchone()['count']

                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Total Content", total)
                    col2.metric("Used", used)
                    col3.metric("Unused", total - used)
                    col4.metric("With Workflow", with_workflow)

        except Exception as e:
            logger.error(f"Error rendering stats for {content_type}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            st.error(f"Error loading stats: {str(e)}")

    # =========================================================================
    # CONNECTIONS VIEW
    # =========================================================================

    def _render_content_connections_view(self, content_type, selected_account_id):
        st.markdown("### 🔗 Content-Workflow-Link Connections")
        try:
            handler = get_content_handler(content_type)
            all_data = handler.get_comprehensive_data(account_id=selected_account_id)

            if not all_data:
                st.info(f"No {content_type} data found for the selected account")
                return

            section1, section2, section3 = st.tabs([
                "🔗 Grouped by Links",
                "🔄 Grouped by Workflows",
                "📊 Connection Statistics"
            ])

            with section1:
                self._render_content_grouped_by_links(content_type, all_data)

            with section2:
                self._render_content_grouped_by_workflows(content_type, all_data)

            with section3:
                self._render_connection_statistics(content_type, all_data)

        except Exception as e:
            logger.error(f"Error rendering content connections view for {content_type}: {e}")
            st.error(f"Error loading connections: {str(e)}")

    # =========================================================================
    # CONTENT STATISTICS (per-type stats page)
    # =========================================================================

    def _render_content_statistics(self, content_type: str, account_id: int = None):
        try:
            handler = get_content_handler(content_type)
            stats = handler.get_stats()
            account_stats = {}
            if account_id:
                account_stats = handler.get_account_statistics(account_id)

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                total = stats.get(f'total_{content_type}', 0)
                st.metric("Total Content", total)
            with col2:
                used = stats.get(f'used_{content_type}', 0)
                st.metric("Used Content", used)
            with col3:
                unused = stats.get(f'unused_{content_type}', 0)
                st.metric("Unused Content", unused)
            with col4:
                usage_rate = (used / total * 100) if total > 0 else 0
                st.metric("Usage Rate", f"{usage_rate:.1f}%")

            st.divider()
            st.markdown("#### 📊 Content Name Distribution")
            content_name_stats = handler.get_content_name_statistics(account_id)

            if content_name_stats:
                stats_data = []
                for name, name_stats in content_name_stats.items():
                    stats_data.append({
                        'Content Name': name,
                        'Total': name_stats.get('total', 0),
                        'Used': name_stats.get('used', 0),
                        'Unused': name_stats.get('unused', 0),
                        'Usage %': f"{(name_stats.get('used', 0) / name_stats.get('total', 1) * 100):.1f}%"
                    })
                stats_df = pd.DataFrame(stats_data)
                st.dataframe(stats_df, use_container_width=True, hide_index=True)
                if len(stats_df) > 0:
                    st.bar_chart(stats_df[['Content Name', 'Total']].head(10).set_index('Content Name'))
            else:
                st.info("No content name statistics available")

            st.divider()
            st.markdown("#### 🔄 Workflow Status")
            if account_stats and 'workflow_status_breakdown' in account_stats:
                workflow_data = [
                    {'Status': s, 'Count': c}
                    for s, c in account_stats['workflow_status_breakdown'].items()
                ]
                if workflow_data:
                    st.dataframe(pd.DataFrame(workflow_data), use_container_width=True, hide_index=True)
            else:
                st.info("No workflow status data available")

            st.divider()
            st.markdown("#### 📅 Recent Activity (Last 7 Days)")
            if account_stats and 'recent_activity' in account_stats:
                activity_data = account_stats['recent_activity']
                if activity_data:
                    activity_df = pd.DataFrame(activity_data)
                    if 'date' in activity_df.columns and 'count' in activity_df.columns:
                        st.line_chart(activity_df.set_index('date')['count'])
                    else:
                        st.info("No recent activity data available")
                else:
                    st.info("No recent activity data available")
            else:
                st.info("No recent activity data available")

        except Exception as e:
            logger.error(f"Error rendering statistics for {content_type}: {e}")
            st.error(f"Error loading statistics: {str(e)}")

    # =========================================================================
    # ACCOUNT SELECTOR
    # =========================================================================

    def _render_account_selector(self):
        try:
            accounts = pg_utils.get_all_accounts()
            if accounts:
                account_options = ["All Accounts"] + [
                    f"{acc['username']} ({acc['profile_id'][:8]}...)"
                    for acc in accounts
                ]
                account_map = {
                    f"{acc['username']} ({acc['profile_id'][:8]}...)": acc['account_id']
                    for acc in accounts
                }

                selected = st.selectbox(
                    "Filter by Account:",
                    account_options,
                    key="account_selector",
                    help="Select an account to filter content"
                )

                if selected == "All Accounts":
                    st.session_state['selected_account_id'] = None
                else:
                    st.session_state['selected_account_id'] = account_map.get(selected)

                if st.session_state.get('selected_account_id'):
                    try:
                        account_info = pg_utils.get_account_by_id(st.session_state['selected_account_id'])
                        if account_info:
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.info(f"**Username:** {account_info['username']}")
                            with col2:
                                st.info(f"**Profile:** {account_info['profile_id'][:12]}...")
                            with col3:
                                created = account_info['created_time']
                                created_date = created.strftime('%Y-%m-%d') if created else 'N/A'
                                st.info(f"**Created:** {created_date}")
                    except Exception as e:
                        logger.error(f"Error fetching account info: {e}")
            else:
                st.warning("No accounts found in the database.")
        except Exception as e:
            logger.error(f"Error loading accounts: {e}")
            st.error(f"Error loading accounts: {str(e)}")

    # =========================================================================
    # ANALYTICS
    # =========================================================================

    def _render_analytics_section(self, account_id: int, prompt_types: list):
        st.subheader("📊 Analytics Dashboard")
        try:
            if account_id:
                self._render_account_analytics(account_id, prompt_types)
            else:
                self._render_system_analytics(prompt_types)
        except Exception as e:
            logger.error(f"Error rendering analytics: {e}")
            st.error(f"Error loading analytics: {str(e)}")

    def _render_account_analytics(self, account_id: int, prompt_types: list):
        try:
            account_info = pg_utils.get_account_by_id(account_id)
            st.subheader(f"Account: {account_info['username']}")

            total_content = 0
            used_content = 0
            content_breakdown = {}

            for ptype in prompt_types:
                handler = get_content_handler(ptype)
                stats = handler.get_account_statistics(account_id)
                type_total = stats.get(f'total_{ptype}', 0)
                type_used = stats.get(f'used_{ptype}', 0)
                total_content += type_total
                used_content += type_used
                content_breakdown[ptype] = {'total': type_total, 'used': type_used, 'unused': type_total - type_used}

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Content", total_content)
            col2.metric("Used Content", used_content)
            col3.metric("Unused Content", total_content - used_content)
            col4.metric("Usage Rate", f"{(used_content / total_content * 100) if total_content else 0:.1f}%")
            st.divider()

            st.subheader("Content by Type")
            breakdown_data = []
            for ptype in prompt_types:
                b = content_breakdown[ptype]
                breakdown_data.append({
                    'Type': ptype.replace('_', ' ').title(),
                    'Total': b['total'], 'Used': b['used'], 'Unused': b['unused'],
                    'Usage %': f"{(b['used']/b['total']*100) if b['total'] else 0:.1f}%"
                })
            if breakdown_data:
                st.dataframe(pd.DataFrame(breakdown_data), use_container_width=True)
            chart_df = pd.DataFrame([
                {'Type': ptype.replace('_', ' ').title(), 'Count': content_breakdown[ptype]['total']}
                for ptype in prompt_types
            ])
            if not chart_df.empty:
                st.bar_chart(data=chart_df.set_index('Type'))
        except Exception as e:
            logger.error(f"Error rendering account analytics: {e}")
            st.error(f"Error: {str(e)}")

    def _render_system_analytics(self, prompt_types: list):
        try:
            st.subheader("System Overview")
            total_content = 0
            used_content = 0
            content_breakdown = {}

            for ptype in prompt_types:
                handler = get_content_handler(ptype)
                stats = handler.get_stats()
                type_total = stats.get(f'total_{ptype}', 0)
                type_used = stats.get(f'used_{ptype}', 0)
                total_content += type_total
                used_content += type_used
                content_breakdown[ptype] = {'total': type_total, 'used': type_used, 'unused': type_total - type_used}

            accounts = pg_utils.get_all_accounts()
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Accounts", len(accounts))
            col2.metric("Total Content", total_content)
            col3.metric("Used Content", used_content)
            col4.metric("Usage Rate", f"{(used_content / total_content * 100) if total_content else 0:.1f}%")
            st.divider()

            st.subheader("Content Distribution")
            breakdown_data = []
            for ptype in prompt_types:
                b = content_breakdown[ptype]
                breakdown_data.append({
                    'Content Type': ptype.replace('_', ' ').title(),
                    'Total': b['total'], 'Used': b['used'], 'Unused': b['unused'],
                    'Usage %': f"{(b['used']/b['total']*100) if b['total'] else 0:.1f}%"
                })
            if breakdown_data:
                st.dataframe(pd.DataFrame(breakdown_data), use_container_width=True)

            col1, col2 = st.columns(2)
            with col1:
                st.write("**Total Content by Type**")
                chart_df = pd.DataFrame([
                    {'Type': ptype.replace('_', ' ').title(), 'Count': content_breakdown[ptype]['total']}
                    for ptype in prompt_types
                ])
                if not chart_df.empty:
                    st.bar_chart(data=chart_df.set_index('Type'))
            with col2:
                st.write("**Usage Status**")
                status_df = pd.DataFrame([
                    {'Status': 'Used', 'Count': used_content},
                    {'Status': 'Unused', 'Count': total_content - used_content}
                ])
                if not status_df.empty:
                    st.bar_chart(data=status_df.set_index('Status'))
        except Exception as e:
            logger.error(f"Error rendering system analytics: {e}")
            st.error(f"Error: {str(e)}")

    # =========================================================================
    # GROUPED VIEWS (links / workflows / statistics)
    # =========================================================================

    def _render_content_grouped_by_links(self, content_type, all_data):
        st.markdown("#### 🔗 Content Grouped by Shared Links")
        content_with_links = [
            item for item in all_data
            if item.get('has_link') and (
                item.get('primary_link_url') or
                (item.get('shared_link_urls') and len(item.get('shared_link_urls', [])) > 0)
            )
        ]

        if not content_with_links:
            st.info(f"ℹ️ No {content_type} content items have link connections yet.")
            return

        link_groups = {}
        for item in content_with_links:
            link_urls = []
            if item.get('primary_link_url'):
                link_urls.append(item['primary_link_url'])
            if item.get('shared_link_urls'):
                link_urls.extend(item['shared_link_urls'])
            for link_url in link_urls:
                if link_url and link_url != '-':
                    if link_url not in link_groups:
                        link_groups[link_url] = {'content_items': [], 'total_content': 0, 'link_data': {}}
                    link_groups[link_url]['content_items'].append(item)
                    link_groups[link_url]['total_content'] += 1
                    if not link_groups[link_url]['link_data']:
                        link_groups[link_url]['link_data'] = {
                            'link_id': item.get('primary_link_id') or (item.get('shared_link_ids')[0] if item.get('shared_link_ids') else None),
                            'tweet_id': item.get('primary_tweet_id') or (item.get('shared_tweet_ids')[0] if item.get('shared_tweet_ids') else None),
                            'tweeted_time': item.get('primary_tweeted_time'),
                            'link_account': item.get('primary_link_account_username') or (item.get('shared_link_accounts')[0] if item.get('shared_link_accounts') else None),
                            'connected_via_workflow': item.get('primary_link_workflow')
                        }

        shared_links = {url: data for url, data in link_groups.items() if data['total_content'] > 1}

        if not shared_links:
            st.info(f"ℹ️ No links found with multiple {content_type} content items.")
            single_content_links = {url: data for url, data in link_groups.items() if data['total_content'] == 1}
            if single_content_links:
                st.info(f"Found {len(single_content_links)} links with single content items:")
                summary_data = []
                for link_url, link_data in single_content_links.items():
                    item = link_data['content_items'][0]
                    summary_data.append({
                        'Link URL': link_url[:80] + '...' if len(link_url) > 80 else link_url,
                        'Content ID': item.get('content_id'), 'Content Name': item.get('content_name'),
                        'Account': item.get('username'),
                        'Workflow': (item.get('workflow_name', 'N/A') or 'N/A')[:40],
                        'Link Status': '✅ Active' if item.get('link_connection_status') == 'active' else '⚠️ Other'
                    })
                st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)
            return

        st.success(f"✅ Found {len(shared_links)} links with multiple {content_type} content items")
        summary_data = []
        for link_url, link_data in sorted(shared_links.items(), key=lambda x: x[1]['total_content'], reverse=True):
            lm = link_data['link_data']
            summary_data.append({
                'Link URL': link_url[:80] + '...' if len(link_url) > 80 else link_url,
                'Total Content': link_data['total_content'],
                'Content IDs': ', '.join([str(i.get('content_id')) for i in link_data['content_items'][:3]]) + ('...' if link_data['total_content'] > 3 else ''),
                'Link ID': lm.get('link_id', 'N/A'), 'Tweet ID': lm.get('tweet_id', 'N/A'),
                'Tweet Account': lm.get('link_account', 'N/A'),
                'Connected Via': (lm.get('connected_via_workflow') or 'N/A')[:40]
            })
        st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("#### 📋 Detailed View of Shared Links")
        for link_url, link_data in sorted(shared_links.items(), key=lambda x: x[1]['total_content'], reverse=True):
            with st.expander(f"🔗 Link: {link_url[:60]}... | {link_data['total_content']} content items", expanded=False):
                lm = link_data['link_data']
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Link URL:** [{link_url[:100]}...]({link_url})")
                    if lm.get('link_id'):
                        st.write(f"**Link ID:** {lm['link_id']}")
                with col2:
                    if lm.get('link_account'):
                        st.write(f"**Account:** {lm['link_account']}")
                    if lm.get('tweeted_time'):
                        st.write(f"**Tweeted:** {lm['tweeted_time']}")

                content_data = []
                for item in link_data['content_items']:
                    content_data.append({
                        'Content ID': item.get('content_id'), 'Content Name': item.get('content_name'),
                        'Account': item.get('username'),
                        'Content Preview': (item.get('content', '')[:80] + '...') if item.get('content') and len(item.get('content')) > 80 else item.get('content', ''),
                        'Workflow Name': item.get('workflow_name', 'N/A'),
                        'Used': '✅' if item.get('used') else '❌',
                        'Created': item.get('created_time', 'N/A')
                    })
                st.dataframe(pd.DataFrame(content_data), use_container_width=True, hide_index=True)

                col_stat1, col_stat2, col_stat3 = st.columns(3)
                with col_stat1:
                    st.metric("Used Content", sum(1 for i in link_data['content_items'] if i.get('used')))
                with col_stat2:
                    st.metric("Unique Workflows", len(set(i.get('automa_workflow_id') for i in link_data['content_items'] if i.get('automa_workflow_id'))))
                with col_stat3:
                    st.metric("Unique Accounts", len(set(i.get('username') for i in link_data['content_items'] if i.get('username'))))

    def _render_content_grouped_by_workflows(self, content_type, all_data):
        st.markdown("#### 🔄 Content Grouped by Shared Workflows")
        workflow_to_content = {}
        for item in all_data:
            wid = item.get('automa_workflow_id')
            if wid:
                workflow_to_content.setdefault(wid, []).append(item)

        shared_workflows = {wid: items for wid, items in workflow_to_content.items() if len(items) > 1}

        if not shared_workflows:
            st.info(f"ℹ️ No workflows found with multiple {content_type} content items")
            single = {wid: items for wid, items in workflow_to_content.items() if len(items) == 1}
            if single:
                summary_data = []
                for wid, content_list in single.items():
                    item = content_list[0]
                    summary_data.append({
                        'Workflow ID': str(wid)[:20] + '...',
                        'Content ID': item.get('content_id'), 'Content Name': item.get('content_name'),
                        'Account': item.get('username'),
                        'Has Link': '✅' if item.get('has_link') else '❌',
                        'Link URL': (item.get('link_url', '')[:50] + '...') if item.get('link_url') and len(item.get('link_url')) > 50 else item.get('link_url', 'None')
                    })
                if summary_data:
                    st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)
            return

        st.success(f"✅ Found {len(shared_workflows)} workflows with multiple {content_type} content items")
        summary_data = []
        for wid, content_list in sorted(shared_workflows.items(), key=lambda x: len(x[1]), reverse=True):
            link_info = next((i['link_url'] for i in content_list if i.get('link_url')), None)
            accounts = set(i.get('username') for i in content_list if i.get('username'))
            summary_data.append({
                'Workflow ID': str(wid)[:20] + '...',
                'Content Items': len(content_list),
                'Content IDs': ', '.join([str(i.get('content_id')) for i in content_list[:3]]) + ('...' if len(content_list) > 3 else ''),
                'Accounts': ', '.join(list(accounts)[:2]) + ('...' if len(accounts) > 2 else ''),
                'Link URL': (link_info[:50] + '...') if link_info and len(link_info) > 50 else link_info or 'None',
                'Has Link': '✅' if link_info else '❌'
            })
        st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("#### 📋 Detailed View of Shared Workflows")
        for wid, content_list in sorted(shared_workflows.items(), key=lambda x: len(x[1]), reverse=True):
            with st.expander(f"🔄 Workflow: {str(wid)[:20]}... | {len(content_list)} content items", expanded=False):
                content_data = []
                for item in content_list:
                    content_data.append({
                        'Content ID': item.get('content_id'), 'Content Name': item.get('content_name'),
                        'Account': item.get('username'),
                        'Content Preview': (item.get('content', '')[:80] + '...') if item.get('content') and len(item.get('content')) > 80 else item.get('content', ''),
                        'Used': '✅' if item.get('used') else '❌',
                        'Has Link': '✅' if item.get('has_link') else '❌',
                        'Link URL': (item.get('link_url', '')[:50] + '...') if item.get('link_url') and len(item.get('link_url')) > 50 else item.get('link_url', 'None'),
                        'Created': item.get('created_time', 'N/A')
                    })
                st.dataframe(pd.DataFrame(content_data), use_container_width=True, hide_index=True)
                col1, col2, col3 = st.columns(3)
                col1.metric("Used Content", sum(1 for i in content_list if i.get('used')))
                col2.metric("Linked Content", sum(1 for i in content_list if i.get('has_link')))
                col3.metric("Unique Accounts", len(set(i.get('username') for i in content_list if i.get('username'))))

    def _render_connection_statistics(self, content_type, all_data):
        st.markdown("#### 📊 Connection Statistics")
        total_content = len(all_data)
        content_with_links = [i for i in all_data if i.get('has_link')]
        content_without_links = [i for i in all_data if not i.get('has_link')]
        content_with_workflows = [i for i in all_data if i.get('automa_workflow_id')]
        content_without_workflows = [i for i in all_data if not i.get('automa_workflow_id')]
        content_used = [i for i in all_data if i.get('used')]

        link_groups = {}
        for item in content_with_links:
            for url in ([item.get('primary_link_url')] + (item.get('shared_link_urls') or [])):
                if url and url != '-':
                    link_groups.setdefault(url, []).append(item)

        shared_links = {u: v for u, v in link_groups.items() if len(v) > 1}
        single_content_links = {u: v for u, v in link_groups.items() if len(v) == 1}

        workflow_groups = {}
        for item in all_data:
            wid = item.get('automa_workflow_id')
            if wid:
                workflow_groups.setdefault(wid, []).append(item)

        shared_workflows = {wid: v for wid, v in workflow_groups.items() if len(v) > 1}
        single_content_workflows = {wid: v for wid, v in workflow_groups.items() if len(v) == 1}

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Content", total_content)
        col2.metric("With Links", len(content_with_links))
        col3.metric("With Workflows", len(content_with_workflows))
        col4.metric("Used Content", len(content_used))

        col5, col6, col7, col8 = st.columns(4)
        col5.metric("Shared Links", len(shared_links))
        col6.metric("Single Content Links", len(single_content_links))
        col7.metric("Shared Workflows", len(shared_workflows))
        col8.metric("Single Content Workflows", len(single_content_workflows))

        st.divider()
        col_chart1, col_chart2 = st.columns(2)
        with col_chart1:
            st.markdown("**Link Distribution**")
            st.bar_chart(pd.DataFrame({'Type': ['Shared Links', 'Single Links', 'No Links'],
                                       'Count': [len(shared_links), len(single_content_links), len(content_without_links)]}).set_index('Type'))
        with col_chart2:
            st.markdown("**Workflow Distribution**")
            st.bar_chart(pd.DataFrame({'Type': ['Shared Workflows', 'Single Workflows', 'No Workflows'],
                                       'Count': [len(shared_workflows), len(single_content_workflows), len(content_without_workflows)]}).set_index('Type'))

        st.divider()
        st.markdown("##### 🔝 Top Links by Content Count")
        if shared_links:
            top_links = [
                {'Link URL': u[:80], 'Content Count': len(v),
                 'Content IDs': ', '.join([str(i.get('content_id')) for i in v[:3]]) + ('...' if len(v) > 3 else ''),
                 'Used Count': sum(1 for i in v if i.get('used')),
                 'Workflow Count': len(set(i.get('automa_workflow_id') for i in v if i.get('automa_workflow_id')))}
                for u, v in sorted(shared_links.items(), key=lambda x: len(x[1]), reverse=True)[:10]
            ]
            st.dataframe(pd.DataFrame(top_links), use_container_width=True, hide_index=True)
        else:
            st.info("No shared links found")

    # =========================================================================
    # UTILITY / FETCH HELPERS
    # =========================================================================

    def _get_content_stats(self, content_type: str, account_id: int = None):
        try:
            account_filter = "AND c.account_id = %s" if account_id else ""
            params = [content_type]
            if account_id:
                params.append(account_id)
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(f"SELECT COUNT(*) as count FROM content c WHERE c.content_type = %s {account_filter}", params)
                    total = cursor.fetchone()['count']
                    cursor.execute(f"SELECT COUNT(*) as count FROM content c WHERE c.content_type = %s AND c.used = TRUE {account_filter}", params)
                    used = cursor.fetchone()['count']
                    cursor.execute(f"SELECT COUNT(*) as count FROM content c WHERE c.content_type = %s AND c.automa_workflow_id IS NOT NULL {account_filter}", params)
                    with_workflow = cursor.fetchone()['count']
                    return {'total': total, 'used': used, 'unused': total - used, 'with_workflow': with_workflow}
        except Exception as e:
            logger.error(f"Error getting stats for {content_type}: {e}")
            return {'total': 0, 'used': 0, 'unused': 0, 'with_workflow': 0}

    def _get_content_name_options(self, content_type: str, account_id: int = None):
        try:
            handler = get_content_handler(content_type)
            content_names = handler.get_all_content_names(account_id=account_id)
            return ["All"] + sorted(content_names)
        except Exception as e:
            logger.error(f"Error getting content names: {e}")
            return ["All"]

    def _fetch_filtered_content(self, content_type, account_id=None, used=None,
                                content_name=None, created_date=None, limit=100):
        try:
            handler = get_content_handler(content_type)
            all_data = handler.get_comprehensive_data(account_id=account_id, limit=None)
            filtered = all_data

            if used is not None:
                filtered = [i for i in filtered if i.get('used') == used]
            if content_name is not None:
                filtered = [i for i in filtered if i.get('content_name') == content_name]
            if created_date is not None:
                filtered = [i for i in filtered if i.get('created_time') and i['created_time'].date() == created_date]
            if limit:
                filtered = filtered[:limit]

            return filtered
        except Exception as e:
            logger.error(f"Error fetching filtered content: {e}")
            return []

    def _simplify_content_df(self, df):
        display_df = pd.DataFrame()

        if 'content_id' in df.columns:
            display_df['ID'] = df['content_id']
        if 'content_name' in df.columns:
            display_df['Content Name'] = df['content_name']
        if 'username' in df.columns:
            display_df['Account'] = df['username']
        if 'prompt_name' in df.columns:
            display_df['Prompt'] = df['prompt_name']
        if 'content' in df.columns:
            display_df['Content'] = df['content'].apply(
                lambda x: (str(x)[:100] + '...') if x and len(str(x)) > 100 else str(x) if x else ''
            )
        if 'used' in df.columns:
            display_df['Used'] = df['used'].apply(lambda x: '✅' if x else '❌')
        if 'workflow_status' in df.columns:
            display_df['WF Status'] = df['workflow_status']
        if 'automa_workflow_id' in df.columns:
            display_df['WF ID'] = df['automa_workflow_id'].apply(
                lambda x: str(x)[:12] + '...' if pd.notna(x) and x else '-'
            )
        if 'workflow_name' in df.columns:
            display_df['Workflow Name'] = df['workflow_name'].apply(
                lambda x: (str(x)[:40] + '...') if x and len(str(x)) > 40 else (str(x) if x else '-')
            )
        if 'shared_link_ids' in df.columns:
            display_df['Shared Links'] = df['shared_link_ids'].apply(
                lambda x: len(x) if isinstance(x, (list, tuple)) and x else 0
            )
        if 'has_link' in df.columns:
            def format_has_link(x):
                if isinstance(x, (list, tuple)):
                    return '✅' if len(x) > 0 else '❌'
                return '✅' if x else '❌'
            display_df['Has Link'] = df['has_link'].apply(format_has_link)

        if 'created_time' in df.columns:
            display_df['Created'] = df['created_time'].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) and hasattr(x, 'strftime') else str(x) if x else ''
            )
        if 'workflow_generated_time' in df.columns:
            display_df['WF Generated'] = df['workflow_generated_time'].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) and hasattr(x, 'strftime') else str(x) if x else ''
            )
        if 'used_time' in df.columns:
            display_df['Used Time'] = df['used_time'].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) and hasattr(x, 'strftime') else str(x) if x else ''
            )

        return display_df

    def _get_link_url_options(self, data: list, content_type: str) -> list:
        """
        Get list of unique link URLs for filter dropdown.
        Groups by link URL and shows count of content items sharing that link.
        """
        if not data:
            return ["All"]

        link_counts = {}
        for item in data:
            link_url = item.get('link_url')
            if link_url and link_url != '-' and pd.notna(link_url):
                link_counts[link_url] = link_counts.get(link_url, 0) + 1

        shared_links = {url: count for url, count in link_counts.items() if count > 1}

        if not shared_links:
            return ["All"]

        options = ["All", "--- Shared Links ---"]
        for url, count in sorted(shared_links.items(), key=lambda x: x[1], reverse=True):
            display_url = url[:60] + '...' if len(url) > 60 else url
            options.append(f"{display_url} ({count} items)")

        return options

    def _render_content_linked_to_same_link(self, content_type, all_data):
        """Render content items linked to the same link URL."""
        link_to_content = {}
        for item in all_data:
            link_url = item.get('link_url')
            if link_url and link_url != '-':
                link_to_content.setdefault(link_url, []).append(item)

        shared_links = {url: items for url, items in link_to_content.items() if len(items) > 1}

        if shared_links:
            st.success(f"✅ Found {len(shared_links)} links with multiple {content_type} content items")

            summary_data = []
            for link_url, content_list in shared_links.items():
                summary_data.append({
                    'Link URL': link_url[:80] + '...' if len(link_url) > 80 else link_url,
                    'Content Items': len(content_list),
                    'Content IDs': ', '.join([str(item.get('content_id')) for item in content_list[:3]]) +
                                   ('...' if len(content_list) > 3 else ''),
                    'Accounts': ', '.join(set([str(item.get('username', 'Unknown')) for item in content_list[:3]]))
                })

            st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)

            for link_url, content_list in shared_links.items():
                with st.expander(f"🔗 Link: {link_url[:60]}... | {len(content_list)} content items", expanded=False):
                    content_data = []
                    for item in content_list:
                        content_data.append({
                            'Content ID': item.get('content_id'),
                            'Content Name': item.get('content_name'),
                            'Account': item.get('username'),
                            'Content Preview': (item.get('content', '')[:100] + '...') if item.get('content') and len(item.get('content', '')) > 100 else item.get('content', ''),
                            'Workflow ID': str(item.get('automa_workflow_id', ''))[:20] + '...' if item.get('automa_workflow_id') else 'None',
                            'Link Status': item.get('link_connection_status', 'Unknown')
                        })
                    st.dataframe(pd.DataFrame(content_data), use_container_width=True, hide_index=True)
                    st.markdown(f"**Link URL:** [{link_url[:100]}...]({link_url})")
        else:
            st.info(f"ℹ️ No links found with multiple {content_type} content items")

    def _get_prompt_options(self, content_type: str):
        """Get unique prompts for this content type."""
        try:
            from src.core.database.postgres.connection import get_postgres_connection
            from psycopg2.extras import RealDictCursor
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT DISTINCT p.name
                        FROM prompts p
                        WHERE p.prompt_type = %s AND p.is_active = TRUE
                        ORDER BY p.name
                    """, (content_type,))
                    prompts = [row['name'] for row in cursor.fetchall()]
                    return ["All"] + prompts
        except Exception as e:
            logger.error(f"Error getting prompts: {e}")
            return ["All"]
