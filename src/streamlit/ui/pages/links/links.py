# File: pages/links_page.py
# UPDATED: tweet_author_user_id and chat_link columns added throughout

import streamlit as st
import pandas as pd
import logging
from ..base_page import BasePage
from src.core.database.postgres import links as pg_links
from src.core.database.postgres.connection import get_postgres_connection
from ...components.data_filters import DataFilters
from datetime import datetime, timedelta, timezone
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class LinksPage(BasePage):
    """Links page with links view, connections, analytics, bulk actions, and extraction state."""

    def render(self):
        st.header("🔗 Links Management")

        if 'show_reset_confirm' not in st.session_state:
            st.session_state.show_reset_confirm = False
        if 'show_delete_confirm' not in st.session_state:
            st.session_state.show_delete_confirm = False

        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "🔗 Links",
            "💬 Chat Links",
            "🔄 Connections",
            "📈 Analytics",
            "⚙️ Bulk Actions",
            "🔍 Extraction State",
        ])

        with tab1:
            self._render_links_view()
        with tab2:
            self._render_chat_links_view()
        with tab3:
            self._render_connections()
        with tab4:
            self._render_analytics()
        with tab5:
            self._render_bulk_actions()
        with tab6:
            self._render_extraction_state()

    # =========================================================================
    # TAB 1: LINKS VIEW
    # =========================================================================

    def _render_links_view(self):
        st.subheader("🔗 Links")
        st.info("""
        **What's this?** All tweet links extracted from Twitter/X.
        Includes the tweet author's numeric user ID and a direct chat link where available.
        """)

        self._render_links_stats()
        st.divider()
        self._render_links_filters()
        st.divider()
        self._render_links_table()

    def _render_links_filters(self):
        st.markdown("### 🔍 Filters")

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            within_limit_filter = st.selectbox("Within Timeframe:", ["All", "Yes", "No"],
                                               key="links_within_limit_filter")
        with col2:
            filtered_filter = st.selectbox("Filtered:", ["All", "Yes", "No"],
                                           key="links_filtered_filter")
        with col3:
            used_filter = st.selectbox("Used:", ["All", "Yes", "No"],
                                       key="links_used_filter")
        with col4:
            executed_filter = st.selectbox("Executed:", ["All", "Yes", "No"],
                                           key="links_executed_filter")
        with col5:
            success_filter = st.selectbox("Success:", ["All", "Yes", "No"],
                                          key="links_success_filter")

        col6, col7, col8, col9, col10 = st.columns(5)
        with col6:
            failure_filter = st.selectbox("Failure:", ["All", "Yes", "No"],
                                          key="links_failure_filter")
        with col7:
            has_chat_link_filter = st.selectbox(
                "Has Chat Link:", ["All", "Yes", "No"],
                key="links_has_chat_link_filter",
                help="Filter to links that have a resolved DM chat URL"
            )
        with col8:
            has_user_id_filter = st.selectbox(
                "Has User ID:", ["All", "Yes", "No"],
                key="links_has_user_id_filter",
                help="Filter to links where the author's numeric user ID was resolved"
            )
        with col9:
            limit = st.number_input("Show rows:", min_value=10, max_value=500,
                                    value=100, step=10, key="links_limit")

        col_date, col_btn1, col_btn2 = st.columns([2, 1, 1])
        with col_date:
            tweeted_date = st.date_input("Tweeted Date (optional):",
                                         value=None, key="links_tweeted_date")
        with col_btn1:
            st.write("")
            st.write("")
            if st.button("🔍 Apply Filters", key="apply_links_filters", type="primary"):
                for k, v in [
                    ('links_within_limit', within_limit_filter),
                    ('links_filtered',     filtered_filter),
                    ('links_used',         used_filter),
                    ('links_executed',     executed_filter),
                    ('links_success',      success_filter),
                    ('links_failure',      failure_filter),
                    ('links_has_chat',     has_chat_link_filter),
                    ('links_has_uid',      has_user_id_filter),
                    ('links_date',         tweeted_date),
                ]:
                    st.session_state[k] = v
                st.session_state.links_data       = None
                st.session_state.auto_load_links  = True
                st.rerun()
        with col_btn2:
            st.write("")
            st.write("")
            if st.button("🔄 Clear Filters", key="clear_links_filters"):
                for key in ['links_within_limit', 'links_filtered', 'links_used',
                            'links_executed', 'links_success', 'links_failure',
                            'links_has_chat', 'links_has_uid',
                            'links_date', 'links_data', 'auto_load_links']:
                    st.session_state.pop(key, None)
                st.rerun()

        active_filters = []
        if within_limit_filter  != "All": active_filters.append(f"Within Timeframe: {within_limit_filter}")
        if filtered_filter       != "All": active_filters.append(f"Filtered: {filtered_filter}")
        if used_filter           != "All": active_filters.append(f"Used: {used_filter}")
        if executed_filter       != "All": active_filters.append(f"Executed: {executed_filter}")
        if success_filter        != "All": active_filters.append(f"Success: {success_filter}")
        if failure_filter        != "All": active_filters.append(f"Failure: {failure_filter}")
        if has_chat_link_filter  != "All": active_filters.append(f"Has Chat Link: {has_chat_link_filter}")
        if has_user_id_filter    != "All": active_filters.append(f"Has User ID: {has_user_id_filter}")
        if tweeted_date:                   active_filters.append(f"Date: {tweeted_date}")
        if active_filters:
            st.info(f"**Active Filters:** {' | '.join(active_filters)}")

    # ── Delete helpers ────────────────────────────────────────────────────────

    def _show_link_delete_feedback(self, key: str):
        fb_key = f"link_delete_feedback_{key}"
        if fb_key in st.session_state:
            level, msg = st.session_state.pop(fb_key)
            if level == "success":
                st.success(msg)
            else:
                st.error(msg)

    def _execute_link_delete(self, link_id: int, feedback_key: str):
        result = pg_links.delete_link(link_id, cleanup_mongo=True)
        if result["deleted"]:
            parts = [f"🗑️ Deleted link ID {link_id}"]
            if result["mappings_deleted"]:
                parts.append(f"{result['mappings_deleted']} mapping(s)")
            if result["connections_deleted"]:
                parts.append(f"{result['connections_deleted']} connection(s)")
            if result["content_nullified"]:
                parts.append(f"{result['content_nullified']} content back-reference(s) cleared")
            if result["mongo_reset"]:
                parts.append(f"{result['mongo_reset']} MongoDB doc(s) updated")
            st.session_state[f"link_delete_feedback_{feedback_key}"] = (
                "success", " — ".join(parts)
            )
        else:
            st.session_state[f"link_delete_feedback_{feedback_key}"] = (
                "error", f"❌ Delete failed: {result.get('error', 'unknown error')}"
            )
        st.session_state.links_data = None

    def _render_links_table(self):
        st.markdown("### 📋 Links Data")

        within_limit_filter = st.session_state.get('links_within_limit', 'All')
        filtered_filter     = st.session_state.get('links_filtered',     'All')
        used_filter         = st.session_state.get('links_used',         'All')
        executed_filter     = st.session_state.get('links_executed',     'All')
        success_filter      = st.session_state.get('links_success',      'All')
        failure_filter      = st.session_state.get('links_failure',      'All')
        has_chat_filter     = st.session_state.get('links_has_chat',     'All')
        has_uid_filter      = st.session_state.get('links_has_uid',      'All')
        tweeted_date        = st.session_state.get('links_date')
        limit               = st.session_state.get('links_limit', 100)

        within_limit = None if within_limit_filter == 'All' else (within_limit_filter == 'Yes')
        filtered     = None if filtered_filter     == 'All' else (filtered_filter     == 'Yes')
        used         = None if used_filter         == 'All' else (used_filter         == 'Yes')
        executed     = None if executed_filter     == 'All' else (executed_filter     == 'Yes')
        success      = None if success_filter      == 'All' else (success_filter      == 'Yes')
        failure      = None if failure_filter      == 'All' else (failure_filter      == 'Yes')
        has_chat     = None if has_chat_filter     == 'All' else (has_chat_filter     == 'Yes')
        has_uid      = None if has_uid_filter      == 'All' else (has_uid_filter      == 'Yes')

        auto_load = st.session_state.get('auto_load_links', False)

        if st.button("🔍 Load Data", key="load_links_data") or auto_load or 'links_data' not in st.session_state:
            st.session_state.pop('auto_load_links', None)
            with st.spinner("Loading links data..."):
                links_data = pg_links.get_comprehensive_links_with_filters(
                    limit=limit,
                    within_limit=within_limit,
                    filtered=filtered,
                    used=used,
                    executed=executed,
                    success=success,
                    failure=failure,
                    tweeted_date=tweeted_date,
                    has_chat_link=has_chat,
                    has_user_id=has_uid,
                )
                # Note: has_chat_link and has_user_id filters are applied below
                # if the postgres function doesn't support them yet
                if has_chat is not None:
                    links_data = [r for r in links_data if bool(r.get('chat_link')) == has_chat]
                if has_uid is not None:
                    links_data = [r for r in links_data if bool(r.get('tweet_author_user_id')) == has_uid]
                st.session_state.links_data = links_data

        links_data = st.session_state.get('links_data', [])
        self._show_link_delete_feedback("table_level")

        if links_data:
            df = pd.DataFrame(links_data)
            display_df = self._simplify_links_df(df)

            st.dataframe(display_df, use_container_width=True, hide_index=True, height=500)

            csv = display_df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"links_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )

            stats = self._get_links_stats()
            st.caption(f"Showing {len(df)} of {stats['total']} total links")

            chat_count = int(df['chat_link'].notna().sum()) if 'chat_link' in df.columns else 0
            uid_count  = int(df['tweet_author_user_id'].notna().sum()) if 'tweet_author_user_id' in df.columns else 0
            success_count = int(df['success'].sum()) if 'success' in df.columns else 0
            failure_count = int(df['failure'].sum()) if 'failure' in df.columns else 0
            st.caption(
                f"✅ Success: {success_count} | ❌ Failure: {failure_count} | "
                f"💬 Chat Links: {chat_count} | 🆔 User IDs: {uid_count}"
            )

            st.markdown("---")
            with st.expander("🗑️ Delete individual links", expanded=False):
                st.info(
                    "Select a link ID to delete it along with all its mappings, "
                    "connections, and content back-references."
                )
                self._render_per_row_delete_controls(links_data)

        else:
            st.warning("⚠️ No links found matching the current filters.")
            st.info("💡 **Tip**: Click '🔍 Load Data' or '🔍 Apply Filters' to fetch data")

    def _render_per_row_delete_controls(self, links_data: list):
        if not links_data:
            st.info("No links loaded.")
            return

        id_options = {}
        for item in links_data:
            lid  = item.get("links_id")
            url  = item.get("link", "") or ""
            date = ""
            tt   = item.get("tweeted_time")
            if tt:
                try:
                    date = tt.strftime("%Y-%m-%d") if hasattr(tt, "strftime") else str(tt)[:10]
                except Exception:
                    date = str(tt)[:10]
            label = f"#{lid}  —  {url[:55]}{'…' if len(url) > 55 else ''}  [{date}]"
            id_options[label] = lid

        col_sel, col_arm, col_confirm, col_cancel = st.columns([4, 1, 1, 1])

        with col_sel:
            selected_label = st.selectbox(
                "Select link to delete:",
                options=list(id_options.keys()),
                key="del_link_selectbox",
                label_visibility="collapsed"
            )

        selected_id = id_options.get(selected_label)
        arm_key = f"arm_link_delete_{selected_id}"

        with col_arm:
            if not st.session_state.get(arm_key, False):
                if st.button("🗑️ Delete", key="btn_arm_link_delete",
                             use_container_width=True):
                    st.session_state[arm_key] = True
                    st.rerun()

        if st.session_state.get(arm_key, False):
            with col_confirm:
                if st.button("⚠️ Confirm", key="btn_confirm_link_delete",
                             use_container_width=True, type="primary"):
                    st.session_state[arm_key] = False
                    self._execute_link_delete(selected_id, "table_level")
                    st.rerun()
            with col_cancel:
                if st.button("Cancel", key="btn_cancel_link_delete",
                             use_container_width=True):
                    st.session_state[arm_key] = False
                    st.rerun()
            st.warning(
                f"⚠️ About to permanently delete **link #{selected_id}** "
                f"and all associated mappings / connections. This cannot be undone."
            )

    # =========================================================================
    # TAB 2: CHAT LINKS
    # =========================================================================

    def _render_chat_links_view(self):
        st.subheader("💬 Chat Links")
        st.info("""
        **What's this?** Links that have a resolved X/Twitter DM chat URL.

        - **Author User ID**: The numeric X user ID of the tweet author
        - **Chat Link**: `https://x.com/i/chat/{YOUR_ID}-{THEIR_ID}`  
          Opens the DM conversation between you and the tweet author directly.
        - Chat links are populated automatically during extraction when  
          `YOUR_TWITTER_USER_ID` is set in the environment.
        """)

        self._render_chat_links_stats()
        st.divider()

        col_load, col_filter, _ = st.columns([1, 2, 2])
        with col_load:
            load_btn = st.button("📋 Load Chat Links", key="load_chat_links", type="primary")
        with col_filter:
            chat_search = st.text_input("Filter by username / user ID:", key="chat_search",
                                        placeholder="e.g. touchofm_ or 44196397")

        if load_btn or 'chat_links_data' not in st.session_state:
            with st.spinner("Loading chat links..."):
                st.session_state.chat_links_data = self._fetch_chat_links()

        chat_data = st.session_state.get('chat_links_data', [])

        if chat_search and chat_data:
            q = chat_search.strip().lower()
            chat_data = [
                r for r in chat_data
                if q in (r.get('link_account_username') or '').lower()
                or q in (r.get('tweet_author_user_id') or '').lower()
                or q in (r.get('link') or '').lower()
            ]

        if not chat_data:
            st.warning("No chat links found. Make sure `YOUR_TWITTER_USER_ID` is set in the environment and extraction has run.")
            return

        df = pd.DataFrame(chat_data)
        display_df = self._simplify_chat_links_df(df)
        st.dataframe(display_df, use_container_width=True, hide_index=True, height=500)
        st.caption(f"Showing {len(display_df)} chat links")

        csv = display_df.to_csv(index=False)
        st.download_button(
            label="📥 Download Chat Links CSV",
            data=csv,
            file_name=f"chat_links_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

        st.divider()
        st.markdown("### 🔗 Quick-Open Chat Links")
        st.markdown("Click a link below to open the DM conversation on X:")
        for row in chat_data[:30]:
            chat = row.get('chat_link')
            uid  = row.get('tweet_author_user_id', '')
            url  = (row.get('link') or '')[:60]
            if chat:
                st.markdown(f"- [@user `{uid}`]({chat}) — `{url}`")

    def _render_chat_links_stats(self):
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("SELECT COUNT(*) AS c FROM links WHERE chat_link IS NOT NULL")
                    total_chat = (cursor.fetchone() or {}).get('c', 0)
                    cursor.execute("SELECT COUNT(*) AS c FROM links WHERE tweet_author_user_id IS NOT NULL")
                    total_uid = (cursor.fetchone() or {}).get('c', 0)
                    cursor.execute("SELECT COUNT(*) AS c FROM links")
                    total_links = (cursor.fetchone() or {}).get('c', 0)
                    cursor.execute("SELECT COUNT(DISTINCT tweet_author_user_id) AS c FROM links WHERE tweet_author_user_id IS NOT NULL")
                    unique_authors = (cursor.fetchone() or {}).get('c', 0)

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Links",        total_links)
            col2.metric("With User ID",        total_uid)
            col3.metric("With Chat Link",      total_chat)
            col4.metric("Unique Authors",      unique_authors)
            coverage = round(total_chat / total_links * 100, 1) if total_links > 0 else 0
            st.caption(f"Chat link coverage: {coverage}% of all links")
        except Exception as e:
            logger.error(f"Error fetching chat link stats: {e}")

    def _fetch_chat_links(self) -> list:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            l.links_id,
                            a.username AS link_account_username,
                            l.link,
                            l.tweet_id,
                            l.tweet_author_user_id,
                            l.chat_link,
                            l.tweeted_time,
                            l.used,
                            l.filtered,
                            l.executed,
                            l.success,
                            l.failure,
                            l.workflow_status
                        FROM links l
                        LEFT JOIN accounts a ON l.account_id = a.account_id
                        WHERE l.chat_link IS NOT NULL
                        ORDER BY l.tweeted_time DESC NULLS LAST
                        LIMIT 500
                    """)
                    return [dict(r) for r in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error fetching chat links: {e}")
            return []

    def _simplify_chat_links_df(self, df):
        display = pd.DataFrame()
        if 'links_id'              in df.columns: display['ID']            = df['links_id']
        if 'link_account_username' in df.columns: display['Account']       = df['link_account_username']
        if 'tweet_id'              in df.columns: display['Tweet ID']      = df['tweet_id']
        if 'tweet_author_user_id'  in df.columns: display['Author User ID']= df['tweet_author_user_id']
        if 'chat_link'             in df.columns: display['Chat Link']     = df['chat_link']
        if 'link'                  in df.columns: display['Tweet URL']     = df['link']
        if 'tweeted_time'          in df.columns:
            display['Tweeted At'] = pd.to_datetime(df['tweeted_time'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
        if 'used'      in df.columns: display['Used']     = df['used'].apply(lambda x: '✅' if x else '❌')
        if 'executed'  in df.columns: display['Executed'] = df['executed'].apply(lambda x: '✅' if x else '❌')
        if 'success'   in df.columns: display['Success']  = df['success'].apply(lambda x: '✅' if x else '')
        return display

    # =========================================================================
    # TAB 1 STATS
    # =========================================================================

    def _render_links_stats(self):
        st.markdown("### 📊 Link Statistics")

        col1, col2, col3, col4 = st.columns(4)
        stats = self._get_links_stats()

        col1.metric("Total Links", stats['total'])
        col2.metric("Used",        stats['used'])
        col3.metric("Filtered",    stats['filtered'])
        col4.metric("Executed",    stats['executed'])

        st.divider()

        st.markdown("#### 🆔 Author & Chat Link Coverage")
        col5, col6, col7, col8 = st.columns(4)
        col5.metric("With User ID",   stats.get('with_user_id', 0))
        col6.metric("With Chat Link", stats.get('with_chat_link', 0))
        coverage = (
            round(stats.get('with_chat_link', 0) / stats['total'] * 100, 1)
            if stats['total'] > 0 else 0
        )
        col7.metric("Chat Coverage",  f"{coverage}%")
        col8.metric("✅ Success",      stats.get('success', 0))

        st.divider()

        st.markdown("#### 🔄 Filtering Stages")
        col9, col10, col11 = st.columns(3)
        col9.metric("1️⃣ Within Timeframe", stats.get('within_limit', 0))
        col10.metric("2️⃣ Filtered (Content)", stats.get('filtered', 0))
        col11.metric("3️⃣ Used (Assigned)",    stats.get('used', 0))

    def _get_links_stats(self):
        try:
            detailed_stats = pg_links.get_detailed_links_stats()
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("SELECT COUNT(*) as c FROM links WHERE success = TRUE")
                    success = (cursor.fetchone() or {}).get('c', 0)
                    cursor.execute("SELECT COUNT(*) as c FROM links WHERE failure = TRUE")
                    failure = (cursor.fetchone() or {}).get('c', 0)
                    cursor.execute("SELECT COUNT(*) as c FROM links WHERE tweet_author_user_id IS NOT NULL")
                    with_user_id = (cursor.fetchone() or {}).get('c', 0)
                    cursor.execute("SELECT COUNT(*) as c FROM links WHERE chat_link IS NOT NULL")
                    with_chat_link = (cursor.fetchone() or {}).get('c', 0)
            return {
                'total':          detailed_stats.get('total_links', 0),
                'used':           detailed_stats.get('used_links', 0),
                'filtered':       detailed_stats.get('filtered_links', 0),
                'executed':       detailed_stats.get('executed_links', 0),
                'within_limit':   detailed_stats.get('within_limit_count', 0),
                'success':        success,
                'failure':        failure,
                'with_user_id':   with_user_id,
                'with_chat_link': with_chat_link,
            }
        except Exception as e:
            logger.error(f"Error getting links stats: {e}")
            return {
                'total': 0, 'used': 0, 'filtered': 0, 'executed': 0,
                'within_limit': 0, 'success': 0, 'failure': 0,
                'with_user_id': 0, 'with_chat_link': 0,
            }

    def _simplify_links_df(self, df):
        display_df = pd.DataFrame()
    
        if 'link'                  in df.columns: display_df['URL']            = df['link']
        if 'tweeted_time'          in df.columns:
            display_df['Tweeted At'] = pd.to_datetime(df['tweeted_time'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
        if 'tweet_author_user_id'  in df.columns: display_df['Author User ID'] = df['tweet_author_user_id']
        if 'within_limit'          in df.columns: display_df['Within Limit']   = df['within_limit'].apply(lambda x: '✅' if x else '❌')
        if 'filtered'              in df.columns: display_df['Filtered']        = df['filtered'].apply(lambda x: '✅' if x else '❌')
        if 'used'                  in df.columns: display_df['Used']            = df['used'].apply(lambda x: '✅' if x else '❌')
        if 'executed'              in df.columns: display_df['Executed']        = df['executed'].apply(lambda x: '✅' if x else '❌')
        if 'success'               in df.columns: display_df['✅ Success']       = df['success'].apply(lambda x: '✅' if x else '')
        if 'failure'               in df.columns: display_df['❌ Failure']       = df['failure'].apply(lambda x: '❌' if x else '')
        if 'tweet_id'              in df.columns: display_df['Tweet ID']        = df['tweet_id']
        if 'links_id'              in df.columns: display_df['ID']              = df['links_id']
        if 'total_content_count'   in df.columns:
            display_df['Content Items'] = df['total_content_count'].fillna(0).astype(int)
    
        return display_df

    # =========================================================================
    # TAB 3: CONNECTIONS
    # =========================================================================

    def _render_connections(self):
        st.subheader("🔗 Links → Workflows → Content Connections")

        st.info("""
        **🔗 Connection Flow**: Links → Workflows → Content

        **💡 Key Tables**:
        - `links` (PostgreSQL): Tweet links with workflow connections, user IDs, and chat links
        - `content` (PostgreSQL): Generated content
        - `workflow_metadata` (MongoDB): Workflow metadata tracking
        - `link_content_mappings` (PostgreSQL): Junction table
        """)

        col1, col2, col3 = st.columns([1, 1, 2])

        with col1:
            if st.button("🔄 Refresh Connections", key="refresh_connections_button"):
                st.session_state.workflow_connections_data = None

        with col2:
            connection_type = st.selectbox(
                "Connection Type:",
                ["All", "Active Only", "With Link & Content", "Missing Content", "Missing Link"],
                key="connection_type_filter"
            )

        if st.button("🔍 Load Connections", key="load_connections_btn", type="primary") or \
           'workflow_connections_data' not in st.session_state:
            with st.spinner("Loading connection data..."):
                connections_data = self._fetch_workflow_connections(connection_type)
                st.session_state.workflow_connections_data = connections_data

        connections_data = st.session_state.get('workflow_connections_data', [])

        if connections_data:
            self._render_connection_metrics(connections_data)
            st.divider()

            tab1, tab2, tab3 = st.tabs([
                "📊 Complete Connections",
                "🔗 Link Details",
                "📝 Content Details"
            ])
            with tab1:
                self._render_complete_connections_table(connections_data)
            with tab2:
                self._render_link_connections_detail(connections_data)
            with tab3:
                self._render_content_connections_detail(connections_data)
        else:
            st.warning("⚠️ No connections found. Click 'Load Connections' to fetch data.")

    def _fetch_workflow_connections(self, connection_type):
        try:
            connections = []
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT
                            l.links_id, l.link as link_url, l.tweet_id, l.tweeted_time,
                            l.used, l.filtered, l.executed, l.within_limit,
                            l.workflow_type, l.connected_content_id, l.connected_via_workflow,
                            l.content_connection_time, l.connection_status, l.account_id,
                            l.tweet_author_user_id, l.chat_link,
                            a.username,
                            c.content_id, c.content_name, c.content_type,
                            c.content as content_text, c.automa_workflow_id,
                            c.workflow_name, c.workflow_status, c.has_content,
                            c.used as content_used
                        FROM links l
                        LEFT JOIN accounts a ON l.account_id = a.account_id
                        LEFT JOIN content c ON l.connected_content_id = c.content_id
                        WHERE 1=1
                    """
                    if connection_type == "Active Only":
                        query += " AND l.connection_status = 'active'"
                    elif connection_type == "With Link & Content":
                        query += " AND l.connected_content_id IS NOT NULL AND c.automa_workflow_id IS NOT NULL"
                    elif connection_type == "Missing Content":
                        query += " AND l.connected_content_id IS NULL AND l.used = TRUE"
                    elif connection_type == "Missing Link":
                        query += " AND c.content_id IS NOT NULL AND c.automa_workflow_id IS NOT NULL AND l.links_id IS NULL"

                    query += " ORDER BY l.tweeted_time DESC NULLS LAST LIMIT 500"
                    cursor.execute(query)
                    rows = cursor.fetchall()

                    for row in rows:
                        connection = dict(row)
                        if connection.get('automa_workflow_id'):
                            try:
                                connection['workflow_metadata'] = self._get_workflow_metadata_from_mongo(
                                    connection['automa_workflow_id']
                                )
                            except Exception as e:
                                logger.error(f"Error fetching workflow metadata: {e}")
                                connection['workflow_metadata'] = None
                        else:
                            connection['workflow_metadata'] = None
                        connections.append(connection)

            return connections

        except Exception as e:
            logger.error(f"Error fetching workflow connections: {e}")
            import traceback
            logger.error(traceback.format_exc())
            st.error(f"Error fetching connections: {str(e)}")
            return []

    def _get_workflow_metadata_from_mongo(self, automa_workflow_id):
        try:
            from src.core.database.mongodb.connection import get_mongo_collection
            from bson import ObjectId

            metadata_collection = get_mongo_collection('workflow_metadata', db_name='messages_db')
            if metadata_collection is None:
                return None

            if isinstance(automa_workflow_id, str):
                try:
                    automa_workflow_id = ObjectId(automa_workflow_id)
                except Exception:
                    pass

            metadata = metadata_collection.find_one({'automa_workflow_id': automa_workflow_id})
            if metadata:
                if '_id' in metadata:
                    metadata['_id'] = str(metadata['_id'])
                if 'automa_workflow_id' in metadata:
                    metadata['automa_workflow_id'] = str(metadata['automa_workflow_id'])
                return metadata
            return None

        except Exception as e:
            logger.error(f"Error getting workflow metadata from MongoDB: {e}")
            return None

    def _render_connection_metrics(self, connections_data):
        st.markdown("### 📊 Connection Summary")

        total           = len(connections_data)
        with_content    = len([c for c in connections_data if c.get('connected_content_id') is not None])
        with_workflow   = len([c for c in connections_data if c.get('automa_workflow_id') is not None])
        fully_connected = len([c for c in connections_data if c.get('connected_content_id') and c.get('automa_workflow_id')])
        active          = len([c for c in connections_data if c.get('connection_status') == 'active'])
        used_links      = len([c for c in connections_data if c.get('used') is True])
        executed_links  = len([c for c in connections_data if c.get('executed') is True])
        with_chat       = len([c for c in connections_data if c.get('chat_link')])

        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT COUNT(DISTINCT link_id) as links_with_mappings,
                               COUNT(DISTINCT lcm.content_id) as content_with_mappings,
                               COUNT(*) as total_mappings
                        FROM link_content_mappings lcm
                    """)
                    junction_stats = cursor.fetchone() or {}
        except Exception as e:
            logger.error(f"Error getting junction stats: {e}")
            junction_stats = {}

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Links",       total)
        col2.metric("With Content",      with_content)
        col3.metric("With Workflow",     with_workflow)
        col4.metric("Fully Connected",   fully_connected)

        col5, col6, col7, col8 = st.columns(4)
        col5.metric("Active",            active)
        col6.metric("Used",              used_links)
        col7.metric("Executed",          executed_links)
        col8.metric("💬 With Chat Link", with_chat)

        st.divider()
        st.markdown("### 🔗 Junction Table Statistics")
        col9, col10, col11 = st.columns(3)
        col9.metric("Links with Mappings",    junction_stats.get('links_with_mappings', 0))
        col10.metric("Content with Mappings", junction_stats.get('content_with_mappings', 0))
        col11.metric("Total Mappings",        junction_stats.get('total_mappings', 0))

    def _render_complete_connections_table(self, connections_data):
        st.markdown("### 📊 Complete Connection Overview")
        if not connections_data:
            st.info("No connections to display")
            return

        display_data = []
        for conn in connections_data:
            wf_meta = conn.get('workflow_metadata', {}) or {}
            content_items = wf_meta.get('content_items', [])
            content_items_preview = ", ".join([item.get('label', 'CONTENT') for item in content_items]) if content_items else ""

            try:
                with get_postgres_connection() as conn_db:
                    with conn_db.cursor(cursor_factory=RealDictCursor) as cursor:
                        cursor.execute("""
                            SELECT lcm.content_id, c.content_name
                            FROM link_content_mappings lcm
                            JOIN content c ON lcm.content_id = c.content_id
                            WHERE lcm.link_id = %s
                        """, (conn.get('links_id'),))
                        junction_mappings = cursor.fetchall()
                        junction_content_ids = [str(m['content_id']) for m in junction_mappings]
            except Exception:
                junction_content_ids = []

            display_data.append({
                'Link ID':           conn.get('links_id'),
                'Tweet ID':          conn.get('tweet_id'),
                'Account':           conn.get('username', 'Unknown'),
                'Author User ID':    conn.get('tweet_author_user_id') or '—',
                'Chat Link':         conn.get('chat_link') or '—',
                'Link Status':       '✅ Used' if conn.get('used') else '⚪ Unused',
                'Workflow Name':     conn.get('workflow_name') or conn.get('connected_via_workflow') or '—',
                'Workflow Status':   wf_meta.get('status', '—'),
                'Has Content':       '✅' if wf_meta.get('has_content') else '❌',
                'Content Items':     content_items_preview or '—',
                'Direct Content ID': conn.get('connected_content_id') or '—',
                'Junction Mappings': len(junction_content_ids),
                'Tweeted':           conn.get('tweeted_time').strftime('%Y-%m-%d %H:%M') if conn.get('tweeted_time') else '—'
            })

        df = pd.DataFrame(display_data)
        st.dataframe(df, use_container_width=True, hide_index=True, height=500)

        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"connections_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

    def _render_link_connections_detail(self, connections_data):
        st.markdown("### 🔗 Link Connection Details")
        connected_links = [c for c in connections_data if c.get('connected_content_id') or c.get('automa_workflow_id')]

        if not connected_links:
            st.info("No links with connections found")
            return

        st.write(f"Showing {len(connected_links)} links with connections")

        for conn in connected_links[:50]:
            with st.expander(f"🔗 Link #{conn.get('links_id')} - {conn.get('tweet_id')}", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**Link Information:**")
                    st.write(f"**Link ID:** {conn.get('links_id')}")
                    st.write(f"**Tweet ID:** {conn.get('tweet_id')}")
                    st.write(f"**URL:** {conn.get('link_url', '')[:100]}...")
                    st.write(f"**Account:** {conn.get('username')}")
                    st.write(f"**Author User ID:** {conn.get('tweet_author_user_id') or '—'}")
                    chat = conn.get('chat_link')
                    if chat:
                        st.markdown(f"**Chat Link:** [{chat[:60]}]({chat})")
                    else:
                        st.write("**Chat Link:** —")
                    st.write(f"**Used:** {'✅ Yes' if conn.get('used') else '❌ No'}")
                    st.write(f"**Executed:** {'✅ Yes' if conn.get('executed') else '❌ No'}")
                with col2:
                    st.markdown("**Connection Information:**")
                    st.write(f"**Content ID:** {conn.get('connected_content_id') or 'None'}")
                    st.write(f"**Workflow:** {conn.get('connected_via_workflow') or 'None'}")
                    st.write(f"**Status:** {conn.get('connection_status', 'N/A')}")
                    wf_meta = conn.get('workflow_metadata', {})
                    if wf_meta:
                        st.write(f"**Workflow Status:** {wf_meta.get('status', 'Unknown')}")
                        st.write(f"**Has Content:** {'✅' if wf_meta.get('has_content') else '❌'}")

    def _render_content_connections_detail(self, connections_data):
        st.markdown("### 📝 Content Connection Details")
        with_content = [c for c in connections_data if c.get('connected_content_id')]

        if not with_content:
            st.info("No content connections found")
            return

        st.write(f"Showing {len(with_content)} content connections")

        for conn in with_content[:50]:
            with st.expander(f"📝 Content #{conn.get('content_id')} - {conn.get('content_name')}", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**Content Information:**")
                    st.write(f"**Content ID:** {conn.get('content_id')}")
                    st.write(f"**Name:** {conn.get('content_name')}")
                    st.write(f"**Type:** {conn.get('content_type')}")
                    st.write(f"**Used:** {'✅' if conn.get('content_used') else '❌'}")
                with col2:
                    st.markdown("**Workflow Information:**")
                    st.write(f"**Workflow ID:** {conn.get('automa_workflow_id') or 'None'}")
                    st.write(f"**Workflow Name:** {conn.get('workflow_name') or 'None'}")
                    st.markdown("**Link Connection:**")
                    st.write(f"**Link ID:** {conn.get('links_id', 'None')}")
                    st.write(f"**Author User ID:** {conn.get('tweet_author_user_id') or '—'}")
                    chat = conn.get('chat_link')
                    if chat:
                        st.markdown(f"**Chat Link:** [{chat[:50]}...]({chat})")
                    st.write(f"**Status:** {conn.get('connection_status', 'N/A')}")

    # =========================================================================
    # TAB 4: ANALYTICS
    # =========================================================================

    def _render_analytics(self):
        st.subheader("📈 Analytics & Insights")

        if 'links_data' in st.session_state and st.session_state.links_data:
            links_df = pd.DataFrame(st.session_state.links_data)
        else:
            links_data = pg_links.get_comprehensive_links_with_filters(limit=500)
            links_df = pd.DataFrame(links_data) if links_data else pd.DataFrame()

        if links_df.empty:
            st.warning("No data available for analytics. Please load data in the Links tab first.")
            return

        st.markdown("### 📊 Overview Metrics")
        col1, col2, col3, col4 = st.columns(4)

        total_links    = len(links_df)
        used_links     = int(links_df['used'].sum())           if 'used'     in links_df.columns else 0
        filtered_links = int(links_df['filtered'].sum())       if 'filtered' in links_df.columns else 0
        executed_links = int(links_df['executed'].sum())       if 'executed' in links_df.columns else 0
        uid_count      = int(links_df['tweet_author_user_id'].notna().sum()) if 'tweet_author_user_id' in links_df.columns else 0
        chat_count     = int(links_df['chat_link'].notna().sum())            if 'chat_link'            in links_df.columns else 0

        col1.metric("Total Links",    total_links)
        col2.metric("Used Links",     used_links)
        col3.metric("With User ID",   uid_count)
        col4.metric("With Chat Link", chat_count)

        st.divider()

        col_chart1, col_chart2 = st.columns(2)

        with col_chart1:
            st.markdown("### 📅 Daily Link Trends")
            if 'tweeted_time' in links_df.columns:
                links_df['tweeted_date'] = pd.to_datetime(links_df['tweeted_time'], errors='coerce').dt.date
                daily_links = links_df.groupby('tweeted_date').size().reset_index(name='Count')
                st.line_chart(data=daily_links.set_index('tweeted_date')['Count'], use_container_width=True)
            else:
                st.info("No date data available")

        with col_chart2:
            st.markdown("### 🎯 Link Status Distribution")
            status_data = {
                'Used':             used_links,
                'Filtered':         filtered_links,
                'Executed':         executed_links,
                'With User ID':     uid_count,
                'With Chat Link':   chat_count,
            }
            status_df = pd.DataFrame(list(status_data.items()), columns=['Status', 'Count'])
            st.bar_chart(status_df.set_index('Status')['Count'], use_container_width=True)

        st.divider()

        st.markdown("### 🔄 Workflow Processing Funnel")
        within_count = int(links_df['within_limit'].sum()) if 'within_limit' in links_df.columns else 0
        funnel_data = {
            'Stage': ['Total Links', 'Within Timeframe', 'Not Filtered', 'Used', 'Executed'],
            'Count': [
                total_links,
                within_count,
                total_links - filtered_links,
                used_links,
                executed_links
            ]
        }
        funnel_df = pd.DataFrame(funnel_data)
        st.bar_chart(funnel_df.set_index('Stage')['Count'], use_container_width=True, horizontal=True)

        if total_links > 0:
            col_conv1, col_conv2, col_conv3 = st.columns(3)
            used_rate     = (used_links / total_links) * 100
            executed_rate = (executed_links / used_links) * 100 if used_links > 0 else 0
            chat_rate     = (chat_count / total_links) * 100
            col_conv1.metric("Used Rate",       f"{used_rate:.1f}%")
            col_conv2.metric("Execution Rate",  f"{executed_rate:.1f}%")
            col_conv3.metric("Chat Coverage",   f"{chat_rate:.1f}%")

        st.divider()
        st.markdown("### 👤 Account Breakdown")
        if 'link_account_username' in links_df.columns:
            account_links = links_df.groupby('link_account_username').size().reset_index(name='Count')
            st.dataframe(account_links, use_container_width=True, hide_index=True)
        else:
            st.info("No account data available")

    # =========================================================================
    # TAB 5: BULK ACTIONS
    # =========================================================================

    def _render_bulk_actions(self):
        st.subheader("⚙️ Bulk Actions & Management")
        st.warning("⚠️ **DANGER ZONE**: These actions affect data from extraction and filtering pipelines.")

        st.markdown("### 🎯 Select Account")
        accounts = self._get_available_accounts()
        account_options = {f"{a['username']} (ID: {a['account_id']})": a['account_id'] for a in accounts}

        selected_account_name = st.selectbox(
            "Account:", options=list(account_options.keys()), key="account_select"
        )
        account_id = account_options[selected_account_name]

        st.markdown("### 🔍 Current State Preview")
        if st.button("🔍 Preview", key="preview_state"):
            self._render_state_preview(account_id)

        st.divider()

        st.markdown("### 🔄 Reverse filter_links DAG")
        st.info("""
        Resets filtering operations:
        - `filtered`, `within_limit`, `used` flags → FALSE
        - Deletes `link_content_mappings`
        - Resets MongoDB workflow metadata
        - **Does NOT clear** `tweet_author_user_id` or `chat_link`
        """)

        if st.button("🔄 Reverse filter_links", key="reverse_filter", type="secondary"):
            st.session_state.show_reverse_confirm = True
            st.rerun()

        if st.session_state.get('show_reverse_confirm', False):
            self._render_reverse_confirmation(account_id, selected_account_name)

        st.divider()

        st.markdown("### 🗑️ Delete ALL Pipeline Data")
        st.error("""
        **WARNING**: Deletes ALL data for this account:
        - All links from `links` table (including user IDs and chat links)
        - All content mappings
        - MongoDB workflow metadata

        **THIS CANNOT BE UNDONE!**
        """)

        if st.button("🗑️ Delete ALL Data", key="delete_all", type="primary"):
            st.session_state.show_delete_confirm = True
            st.rerun()

        if st.session_state.get('show_delete_confirm', False):
            self._render_delete_confirmation(account_id, selected_account_name)

    def _get_available_accounts(self):
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT a.account_id, a.username, COUNT(DISTINCT l.links_id) as total_links
                        FROM accounts a
                        LEFT JOIN links l ON a.account_id = l.account_id
                        WHERE a.username IS NOT NULL
                        GROUP BY a.account_id, a.username
                        ORDER BY a.username
                    """)
                    return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching accounts: {e}")
            return []

    def _render_state_preview(self, account_id):
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            COUNT(*) as total_links,
                            COUNT(*) FILTER (WHERE filtered = TRUE) as filtered_count,
                            COUNT(*) FILTER (WHERE within_limit = TRUE) as within_limit_count,
                            COUNT(*) FILTER (WHERE used = TRUE) as used_count,
                            COUNT(*) FILTER (WHERE executed = TRUE) as executed_count,
                            COUNT(*) FILTER (WHERE tweet_author_user_id IS NOT NULL) as with_user_id,
                            COUNT(*) FILTER (WHERE chat_link IS NOT NULL) as with_chat_link
                        FROM links WHERE account_id = %s
                    """, (account_id,))
                    stats = cursor.fetchone()

                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Links", stats['total_links'])
                        st.metric("Filtered",    stats['filtered_count'])
                    with col2:
                        st.metric("Within Timeframe", stats['within_limit_count'])
                        st.metric("Used",             stats['used_count'])
                    with col3:
                        st.metric("Executed",       stats['executed_count'])
                        st.metric("With User ID",   stats['with_user_id'])
                    with col4:
                        st.metric("With Chat Link", stats['with_chat_link'])
        except Exception as e:
            st.error(f"Error: {e}")

    def _render_reverse_confirmation(self, account_id, account_name):
        st.error("⚠️ **Confirm Reverse**")
        st.write(f"**Account:** {account_name}")

        col_yes, col_no = st.columns(2)
        with col_yes:
            if st.button("✅ Yes, Reverse", key="confirm_reverse"):
                try:
                    result = pg_links.reverse_filter_links_dag(account_id)
                    st.success("✅ Operation reversed!")
                    col1, col2 = st.columns(2)
                    col1.metric("Links Reset",       result['links_reset'])
                    col2.metric("Mappings Deleted",  result['link_content_mappings_deleted'])
                    st.session_state.show_reverse_confirm = False
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")
        with col_no:
            if st.button("❌ Cancel", key="cancel_reverse"):
                st.session_state.show_reverse_confirm = False
                st.rerun()

    def _render_delete_confirmation(self, account_id, account_name):
        st.error("⚠️ **DANGER: Confirm Delete**")
        st.write(f"**Account:** {account_name}")
        st.write("**This will delete ALL pipeline data!**")

        col_yes, col_no = st.columns(2)
        with col_yes:
            if st.button("🗑️ Yes, Delete ALL", key="confirm_delete"):
                try:
                    result = pg_links.delete_all_comprehensive(account_id)
                    st.success("✅ Deletion complete!")
                    col1, col2 = st.columns(2)
                    col1.metric("Links Deleted",    result['links'])
                    col2.metric("Mappings Deleted", result['link_content_mappings'])
                    st.session_state.show_delete_confirm = False
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")
        with col_no:
            if st.button("❌ Cancel", key="cancel_delete"):
                st.session_state.show_delete_confirm = False
                st.rerun()

    # =========================================================================
    # TAB 6: EXTRACTION STATE
    # =========================================================================

    def _render_extraction_state(self):
        st.subheader("🔍 Extraction State")

        st.info("""
        **What's this?** Tracks the last-seen tweet ID per account from the extraction pipeline.

        - The extractor saves the newest tweet ID after each successful run
        - On the next run, scrolling stops as soon as that tweet ID reappears — making extraction **incremental**
        - If no state exists for an account, the extractor does a full depth scan
        - Reset an account here to force a full re-scan on the next run
        """)

        self._render_extraction_state_metrics()
        st.divider()

        col_refresh, col_load, _ = st.columns([1, 1, 2])
        with col_refresh:
            if st.button("🔄 Refresh", key="refresh_extraction_state"):
                st.session_state.pop('extraction_state_data', None)
                st.rerun()
        with col_load:
            load_clicked = st.button("📋 Load State Table", key="load_extraction_state", type="primary")

        if load_clicked or 'extraction_state_data' not in st.session_state:
            with st.spinner("Loading extraction state..."):
                st.session_state.extraction_state_data = self._fetch_extraction_state()

        state_data = st.session_state.get('extraction_state_data', [])

        if not state_data:
            st.warning("No extraction state records found.")
            st.info(
                "Records appear here after the first successful extraction run. "
                "Each account gets one row once the Node.js pipeline saves its last-seen tweet ID."
            )
            self._render_reset_all_section(state_data)
            return

        st.markdown("### 📋 Per-Account Extraction State")
        df = self._build_extraction_state_df(state_data)
        st.dataframe(df, use_container_width=True, hide_index=True, height=400)

        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"extraction_state_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

        st.divider()
        st.markdown("### 🎛️ Per-Account Controls")
        self._render_per_account_controls(state_data)

        st.divider()
        self._render_reset_all_section(state_data)

    def _render_extraction_state_metrics(self):
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_schema = 'public'
                              AND table_name = 'account_extraction_state'
                        ) AS table_exists
                    """)
                    if not cursor.fetchone()['table_exists']:
                        st.warning(
                            "⚠️ `account_extraction_state` table not found. "
                            "Run the migration SQL first."
                        )
                        return

                    cursor.execute("""
                        SELECT
                            COUNT(*)                                               AS total_accounts,
                            COUNT(*) FILTER (WHERE last_seen_tweet_id IS NOT NULL) AS with_state,
                            COUNT(*) FILTER (WHERE last_seen_tweet_id IS NULL)     AS without_state,
                            COUNT(*) FILTER (
                                WHERE last_extraction_time IS NOT NULL
                                  AND NOW() - last_extraction_time > INTERVAL '6 hours'
                            )                                                      AS stale_count,
                            MAX(last_extraction_time)                              AS most_recent_run,
                            SUM(tweets_found_last_run)                             AS total_tweets_last_run,
                            SUM(parents_found_last_run)                            AS total_parents_last_run
                        FROM account_extraction_state
                    """)
                    m = cursor.fetchone()

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Tracked Accounts",  m['total_accounts'] or 0)
            col2.metric("With State",         m['with_state'] or 0)
            col3.metric("No State Yet",       m['without_state'] or 0)
            col4.metric("⚠️ Stale (>6h)",    m['stale_count'] or 0)
            st.divider()

            col5, col6, col7 = st.columns(3)
            most_recent = m['most_recent_run']
            if most_recent:
                ago_minutes = int(
                    (datetime.now(timezone.utc) - most_recent.replace(tzinfo=timezone.utc)).total_seconds() / 60
                )
                col5.metric("Most Recent Run", most_recent.strftime('%Y-%m-%d %H:%M'),
                            delta=f"{ago_minutes} min ago")
            else:
                col5.metric("Most Recent Run", "Never")
            col6.metric("Tweets (last run)",  m['total_tweets_last_run']  or 0)
            col7.metric("Parents (last run)", m['total_parents_last_run'] or 0)

        except Exception as e:
            logger.error(f"Error rendering extraction state metrics: {e}")
            st.error(f"Error loading metrics: {e}")

    def _fetch_extraction_state(self):
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.views
                            WHERE table_schema = 'public'
                              AND table_name = 'extraction_state_summary'
                        ) AS view_exists
                    """)
                    view_exists = cursor.fetchone()['view_exists']

                    if view_exists:
                        cursor.execute("SELECT * FROM extraction_state_summary")
                    else:
                        cursor.execute("""
                            SELECT
                                state_id, username, last_seen_tweet_id, last_extraction_time,
                                tweets_found_last_run, parents_found_last_run, last_tweet_url,
                                NULL AS hours_since_last_extraction, FALSE AS is_stale,
                                created_at, updated_at
                            FROM account_extraction_state
                            ORDER BY last_extraction_time DESC NULLS LAST
                        """)

                    rows = cursor.fetchall()
                    return [dict(r) for r in rows]

        except Exception as e:
            logger.error(f"Error fetching extraction state: {e}")
            st.error(f"Could not load extraction state: {e}")
            return []

    def _build_extraction_state_df(self, state_data):
        rows = []
        for s in state_data:
            last_run  = s.get('last_extraction_time')
            hours_ago = s.get('hours_since_last_extraction')
            last_run_str = (
                last_run.strftime('%Y-%m-%d %H:%M') if last_run and hasattr(last_run, 'strftime')
                else (str(last_run)[:16] if last_run else 'Never')
            )
            try:
                age_str = f"{float(hours_ago):.1f}h ago" if hours_ago is not None else '—'
            except (TypeError, ValueError):
                age_str = '—'

            tweet_url = s.get('last_tweet_url') or ''
            rows.append({
                'Account':            f"@{s['username']}",
                'Last Run':           last_run_str,
                'Age':                age_str,
                'Status':             '⚠️ Stale' if s.get('is_stale', False) else '✅ Fresh',
                'Last Tweet ID':      s.get('last_seen_tweet_id') or '—',
                'Last Tweet URL':     (tweet_url[:60] + '...') if len(tweet_url) > 60 else tweet_url,
                'Tweets (last run)':  s.get('tweets_found_last_run') or 0,
                'Parents (last run)': s.get('parents_found_last_run') or 0,
                'State ID':           s.get('state_id'),
            })
        return pd.DataFrame(rows)

    def _render_per_account_controls(self, state_data):
        if not state_data:
            st.info("No accounts to manage yet.")
            return

        for s in state_data:
            username    = s.get('username', 'unknown')
            last_run    = s.get('last_extraction_time')
            is_stale    = s.get('is_stale', False)
            tweet_id    = s.get('last_seen_tweet_id')
            tweet_url   = s.get('last_tweet_url') or ''
            status_icon = '⚠️' if is_stale else '✅'

            last_run_str = (
                last_run.strftime('%Y-%m-%d %H:%M') if last_run and hasattr(last_run, 'strftime')
                else (str(last_run)[:16] if last_run else None)
            )
            label = f"{status_icon} @{username}"
            label += f"  —  last run {last_run_str}" if last_run_str else "  —  never run"

            with st.expander(label, expanded=False):
                col_info, col_action = st.columns([3, 1])

                with col_info:
                    st.markdown(f"**Username:** @{username}")
                    st.markdown(f"**Last extraction:** {last_run_str or 'Never'}")
                    st.markdown(f"**Status:** {'⚠️ Stale — no run in 6+ hours' if is_stale else '✅ Fresh'}")
                    st.markdown(f"**Last seen tweet ID:** `{tweet_id or 'None'}`")
                    if tweet_url:
                        st.markdown(f"**Last tweet URL:** [{tweet_url[:60]}...]({tweet_url})")
                    st.markdown(
                        f"**Tweets found last run:** {s.get('tweets_found_last_run') or 0} &nbsp;&nbsp; "
                        f"**Parents found:** {s.get('parents_found_last_run') or 0}"
                    )

                with col_action:
                    st.markdown("&nbsp;")
                    reset_key   = f"reset_account_{username}"
                    confirm_key = f"confirm_reset_{username}"

                    if st.button("🔄 Reset State", key=reset_key,
                                 help=f"Clear last-seen tweet ID for @{username}"):
                        st.session_state[confirm_key] = True
                        st.rerun()

                    if st.session_state.get(confirm_key, False):
                        st.warning(f"Reset @{username}?")
                        col_yes, col_no = st.columns(2)
                        with col_yes:
                            if st.button("✅ Yes", key=f"yes_{username}"):
                                if self._reset_account_state(username):
                                    st.success(f"✅ @{username} reset")
                                    st.session_state.pop(confirm_key, None)
                                    st.session_state.pop('extraction_state_data', None)
                                    st.rerun()
                                else:
                                    st.error("Reset failed — check logs")
                        with col_no:
                            if st.button("❌ No", key=f"no_{username}"):
                                st.session_state.pop(confirm_key, None)
                                st.rerun()

    def _render_reset_all_section(self, state_data):
        st.markdown("### ⚠️ Reset All Accounts")
        st.warning("Clears the last-seen tweet ID for **every** account.")

        if st.button("🔄 Reset ALL Extraction State", key="reset_all_state", type="secondary"):
            st.session_state.show_reset_all_confirm = True
            st.rerun()

        if st.session_state.get('show_reset_all_confirm', False):
            st.error("⚠️ **Confirm: reset all accounts?** This cannot be undone.")
            col_yes, col_no = st.columns(2)
            with col_yes:
                if st.button("✅ Yes, Reset All", key="confirm_reset_all"):
                    count = self._reset_all_states()
                    if count is not None:
                        st.success(f"✅ Reset {count} account(s)")
                        st.session_state.show_reset_all_confirm = False
                        st.session_state.pop('extraction_state_data', None)
                        st.rerun()
                    else:
                        st.error("Reset failed — check logs")
            with col_no:
                if st.button("❌ Cancel", key="cancel_reset_all"):
                    st.session_state.show_reset_all_confirm = False
                    st.rerun()

    def _reset_account_state(self, username: str) -> bool:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT reset_extraction_state(%s)", (username,))
                    cursor.fetchone()
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error resetting extraction state for @{username}: {e}")
            return False

    def _reset_all_states(self):
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT reset_all_extraction_state()")
                    count = cursor.fetchone()[0]
                conn.commit()
            return count
        except Exception as e:
            logger.error(f"Error resetting all extraction states: {e}")
            return None
