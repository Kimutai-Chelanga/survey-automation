import streamlit as st
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd
from bson import ObjectId
from io import BytesIO
from PIL import Image
from gridfs import GridFS
import logging
import base64

from src.core.database.postgres import accounts as pg_accounts
from src.core.database.postgres.mongodb import get_mongodb_connection
from ..base_page import BasePage

logger = logging.getLogger(__name__)

# ── Streaming size threshold ───────────────────────────────────────────────────
VIDEO_INLINE_STREAM_LIMIT_MB = 50


# =============================================================================
# Timing display helper
# =============================================================================
def _render_timing_metrics(
    estimated_wait_ms: Optional[float],
    configured_wait_ms: Optional[float],
    actual_wait_ms: Optional[float],
    wait_margin_ms: Optional[float] = None,
    estimation_method: Optional[str] = None,
    compact: bool = False,
):
    def _fmt(ms) -> str:
        if ms is None:
            return "—"
        s = ms / 1000
        if s >= 60:
            return f"{s / 60:.1f} min"
        return f"{s:.0f}s"

    def _delta(actual, configured) -> Optional[str]:
        if actual is None or configured is None:
            return None
        diff_s = (actual - configured) / 1000
        sign   = "+" if diff_s > 0 else ""
        return f"{sign}{diff_s:.0f}s vs configured"

    if compact:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("⏱ Estimated", _fmt(estimated_wait_ms), help="Bottom-up block analysis result")
        with c2:
            st.metric("⚙️ Configured", _fmt(configured_wait_ms), help="Estimated + wait margin (what we slept)")
        with c3:
            delta = _delta(actual_wait_ms, configured_wait_ms)
            st.metric("✅ Actual", _fmt(actual_wait_ms), delta=delta, help="Real wall-clock sleep duration")
    else:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric(
                "⏱ Estimated Wait",
                _fmt(estimated_wait_ms),
                help="Bottom-up block analysis: Σ(delay timeouts + click waits + page loads) across all sub-workflows"
            )
        with c2:
            st.metric(
                "🛡️ Wait Margin",
                _fmt(wait_margin_ms),
                help="Safety buffer configured in Streamlit, added on top of the estimate"
            )
        with c3:
            st.metric(
                "⚙️ Configured Wait",
                _fmt(configured_wait_ms),
                help="Estimated + margin — the actual sleep() call duration"
            )
        with c4:
            delta = _delta(actual_wait_ms, configured_wait_ms)
            st.metric(
                "✅ Actual Wait",
                _fmt(actual_wait_ms),
                delta=delta,
                delta_color="inverse",
                help="Real wall-clock ms reported by sleep()"
            )

        if estimation_method or wait_margin_ms is not None:
            parts = []
            if estimation_method:
                parts.append(f"Method: **{estimation_method}**")
            if estimated_wait_ms is not None and wait_margin_ms is not None:
                parts.append(
                    f"{_fmt(estimated_wait_ms)} estimated + {_fmt(wait_margin_ms)} margin"
                    f" = {_fmt(configured_wait_ms)} configured"
                )
            if parts:
                st.caption("  •  ".join(parts))


def _timing_from_session(session: Dict) -> Dict:
    return {
        'estimated_wait_ms':  session.get('estimated_wait_ms'),
        'configured_wait_ms': session.get('configured_wait_ms'),
        'actual_wait_ms':     session.get('actual_wait_ms'),
        'wait_margin_ms':     session.get('wait_margin_ms'),
        'estimation_method':  session.get('timing_breakdown', {}).get('estimation_method'),
    }


# =============================================================================
# GridFSMediaViewer
# =============================================================================
class GridFSMediaViewer:
    """Component for viewing GridFS screenshots and videos."""

    def __init__(self, mongo_db):
        self.mongo_db = mongo_db
        self.screenshot_fs = GridFS(mongo_db, collection='screenshots')
        self.video_fs = GridFS(mongo_db, collection='video_recordings')

    def display_screenshot(self, gridfs_file_id, max_width: int = 400):
        try:
            file_id = ObjectId(gridfs_file_id) if isinstance(gridfs_file_id, str) else gridfs_file_id
            if not self.screenshot_fs.exists(file_id):
                st.warning(f"Screenshot not found: {gridfs_file_id}")
                return False
            image_data = self.screenshot_fs.get(file_id).read()
            image = Image.open(BytesIO(image_data))
            st.image(image, use_container_width=True)
            return True
        except Exception as e:
            logger.error(f"Error displaying screenshot: {e}")
            st.error(f"Failed to load screenshot: {str(e)}")
            return False

    def display_screenshot_from_base64(self, base64_data: str, caption: str = ""):
        """Display a screenshot from base64 data (from Automa logs)."""
        try:
            # Remove data URL prefix if present
            if ',' in base64_data:
                base64_data = base64_data.split(',')[1]
            
            image_data = base64.b64decode(base64_data)
            image = Image.open(BytesIO(image_data))
            st.image(image, caption=caption, use_container_width=True)
            return True
        except Exception as e:
            logger.error(f"Error displaying base64 screenshot: {e}")
            return False

    def display_thumbnail(self, gridfs_file_id, caption: str = ""):
        try:
            file_id = ObjectId(gridfs_file_id) if isinstance(gridfs_file_id, str) else gridfs_file_id
            if self.screenshot_fs.exists(file_id):
                image_data = self.screenshot_fs.get(file_id).read()
                image = Image.open(BytesIO(image_data))
                st.image(image, caption=caption, use_container_width=True)
                return True
            return False
        except Exception as e:
            logger.error(f"Error displaying thumbnail: {e}")
            return False

    def display_video(self, gridfs_file_id, caption: str = ""):
        try:
            file_id = ObjectId(gridfs_file_id) if isinstance(gridfs_file_id, str) else gridfs_file_id
            if not self.video_fs.exists(file_id):
                st.warning(f"Video not found in GridFS: {gridfs_file_id}")
                return False

            video_file    = self.video_fs.get(file_id)
            file_size_mb  = video_file.length / (1024 * 1024)

            if caption:
                st.markdown(f"**{caption}**")
            st.caption(f"Size: {file_size_mb:.2f} MB")

            video_data = video_file.read()

            if file_size_mb > VIDEO_INLINE_STREAM_LIMIT_MB:
                st.warning(
                    f"⚠️ This video is **{file_size_mb:.0f} MB** — too large to stream inline "
                    f"(limit: {VIDEO_INLINE_STREAM_LIMIT_MB} MB). "
                    f"Use the download button below to watch it locally."
                )
                st.info(
                    "💡 Tip: If you keep hitting this limit, raise `VIDEO_INLINE_STREAM_LIMIT_MB` "
                    "in `dashboard_page.py`, or reduce recording CRF / resolution in `VideoRecorder.js`."
                )
            else:
                st.video(video_data, format='video/mp4')

            st.download_button(
                label="📥 Download Video",
                data=video_data,
                file_name=f"recording_{gridfs_file_id}.mp4",
                mime="video/mp4",
                key=f"download_video_{gridfs_file_id}"
            )
            return True

        except Exception as e:
            logger.error(f"Error displaying video: {e}")
            st.error(f"Failed to load video: {str(e)}")
            return False

    def get_video_info(self, gridfs_file_id) -> Optional[Dict]:
        try:
            file_id = ObjectId(gridfs_file_id) if isinstance(gridfs_file_id, str) else gridfs_file_id
            if not self.video_fs.exists(file_id):
                return None
            video_file = self.video_fs.get(file_id)
            return {
                'filename': video_file.filename,
                'length': video_file.length,
                'upload_date': video_file.upload_date,
                'metadata': video_file.metadata
            }
        except Exception as e:
            logger.error(f"Error getting video info: {e}")
            return None

    def delete_video(self, gridfs_file_id) -> bool:
        try:
            file_id = ObjectId(gridfs_file_id) if isinstance(gridfs_file_id, str) else gridfs_file_id
            if not self.video_fs.exists(file_id):
                return False
            self.video_fs.delete(file_id)
            return True
        except Exception as e:
            logger.error(f"Error deleting video: {e}")
            return False

    def delete_screenshot(self, gridfs_file_id) -> bool:
        try:
            file_id = ObjectId(gridfs_file_id) if isinstance(gridfs_file_id, str) else gridfs_file_id
            if not self.screenshot_fs.exists(file_id):
                return False
            self.screenshot_fs.delete(file_id)
            return True
        except Exception as e:
            logger.error(f"Error deleting screenshot: {e}")
            return False

    def count_screenshots_in_gridfs(self) -> int:
        try:
            return self.mongo_db['screenshots.files'].count_documents({})
        except Exception:
            return 0

    def count_videos_in_gridfs(self) -> int:
        try:
            return self.mongo_db['video_recordings.files'].count_documents({})
        except Exception:
            return 0

    def list_recent_screenshots(self, limit: int = 20) -> List[Dict]:
        try:
            return list(
                self.mongo_db['screenshots.files']
                .find({}, {'filename': 1, 'uploadDate': 1, 'length': 1, 'metadata': 1})
                .sort('uploadDate', -1)
                .limit(limit)
            )
        except Exception:
            return []


# =============================================================================
# DashboardPage
# =============================================================================
class DashboardPage(BasePage):
    """Enhanced dashboard with global summary, media viewing, and GridFS debug."""

    def __init__(self, db_manager):
        super().__init__(db_manager)
        self.mongo_db = get_mongodb_connection()
        self.media_viewer = GridFSMediaViewer(self.mongo_db)

    # -------------------------------------------------------------------------
    # Metadata helpers
    # -------------------------------------------------------------------------
    def _delete_video_metadata(self, video_id: str) -> bool:
        try:
            result = self.mongo_db.video_recording_metadata.delete_one({'_id': ObjectId(video_id)})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error deleting video metadata: {e}")
            return False

    def _delete_screenshot_metadata(self, screenshot_id: str) -> bool:
        try:
            result = self.mongo_db.screenshot_metadata.delete_one({'_id': ObjectId(screenshot_id)})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error deleting screenshot metadata: {e}")
            return False

    def _bulk_delete_videos(self, account_id: int, date_range: Dict) -> Dict[str, int]:
        try:
            videos = self._fetch_video_recordings_from_metadata(account_id, date_range)
            deleted_count = 0
            failed_count = 0
            for video in videos:
                gridfs_id = video.get('gridfs_file_id')
                video_mongo_id = str(video.get('_id'))
                if gridfs_id:
                    gd = self.media_viewer.delete_video(gridfs_id)
                    md = self._delete_video_metadata(video_mongo_id)
                    if gd and md:
                        deleted_count += 1
                    else:
                        failed_count += 1
            return {'deleted': deleted_count, 'failed': failed_count, 'total': len(videos)}
        except Exception as e:
            logger.error(f"Error in bulk delete videos: {e}")
            return {'deleted': 0, 'failed': 0, 'total': 0, 'error': str(e)}

    def _bulk_delete_screenshots(self, account_id: int, date_range: Dict, categories: List[str]) -> Dict[str, int]:
        try:
            screenshots = self._fetch_screenshots(account_id, date_range, categories)
            deleted_count = 0
            failed_count = 0
            for screenshot in screenshots:
                gridfs_id = screenshot.get('gridfs_file_id')
                screenshot_mongo_id = str(screenshot.get('_id'))
                if gridfs_id:
                    gd = self.media_viewer.delete_screenshot(gridfs_id)
                    md = self._delete_screenshot_metadata(screenshot_mongo_id)
                    if gd and md:
                        deleted_count += 1
                    else:
                        failed_count += 1
            return {'deleted': deleted_count, 'failed': failed_count, 'total': len(screenshots)}
        except Exception as e:
            logger.error(f"Error in bulk delete screenshots: {e}")
            return {'deleted': 0, 'failed': 0, 'total': 0, 'error': str(e)}

    # -------------------------------------------------------------------------
    # GridFS Debug Panel
    # -------------------------------------------------------------------------
    def _render_gridfs_debug_panel(self):
        with st.expander("🔬 GridFS Debug Panel — Is data actually arriving?", expanded=False):
            st.markdown("Use this to confirm whether screenshots are reaching the database after a workflow run.")
            st.markdown("---")

            col1, col2, col3, col4 = st.columns(4)

            gridfs_screenshot_count = self.media_viewer.count_screenshots_in_gridfs()
            gridfs_video_count = self.media_viewer.count_videos_in_gridfs()

            try:
                meta_screenshot_count = self.mongo_db.screenshot_metadata.count_documents({})
            except Exception:
                meta_screenshot_count = -1

            try:
                meta_video_count = self.mongo_db.video_recording_metadata.count_documents({})
            except Exception:
                meta_video_count = -1

            with col1:
                st.metric("GridFS Screenshots (raw)", gridfs_screenshot_count)
                st.caption("Files in screenshots.files")
            with col2:
                st.metric("Metadata Screenshots", meta_screenshot_count)
                st.caption("Docs in screenshot_metadata")
            with col3:
                st.metric("GridFS Videos (raw)", gridfs_video_count)
                st.caption("Files in video_recordings.files")
            with col4:
                st.metric("Metadata Videos", meta_video_count)
                st.caption("Docs in video_recording_metadata")

            if gridfs_screenshot_count > 0 and meta_screenshot_count == 0:
                st.error(
                    "⚠️ **Mismatch detected:** Screenshots exist in GridFS but `screenshot_metadata` is empty. "
                    "The `ScreenshotCapture.createScreenshotMetadata()` step is likely failing silently."
                )
            elif gridfs_screenshot_count == 0 and meta_screenshot_count == 0:
                st.warning("📭 **No screenshots in GridFS yet.**")
            elif gridfs_screenshot_count == meta_screenshot_count and gridfs_screenshot_count > 0:
                st.success(f"✅ GridFS and metadata counts match ({gridfs_screenshot_count}). Pipeline looks healthy.")
            elif gridfs_screenshot_count != meta_screenshot_count:
                st.warning(
                    f"⚠️ Count mismatch: {gridfs_screenshot_count} GridFS files vs "
                    f"{meta_screenshot_count} metadata docs."
                )

            st.markdown("---")
            st.markdown("**Most Recent Screenshot Files in GridFS**")

            recent_files = self.media_viewer.list_recent_screenshots(limit=10)
            if recent_files:
                rows = []
                for f in recent_files:
                    upload_date = f.get('uploadDate', '')
                    if isinstance(upload_date, datetime):
                        upload_date = upload_date.strftime('%Y-%m-%d %H:%M:%S')
                    rows.append({
                        'Filename':    f.get('filename', '—'),
                        'Size (KB)':   round(f.get('length', 0) / 1024, 1),
                        'Uploaded':    upload_date,
                        'GridFS ID':   str(f.get('_id', '—')),
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                st.markdown("**Preview of most recent screenshot:**")
                latest_id = str(recent_files[0].get('_id', ''))
                if latest_id:
                    self.media_viewer.display_screenshot(latest_id)
            else:
                st.info("No files in screenshots.files GridFS bucket.")

            st.markdown("---")
            st.markdown("**Raw MongoDB collections available:**")
            try:
                collections = sorted(self.mongo_db.list_collection_names())
                cols = st.columns(3)
                for i, c in enumerate(collections):
                    cols[i % 3].caption(f"• {c}")
            except Exception as e:
                st.caption(f"Could not list collections: {e}")

    # -------------------------------------------------------------------------
    # Main render
    # -------------------------------------------------------------------------
    def render(self):
        try:
            st.set_page_config(
                page_title="Account Media Dashboard",
                layout="wide",
                initial_sidebar_state="expanded"
            )
        except st.errors.StreamlitAPIException:
            pass

        st.title("🎯 Account Media Dashboard")
        st.markdown("View and manage screenshots and videos across all accounts")
        st.markdown("---")

        self._render_gridfs_debug_panel()
        st.markdown("---")

        st.subheader("📊 View Mode")
        col1, col2, col3 = st.columns([2, 2, 1])

        with col1:
            view_mode = st.radio(
                "Select View:",
                ["All Accounts Summary", "Single Account Detail"],
                key="view_mode",
                horizontal=True
            )

        with col2:
            selected_account = None
            if view_mode == "Single Account Detail":
                accounts = pg_accounts.get_all_active_accounts()
                if accounts:
                    account_options = {
                        f"{acc['username']} (ID: {acc['account_id']})": acc for acc in accounts
                    }
                    selected = st.selectbox(
                        "Select Account:",
                        options=list(account_options.keys()),
                        key="main_account_selector"
                    )
                    selected_account = account_options[selected] if selected else None
                else:
                    st.warning("No accounts found")
            else:
                st.info("Switch to 'Single Account Detail' to select an account")

        with col3:
            st.write("")
            if st.button("🔄 Refresh Data", use_container_width=True):
                st.rerun()

        st.markdown("---")

        st.subheader("🔍 Filters")
        filter_col1, filter_col2 = st.columns([1, 2])
        with filter_col1:
            date_range = self._render_date_filter()
        with filter_col2:
            media_filters = self._render_media_filters()

        st.markdown("---")

        if view_mode == "All Accounts Summary":
            self._render_global_summary()
            st.markdown("---")
            self._render_all_accounts_view(date_range, media_filters)
        elif selected_account:
            self._render_single_account_view(selected_account, date_range, media_filters)
        else:
            st.info("👆 Please select an account above to view details")

    # -------------------------------------------------------------------------
    # Global Summary
    # -------------------------------------------------------------------------
    def _render_global_summary(self):
        st.header("Global Summary")
        try:
            all_accounts = pg_accounts.get_all_active_accounts()
            total_accounts = len(all_accounts)

            total_sessions = self.mongo_db.execution_sessions.count_documents({})

            wf_agg = list(self.mongo_db.execution_sessions.aggregate([
                {'$group': {'_id': None, 'total': {'$sum': '$workflows_executed'}}}
            ]))
            total_workflows = wf_agg[0]['total'] if wf_agg else 0

            total_videos = self.mongo_db.video_recording_metadata.count_documents({})
            total_screenshots = self.mongo_db.screenshot_metadata.count_documents({})

            last_24h = datetime.now() - timedelta(hours=24)
            recent_sessions = self.mongo_db.execution_sessions.count_documents({
                'created_at': {'$gte': last_24h}
            })

            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("Total Accounts", total_accounts)
                accounts_with_profiles = sum(1 for acc in all_accounts if acc.get('profile_id'))
                st.caption(f"{accounts_with_profiles} with profiles")
            with col2:
                st.metric("Total Sessions", total_sessions)
                st.caption(f"{recent_sessions} in last 24h")
            with col3:
                st.metric("Total Workflows", total_workflows)
                avg_per_session = total_workflows / total_sessions if total_sessions > 0 else 0
                st.caption(f"{avg_per_session:.1f} avg/session")
            with col4:
                st.metric("Total Videos", total_videos)
                video_size_gb = self._get_total_video_storage()
                st.caption(f"{video_size_gb:.2f} GB storage")
            with col5:
                st.metric("Total Screenshots", total_screenshots)
                screenshot_size_mb = self._get_total_screenshot_storage()
                st.caption(f"{screenshot_size_mb:.0f} MB storage")

            st.markdown("#### Recent Activity (Last 7 Days)")
            activity_df = self._get_activity_timeline(days=7)
            if not activity_df.empty:
                st.line_chart(activity_df)
            else:
                st.info("No activity in the last 7 days")

        except Exception as e:
            st.error(f"Error loading global summary: {str(e)}")
            logger.error(f"Global summary error: {e}", exc_info=True)

    # -------------------------------------------------------------------------
    # Storage helpers
    # -------------------------------------------------------------------------
    def _get_total_video_storage(self) -> float:
        try:
            result = list(self.mongo_db['video_recordings.files'].aggregate([
                {'$group': {'_id': None, 'total_size': {'$sum': '$length'}}}
            ]))
            return result[0].get('total_size', 0) / (1024 ** 3) if result else 0.0
        except Exception:
            return 0.0

    def _get_total_screenshot_storage(self) -> float:
        try:
            result = list(self.mongo_db.screenshot_metadata.aggregate([
                {'$group': {'_id': None, 'total_size': {'$sum': '$size'}}}
            ]))
            return result[0].get('total_size', 0) / (1024 * 1024) if result else 0.0
        except Exception:
            return 0.0

    def _get_activity_timeline(self, days: int = 7) -> pd.DataFrame:
        try:
            start_date = datetime.now() - timedelta(days=days)
            pipeline = [
                {'$match': {'created_at': {'$gte': start_date}}},
                {'$group': {
                    '_id': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$created_at'}},
                    'sessions': {'$sum': 1},
                    'workflows': {'$sum': '$workflows_executed'}
                }},
                {'$sort': {'_id': 1}}
            ]
            results = list(self.mongo_db.execution_sessions.aggregate(pipeline))
            if results:
                df = pd.DataFrame(results)
                df.columns = ['date', 'Sessions', 'Workflows']
                return df.set_index('date')
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error getting activity timeline: {e}")
            return pd.DataFrame()

    # -------------------------------------------------------------------------
    # All Accounts View
    # -------------------------------------------------------------------------
    def _render_all_accounts_view(self, date_range: Dict, filters: Dict):
        st.header("All Accounts Overview")
        accounts = pg_accounts.get_all_active_accounts()
        if not accounts:
            st.warning("No accounts found")
            return

        accounts_data = []
        for acc in accounts:
            stats = self._get_account_stats(acc['account_id'], date_range)
            profile_id = acc.get('profile_id', '')
            accounts_data.append({
                'Account ID': acc['account_id'],
                'Username': acc['username'],
                'Profile ID': (profile_id[:8] + '...') if profile_id else 'Not linked',
                'Sessions': stats.get('total_sessions', 0),
                'Workflows': stats.get('total_workflows', 0),
                'Success Rate': f"{stats.get('success_rate', 0):.1f}%",
                'Videos': stats.get('total_videos', 0),
                'Screenshots': stats.get('total_screenshots', 0),
                'Last Activity': stats.get('last_activity', 'Never')
            })

        df = pd.DataFrame(accounts_data)

        col1, col2, col3 = st.columns(3)
        with col1:
            active_accounts = df[df['Sessions'] > 0].shape[0]
            st.metric("Active Accounts", active_accounts)
        with col2:
            total_workflows = int(df['Workflows'].sum())
            st.metric("Total Workflows", total_workflows)
        with col3:
            try:
                avg_success_rate = df['Success Rate'].str.rstrip('%').astype(float).mean()
            except (ValueError, AttributeError):
                avg_success_rate = 0.0
            st.metric("Avg Success Rate", f"{avg_success_rate:.1f}%")

        st.markdown("---")
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.markdown("#### Top Performers")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Most Workflows**")
            top_workflows = df.nlargest(5, 'Workflows')[['Username', 'Workflows']]
            st.dataframe(top_workflows, hide_index=True)
        with col2:
            st.markdown("**Best Success Rate**")
            try:
                df['Success Rate Numeric'] = df['Success Rate'].str.rstrip('%').astype(float)
                top_success = df.nlargest(5, 'Success Rate Numeric')[['Username', 'Success Rate']]
                st.dataframe(top_success, hide_index=True)
            except Exception:
                st.info("No success rate data available yet")

    def _get_account_stats(self, account_id: int, date_range: Dict) -> Dict[str, Any]:
        try:
            query = {
                'postgres_account_id': account_id,
                'created_at': {'$gte': date_range['start'], '$lte': date_range['end']}
            }
            pipeline = [
                {'$match': query},
                {'$group': {
                    '_id': None,
                    'total_sessions': {'$sum': 1},
                    'total_workflows': {'$sum': '$workflows_executed'},
                    'successful_workflows': {'$sum': '$successful_workflows'},
                    'last_activity': {'$max': '$created_at'}
                }}
            ]
            result = list(self.mongo_db.execution_sessions.aggregate(pipeline))
            videos_count = self.mongo_db.video_recording_metadata.count_documents({
                'postgres_account_id': account_id,
                'created_at': {'$gte': date_range['start'], '$lte': date_range['end']}
            })
            screenshots_count = self.mongo_db.screenshot_metadata.count_documents({
                'postgres_account_id': account_id,
                'created_at': {'$gte': date_range['start'], '$lte': date_range['end']}
            })
            if result:
                stats = result[0]
                total_wf = stats.get('total_workflows', 0)
                success_wf = stats.get('successful_workflows', 0)
                success_rate = (success_wf / total_wf * 100) if total_wf > 0 else 0
                last_activity = stats.get('last_activity')
                if isinstance(last_activity, datetime):
                    last_activity = last_activity.strftime('%Y-%m-%d %H:%M')
                return {
                    'total_sessions': stats.get('total_sessions', 0),
                    'total_workflows': total_wf,
                    'success_rate': success_rate,
                    'total_videos': videos_count,
                    'total_screenshots': screenshots_count,
                    'last_activity': last_activity
                }
            return {
                'total_sessions': 0, 'total_workflows': 0, 'success_rate': 0,
                'total_videos': videos_count, 'total_screenshots': screenshots_count,
                'last_activity': 'Never'
            }
        except Exception as e:
            logger.error(f"Error getting account stats: {e}")
            return {}

    # -------------------------------------------------------------------------
    # Single Account View
    # -------------------------------------------------------------------------
    def _render_single_account_view(self, account: Dict[str, Any], date_range: Dict, filters: Dict):
        st.header(f"Account: {account['username']}")
        self._render_account_profile_overview(account)
        st.markdown("---")
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "Screenshots", "Video Recordings", "Execution Sessions", "Profile Details", "Automa Logs"
        ])
        with tab1:
            self._render_screenshots_view(account, date_range, filters)
        with tab2:
            self._render_videos_view(account, date_range, filters)
        with tab3:
            self._render_execution_sessions(account, date_range)
        with tab4:
            self._render_profile_details(account)
        with tab5:
            self._render_automa_logs_view(account, date_range)

    # -------------------------------------------------------------------------
    # Automa Logs View
    # -------------------------------------------------------------------------
    def _render_automa_logs_view(self, account: Dict[str, Any], date_range: Dict):
        st.subheader("Automa Execution Logs")
        
        # Fetch logs from automa_logs collection
        logs = self._fetch_automa_logs(account['account_id'], date_range)
        
        if not logs:
            st.info("No Automa logs found for this account in the selected date range")
            return
            
        st.success(f"Found {len(logs)} execution log(s)")
        
        for log in logs:
            with st.expander(f"Log: {log.get('workflow_name', 'Unknown')} - {log.get('created_at', 'Unknown')}", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Workflow:** {log.get('workflow_name', 'Unknown')}")
                    st.write(f"**Status:** {log.get('workflow_status', 'Unknown')}")
                with col2:
                    st.write(f"**Execution ID:** {log.get('execution_id', 'Unknown')[:30]}...")
                    st.write(f"**Created:** {log.get('created_at', 'Unknown')}")
                
                # Extract and display screenshots from the log
                logs_data = log.get('logs', {})
                if isinstance(logs_data, str):
                    try:
                        import json
                        logs_data = json.loads(logs_data)
                    except:
                        logs_data = {}
                
                # Look for screenshots in the log data
                screenshots = self._extract_screenshots_from_log(logs_data)
                
                if screenshots:
                    st.markdown("---")
                    st.markdown("#### 📸 Screenshots from this workflow")
                    
                    cols = st.columns(3)
                    for idx, screenshot in enumerate(screenshots):
                        with cols[idx % 3]:
                            st.caption(f"**{screenshot['block_name']}**")
                            if screenshot['data']:
                                self.media_viewer.display_screenshot_from_base64(
                                    screenshot['data'],
                                    caption=f"Step {idx + 1}"
                                )
                            if screenshot['url']:
                                st.caption(f"URL: {screenshot['url'][:50]}...")
                else:
                    st.info("No screenshots found in this log")
    
    def _fetch_automa_logs(self, account_id: int, date_range: Dict) -> List[Dict]:
        """Fetch Automa logs from the automa_logs collection."""
        try:
            query = {
                'postgres_account_id': account_id,
                'created_at': {'$gte': date_range['start'], '$lte': date_range['end']}
            }
            return list(self.mongo_db.automa_logs.find(query).sort('created_at', -1))
        except Exception as e:
            logger.error(f"Error fetching Automa logs: {e}")
            return []
    
    def _extract_screenshots_from_log(self, log_data: Any) -> List[Dict]:
        """Extract screenshot data from Automa log structure."""
        screenshots = []
        
        if not log_data:
            return screenshots
            
        # Handle different log structures
        if isinstance(log_data, dict):
            # Check for logs array
            logs = log_data.get('logs', [])
            for entry in logs:
                if isinstance(entry, dict):
                    # Look for screenshot data in the entry
                    if entry.get('name') == 'take-screenshot' and entry.get('data'):
                        data = entry.get('data', {})
                        screenshot_data = data.get('screenshot')
                        if screenshot_data:
                            screenshots.append({
                                'block_name': data.get('description', 'Screenshot'),
                                'data': screenshot_data,
                                'url': entry.get('activeTabUrl'),
                                'timestamp': entry.get('timestamp')
                            })
        
        return screenshots

    # -------------------------------------------------------------------------
    # Videos View
    # -------------------------------------------------------------------------
    def _render_videos_view(self, account: Dict[str, Any], date_range: Dict, filters: Dict):
        st.subheader("Video Recordings")

        if not filters.get('show_videos'):
            st.info("Videos view is disabled in filters")
            return

        st.info(
            "ℹ️ **Video recording is active in the orchestrator flow.** "
            "Videos are captured for the entire execution duration and saved when workflows complete."
        )

        videos = self._fetch_video_recordings_from_metadata(account['account_id'], date_range)

        if not videos:
            st.caption(f"No video recordings found (account {account['account_id']}, "
                       f"{date_range['start'].date()} → {date_range['end'].date()})")
            return

        st.markdown("---")
        col_bulk1, col_bulk2 = st.columns([4, 1])
        with col_bulk1:
            st.caption(f"Found {len(videos)} video(s) in selected date range")
        with col_bulk2:
            if st.button("🗑️ Delete ALL Videos", key=f"bulk_delete_videos_{account['account_id']}", type="primary"):
                with st.spinner(f"Deleting {len(videos)} videos..."):
                    result = self._bulk_delete_videos(account['account_id'], date_range)
                    if result['deleted'] > 0:
                        st.success(f"✅ Deleted {result['deleted']}/{result['total']} videos successfully!")
                        if result['failed'] > 0:
                            st.warning(f"⚠️ Failed to delete {result['failed']} videos")
                        st.rerun()
                    elif 'error' in result:
                        st.error(f"❌ Error: {result['error']}")
                    else:
                        st.error("❌ Failed to delete videos")
        st.markdown("---")

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Videos", len(videos))
        completed_videos = sum(1 for v in videos if v.get('recording_status') == 'completed')
        col2.metric("Completed", completed_videos)
        total_duration = sum(v.get('duration_seconds', 0) for v in videos)
        col3.metric("Total Duration", f"{total_duration / 60:.1f} min")
        total_size = sum(
            (self.media_viewer.get_video_info(v['gridfs_file_id']) or {}).get('length', 0)
            for v in videos if v.get('gridfs_file_id')
        )
        col4.metric("Total Size", f"{total_size / (1024 * 1024):.1f} MB")
        st.markdown("---")

        for idx, video in enumerate(videos):
            session_id    = video.get('session_id', 'Unknown')
            status        = video.get('recording_status', 'unknown')
            video_mongo_id = str(video.get('_id'))
            status_emoji  = {'completed': '✅', 'in_progress': '⏳', 'failed': '❌', 'pending': '⏸️'}.get(status, '❓')
            workflow_name = video.get('workflow_name', 'Unknown')
            created_at    = video.get('created_at', 'Unknown')
            if isinstance(created_at, datetime):
                created_at = created_at.strftime('%Y-%m-%d %H:%M:%S')

            with st.expander(f"{status_emoji} Video {idx + 1}: {workflow_name} - {created_at}", expanded=(idx == 0)):
                col1, col2 = st.columns([3, 1])

                with col1:
                    gridfs_file_id = video.get('gridfs_file_id')
                    if gridfs_file_id and status == 'completed':
                        st.markdown("#### 🎥 Video Recording")
                        success = self.media_viewer.display_video(gridfs_file_id, caption=f"Session: {session_id}")
                        if not success:
                            st.error("Failed to load video from GridFS")

                        # ── ⏱ Timing panel — pull from the linked execution session ──
                        session_doc = self._get_session_info(session_id)
                        if session_doc:
                            st.markdown("---")
                            st.markdown("#### ⏱ Orchestrator Wait Timing")
                            t = _timing_from_session(session_doc)
                            _render_timing_metrics(
                                estimated_wait_ms=t['estimated_wait_ms'],
                                configured_wait_ms=t['configured_wait_ms'],
                                actual_wait_ms=t['actual_wait_ms'],
                                wait_margin_ms=t['wait_margin_ms'],
                                estimation_method=t['estimation_method'],
                                compact=False,
                            )

                        st.markdown("---")
                        _, delete_col = st.columns([3, 1])
                        with delete_col:
                            if st.button("🗑️ Delete Video", key=f"delete_video_{video_mongo_id}", type="secondary"):
                                with st.spinner("Deleting video..."):
                                    gd = self.media_viewer.delete_video(gridfs_file_id)
                                    md = self._delete_video_metadata(video_mongo_id)
                                    if gd and md:
                                        st.success("✅ Video deleted successfully!")
                                        st.rerun()
                                    else:
                                        st.error("❌ Partial or full delete failure")

                    elif status == 'in_progress':
                        st.info("🎬 Video recording still processing...")
                    elif status == 'failed':
                        st.error(f"❌ Recording failed: {video.get('error_message', 'Unknown error')}")
                    else:
                        st.warning("No video file available")

                with col2:
                    st.markdown("#### 📊 Details")
                    st.metric("Status",   status)
                    st.metric("Duration", f"{video.get('duration_seconds', 0):.1f}s")
                    st.metric("Workflow", video.get('workflow_type', 'N/A'))
                    st.metric("Link ID",  video.get('link_id', 'N/A'))
                    st.markdown("---")
                    st.markdown("**Session Info**")
                    st.caption(f"Session: {str(session_id)[:20]}...")
                    st.caption(f"Account: {video.get('account_username', 'N/A')}")
                    st.caption(f"Method: {video.get('recording_method', 'N/A')}")

                st.markdown("---")
                st.markdown("#### 📸 Related Screenshots")
                session_screenshots = self._fetch_screenshots_by_session(session_id)
                if session_screenshots:
                    cols = st.columns(min(3, len(session_screenshots)))
                    for ss_idx, ss in enumerate(session_screenshots[:6]):
                        with cols[ss_idx % 3]:
                            self._render_screenshot_with_image(ss)
                    if len(session_screenshots) > 6:
                        st.info(f"+ {len(session_screenshots) - 6} more screenshots")
                else:
                    st.info("No screenshots linked to this session")

    def _fetch_video_recordings_from_metadata(self, account_id: int, date_range: Dict) -> List[Dict]:
        try:
            query = {
                'postgres_account_id': account_id,
                'created_at': {'$gte': date_range['start'], '$lte': date_range['end']}
            }
            return list(self.mongo_db.video_recording_metadata.find(query).sort('created_at', -1))
        except Exception as e:
            logger.error(f"Error fetching video recordings: {e}")
            st.error(f"Error fetching videos: {str(e)}")
            return []

    def _fetch_screenshots_by_session(self, session_id: str) -> List[Dict]:
        try:
            return list(
                self.mongo_db.screenshot_metadata
                .find({'session_id': session_id})
                .sort('created_at', 1)
            )
        except Exception as e:
            logger.error(f"Error fetching screenshots for session: {e}")
            return []

    # -------------------------------------------------------------------------
    # Filters
    # -------------------------------------------------------------------------
    def _render_date_filter(self) -> Dict[str, datetime]:
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("From", value=datetime.now() - timedelta(days=7), key="filter_start_date")
        with col2:
            end_date = st.date_input("To", value=datetime.now(), key="filter_end_date")
        return {
            'start': datetime.combine(start_date, datetime.min.time()),
            'end': datetime.combine(end_date, datetime.max.time())
        }

    def _render_media_filters(self) -> Dict[str, Any]:
        filters = {}
        filters['show_screenshots'] = st.checkbox("Show Screenshots", value=True)
        filters['show_videos'] = st.checkbox("Show Videos", value=True)
        filters['screenshot_category'] = st.multiselect(
            "Screenshot Categories",
            options=['reply', 'retweet', 'message', 'errors', 'debug', 'automa_screenshot'],
            default=['reply', 'retweet', 'message', 'automa_screenshot'],
            help="Screenshots from Automa workflow blocks"
        )
        return filters

    # -------------------------------------------------------------------------
    # Account profile overview
    # -------------------------------------------------------------------------
    def _render_account_profile_overview(self, account: Dict[str, Any]):
        profile_info = self._get_profile_info(account['account_id'])
        col1, col2, col3, col4 = st.columns(4)
        profile_id = account.get('profile_id', '')
        with col1:
            st.metric("Account ID",  account['account_id'])
            st.metric("Profile ID",  (profile_id[:12] + '...') if profile_id else 'Not linked')
        with col2:
            st.metric("Total Sessions",   profile_info.get('total_sessions', 0))
            st.metric("Total Workflows",  profile_info.get('total_workflows', 0))
        with col3:
            st.metric("Total Screenshots", profile_info.get('total_screenshots', 0))
            st.metric("Total Videos",      profile_info.get('total_videos', 0))
        with col4:
            last_sync = account.get('last_workflow_sync', 'Never')
            if isinstance(last_sync, datetime):
                last_sync = last_sync.strftime('%Y-%m-%d %H:%M')
            st.metric("Last Activity", last_sync)

    def _get_profile_info(self, account_id: int) -> Dict[str, Any]:
        try:
            sessions_count = self.mongo_db.execution_sessions.count_documents({'postgres_account_id': account_id})
            pipeline = [
                {'$match': {'postgres_account_id': account_id}},
                {'$group': {
                    '_id': None,
                    'total_workflows': {'$sum': '$workflows_executed'},
                    'total_screenshots': {'$sum': {'$size': {'$ifNull': ['$screenshots', []]}}}
                }}
            ]
            result = list(self.mongo_db.execution_sessions.aggregate(pipeline))
            videos_count = self.mongo_db.video_recording_metadata.count_documents({'postgres_account_id': account_id})
            if result:
                return {
                    'total_sessions': sessions_count,
                    'total_workflows': result[0].get('total_workflows', 0),
                    'total_screenshots': result[0].get('total_screenshots', 0),
                    'total_videos': videos_count
                }
            return {
                'total_sessions': sessions_count, 'total_workflows': 0,
                'total_screenshots': 0, 'total_videos': videos_count
            }
        except Exception as e:
            st.error(f"Error fetching profile info: {str(e)}")
            return {}

    # -------------------------------------------------------------------------
    # Screenshots View
    # -------------------------------------------------------------------------
    def _fetch_screenshots(self, account_id: int, date_range: Dict, categories: List[str]) -> List[Dict]:
        try:
            query = {
                'postgres_account_id': account_id,
                'created_at': {'$gte': date_range['start'], '$lte': date_range['end']}
            }
            if categories:
                query['category'] = {'$in': categories}
            logger.info(f"📸 Fetching screenshots with query: {query}")
            screenshots = list(self.mongo_db.screenshot_metadata.find(query).sort('created_at', -1))
            logger.info(f"✅ Found {len(screenshots)} screenshots for account {account_id}")
            return screenshots
        except Exception as e:
            logger.error(f"❌ Error fetching screenshots: {str(e)}", exc_info=True)
            st.error(f"Error fetching screenshots: {str(e)}")
            return []

    def _render_screenshots_view(self, account: Dict[str, Any], date_range: Dict, filters: Dict):
        st.subheader("Screenshots Gallery")

        if not filters.get('show_screenshots'):
            st.info("Screenshots view is disabled in filters")
            return

        with st.expander("🔍 Debug Info - Current Filters", expanded=False):
            st.json({
                'account_id': account['account_id'],
                'date_range': {'start': date_range['start'].isoformat(), 'end': date_range['end'].isoformat()},
                'categories': filters.get('screenshot_category', [])
            })

        screenshots = self._fetch_screenshots(
            account['account_id'],
            date_range,
            filters.get('screenshot_category', [])
        )

        if not screenshots:
            st.warning("No screenshots found for selected filters")

            all_screenshots_count = self.mongo_db.screenshot_metadata.count_documents({
                'postgres_account_id': account['account_id']
            })

            if all_screenshots_count > 0:
                st.info(f"💡 Found {all_screenshots_count} total screenshots for this account (outside current filters)")

                categories_result = list(self.mongo_db.screenshot_metadata.aggregate([
                    {'$match': {'postgres_account_id': account['account_id']}},
                    {'$group': {'_id': '$category', 'count': {'$sum': 1}}}
                ]))
                if categories_result:
                    st.markdown("**Available categories in database:**")
                    for cat in categories_result:
                        st.write(f"- `{cat['_id']}`: {cat['count']} screenshots")
                    st.info("💡 Try adjusting the 'Screenshot Categories' filter above")

                date_result = list(self.mongo_db.screenshot_metadata.aggregate([
                    {'$match': {'postgres_account_id': account['account_id']}},
                    {'$group': {'_id': None, 'oldest': {'$min': '$created_at'}, 'newest': {'$max': '$created_at'}}}
                ]))
                if date_result and date_result[0]:
                    oldest = date_result[0].get('oldest')
                    newest = date_result[0].get('newest')
                    if oldest and newest:
                        st.markdown("**Available date range:**")
                        st.write(f"- Oldest: {oldest.strftime('%Y-%m-%d %H:%M:%S')}")
                        st.write(f"- Newest: {newest.strftime('%Y-%m-%d %H:%M:%S')}")
                        if oldest > date_range['end'] or newest < date_range['start']:
                            st.warning("⚠️ Your selected date range doesn't overlap with available screenshots!")
            else:
                st.info("No screenshots exist for this account yet")
                st.caption("Screenshots will appear here after workflow executions complete successfully")
            return

        st.markdown("---")
        col_bulk1, col_bulk2 = st.columns([4, 1])
        with col_bulk1:
            st.caption(f"Found {len(screenshots)} screenshot(s) matching filters")
        with col_bulk2:
            if st.button("🗑️ Delete ALL Screenshots", key=f"bulk_delete_screenshots_{account['account_id']}", type="primary"):
                with st.spinner(f"Deleting {len(screenshots)} screenshots..."):
                    result = self._bulk_delete_screenshots(
                        account['account_id'],
                        date_range,
                        filters.get('screenshot_category', [])
                    )
                    if result['deleted'] > 0:
                        st.success(f"✅ Deleted {result['deleted']}/{result['total']} screenshots successfully!")
                        if result['failed'] > 0:
                            st.warning(f"⚠️ Failed to delete {result['failed']} screenshots")
                        st.rerun()
                    elif 'error' in result:
                        st.error(f"❌ Error: {result['error']}")
                    else:
                        st.error("❌ Failed to delete screenshots")
        st.markdown("---")

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Screenshots", len(screenshots))
        categories_found = {}
        for ss in screenshots:
            cat = ss.get('category', 'unknown')
            categories_found[cat] = categories_found.get(cat, 0) + 1
        col2.metric("Categories", len(categories_found))
        today = datetime.now().date()
        today_screenshots = sum(
            1 for ss in screenshots
            if isinstance(ss.get('created_at'), datetime) and ss['created_at'].date() == today
        )
        col3.metric("Today's Screenshots", today_screenshots)
        st.markdown("---")

        sessions = {}
        for ss in screenshots:
            sessions.setdefault(ss.get('session_id', 'unknown'), []).append(ss)

        for session_id, session_screenshots in sessions.items():
            with st.expander(f"Session: {session_id} ({len(session_screenshots)} screenshots)", expanded=True):
                session_info = self._get_session_info(session_id)
                if session_info:
                    col1, col2, col3 = st.columns(3)
                    col1.write(f"**Status:** {session_info.get('session_status', 'N/A')}")
                    col2.write(f"**Workflows:** {session_info.get('workflows_executed', 0)}")
                    col3.write(f"**Day:** {session_info.get('execution_day', 'N/A')}")
                cols = st.columns(3)
                for idx, ss in enumerate(session_screenshots):
                    with cols[idx % 3]:
                        self._render_screenshot_with_image(ss)

    def _render_screenshot_with_image(self, screenshot: Dict[str, Any]):
        gridfs_id = screenshot.get('gridfs_file_id')
        screenshot_mongo_id = str(screenshot.get('_id'))

        if not gridfs_id:
            st.warning("No GridFS ID available")
            st.caption(f"Filename: {screenshot.get('filename', 'N/A')}")
            return

        try:
            file_id = ObjectId(gridfs_id) if isinstance(gridfs_id, str) else gridfs_id

            if not self.media_viewer.screenshot_fs.exists(file_id):
                st.error("❌ File not found in GridFS")
                st.caption(f"ID: {gridfs_id}")
                st.caption(f"Filename: {screenshot.get('filename', 'N/A')}")
                return

            category = screenshot.get('category', 'N/A')
            filename = screenshot.get('filename', 'N/A')
            success = self.media_viewer.display_thumbnail(gridfs_id, caption=f"{category} - {filename}")

            if success:
                st.caption(f"Size: {screenshot.get('size', 0) / 1024:.2f} KB")
                created_at = screenshot.get('created_at', 'N/A')
                if isinstance(created_at, datetime):
                    created_at = created_at.strftime('%Y-%m-%d %H:%M:%S')
                st.caption(f"Created: {created_at}")
                st.caption(f"Source: {screenshot.get('captured_by', 'N/A')}")

                if st.button("🗑️ Delete", key=f"delete_screenshot_{screenshot_mongo_id}", type="secondary", use_container_width=True):
                    with st.spinner("Deleting..."):
                        gd = self.media_viewer.delete_screenshot(gridfs_id)
                        md = self._delete_screenshot_metadata(screenshot_mongo_id)
                        if gd and md:
                            st.success("✅ Deleted!")
                            st.rerun()
                        elif gd:
                            st.warning("⚠️ File deleted but metadata removal failed")
                        elif md:
                            st.warning("⚠️ Metadata deleted but file removal failed")
                        else:
                            st.error("❌ Failed to delete")
            else:
                st.error("Failed to display screenshot")

        except Exception as e:
            st.error(f"Error rendering screenshot: {str(e)}")
            logger.error(f"Screenshot rendering error: {e}", exc_info=True)
            with st.expander("Debug Info"):
                st.json({
                    'gridfs_id': str(gridfs_id),
                    'filename': screenshot.get('filename'),
                    'category': screenshot.get('category'),
                    'session_id': screenshot.get('session_id'),
                    'error': str(e)
                })

    # -------------------------------------------------------------------------
    # Execution Sessions
    # -------------------------------------------------------------------------
    def _render_execution_sessions(self, account: Dict[str, Any], date_range: Dict):
        st.subheader("Execution Sessions Timeline")
        sessions = self._fetch_execution_sessions(account['account_id'], date_range)
        if not sessions:
            st.info("No execution sessions found for selected date range")
            return

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Sessions", len(sessions))
        successful = sum(1 for s in sessions if s.get('session_status') == 'completed')
        col2.metric("Successful", successful)
        total_workflows = sum(s.get('workflows_executed', 0) for s in sessions)
        col3.metric("Total Workflows", total_workflows)
        avg_duration = sum(s.get('total_execution_time_seconds', 0) for s in sessions) / len(sessions)
        col4.metric("Avg Duration", f"{avg_duration / 60:.1f} min")
        st.markdown("---")

        for session in sessions:
            status_icon = {'completed': '✅', 'active': '⏳', 'failed': '❌', 'timeout': '⏸️'}.get(
                session.get('session_status'), '❓')
            created_label = session.get('created_at', 'N/A')
            if isinstance(created_label, datetime):
                created_label = created_label.strftime('%Y-%m-%d %H:%M:%S')

            with st.expander(
                f"{status_icon} {session.get('session_id', 'Unknown')} — {created_label}",
                expanded=False,
            ):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write("**Session Info**")
                    st.write(f"Status: {session.get('session_status', 'N/A')}")
                    st.write(f"Day: {session.get('execution_day', 'N/A')}")
                    st.write(f"Time: {session.get('execution_time', 'N/A')}")
                with col2:
                    st.write("**Execution**")
                    st.write(f"Workflows: {session.get('workflows_executed', 0)}")
                    st.write(f"Successful: {session.get('successful_workflows', 0)}")
                    st.write(f"Failed: {session.get('failed_workflows', 0)}")
                with col3:
                    st.write("**Media**")
                    st.write(f"Screenshots: {len(session.get('screenshots', []))}")
                    st.write(f"Video: {'Yes' if session.get('video_recording_id') else 'No'}")
                    st.write(f"Duration: {session.get('total_execution_time_seconds', 0) / 60:.1f} min")

                # ── ⏱ Timing panel ─────────────────────────────────────────
                t = _timing_from_session(session)
                if any(v is not None for v in [t['estimated_wait_ms'], t['configured_wait_ms'], t['actual_wait_ms']]):
                    st.markdown("---")
                    st.markdown("##### ⏱ Orchestrator Wait Timing")
                    _render_timing_metrics(
                        estimated_wait_ms=t['estimated_wait_ms'],
                        configured_wait_ms=t['configured_wait_ms'],
                        actual_wait_ms=t['actual_wait_ms'],
                        wait_margin_ms=t['wait_margin_ms'],
                        estimation_method=t['estimation_method'],
                        compact=False,
                    )

                    # Per-workflow breakdown (inside a nested expander to keep it tidy)
                    breakdown = session.get('timing_breakdown', {})
                    per_wf    = breakdown.get('per_workflow_analysis')
                    if per_wf:
                        with st.expander("📋 Per-workflow timing breakdown", expanded=False):
                            rows = []
                            for wf in per_wf:
                                ms = wf.get('estimatedMs', 0)
                                bd = wf.get('breakdown', {})
                                rows.append({
                                    'Workflow':     wf.get('name', wf.get('docId', '—')),
                                    'Estimated':    f"{ms / 1000:.1f}s",
                                    'Delay blocks': f"{bd.get('delay_ms', 0) / 1000:.1f}s",
                                    'Click waits':  f"{bd.get('click_ms', 0) / 1000:.1f}s",
                                    'Other':        f"{bd.get('other_ms', 0) / 1000:.1f}s",
                                    'Nodes':        bd.get('node_count', '—'),
                                })
                            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                            delay_range = breakdown.get('delay_range', {})
                            if delay_range:
                                st.caption(
                                    f"Inter-workflow delay range: "
                                    f"{delay_range.get('min_milliseconds', 0) / 1000:.0f}s – "
                                    f"{delay_range.get('max_milliseconds', 0) / 1000:.0f}s"
                                )

    # -------------------------------------------------------------------------
    # Profile Details
    # -------------------------------------------------------------------------
    def _render_profile_details(self, account: Dict[str, Any]):
        st.subheader("Profile Details & Complete Media Linkage")
        profile_id = account.get('profile_id')
        if not profile_id:
            st.warning("No profile linked to this account")
            return

        profile_stats = self._get_detailed_profile_stats(account['account_id'], profile_id)
        st.markdown("#### Linkage Hierarchy")
        st.code("""
PostgreSQL Account (account_id)
        ↓
Chrome Profile (profile_id)
        ↓
Execution Sessions (session_id)
        ↓
┌──────────────────┬──────────────────┐
↓                  ↓                  ↓
Screenshots       Automa Logs       Workflow Data
(GridFS)          (MongoDB)         (MongoDB)
        """, language="text")
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("**PostgreSQL Layer**")
            st.info(f"Account: {account['username']}\nID: {account['account_id']}\nProfile: {profile_id[:12]}...")
        with col2:
            st.markdown("**Chrome Profile Layer**")
            st.info(f"Profile ID: {profile_id[:12]}...\nLinked: ✓")
        with col3:
            st.markdown("**MongoDB Media Layer**")
            st.info(f"Sessions: {profile_stats.get('total_sessions', 0)}\nWorkflows: {profile_stats.get('total_workflows', 0)}")
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Sessions",  profile_stats.get('total_sessions', 0))
        col2.metric("Success Rate",    f"{profile_stats.get('success_rate', 0):.1f}%")
        col3.metric("Avg Duration",    f"{profile_stats.get('avg_duration', 0):.1f} min")
        col4.metric("Total Workflows", profile_stats.get('total_workflows', 0))

    def _get_detailed_profile_stats(self, account_id: int, profile_id: str) -> Dict[str, Any]:
        try:
            pipeline = [
                {'$match': {'postgres_account_id': account_id, 'profile_id': profile_id}},
                {'$group': {
                    '_id': None,
                    'total_sessions': {'$sum': 1},
                    'total_workflows': {'$sum': '$workflows_executed'},
                    'successful_workflows': {'$sum': '$successful_workflows'},
                    'avg_duration': {'$avg': '$total_execution_time_seconds'}
                }}
            ]
            result = list(self.mongo_db.execution_sessions.aggregate(pipeline))
            if result:
                stats = result[0]
                total_wf = stats.get('total_workflows', 0)
                success_wf = stats.get('successful_workflows', 0)
                return {
                    'total_sessions': stats.get('total_sessions', 0),
                    'total_workflows': total_wf,
                    'success_rate': (success_wf / total_wf * 100) if total_wf > 0 else 0,
                    'avg_duration': (stats.get('avg_duration') or 0) / 60
                }
            return {'total_sessions': 0, 'total_workflows': 0, 'success_rate': 0, 'avg_duration': 0}
        except Exception as e:
            st.error(f"Error fetching profile stats: {str(e)}")
            return {}

    # -------------------------------------------------------------------------
    # Data fetchers
    # -------------------------------------------------------------------------
    def _fetch_execution_sessions(self, account_id: int, date_range: Dict) -> List[Dict]:
        try:
            query = {
                'postgres_account_id': account_id,
                'created_at': {'$gte': date_range['start'], '$lte': date_range['end']}
            }
            return list(self.mongo_db.execution_sessions.find(query).sort('created_at', -1))
        except Exception as e:
            st.error(f"Error fetching sessions: {str(e)}")
            return []

    def _get_session_info(self, session_id: str) -> Optional[Dict]:
        try:
            return self.mongo_db.execution_sessions.find_one({'session_id': session_id})
        except Exception:
            return None
