"""
Filtering Configuration - Main Settings

UPDATED: Now includes independent time windows for filtering
Category / workflow_type / collection_name are read from automa_workflow_config
Filtering windows can be configured separately from extraction windows
"""

import streamlit as st
from datetime import date, datetime, timedelta
from ...settings.settings_manager import (
    get_filtering_settings,
    update_filtering_settings,
    get_system_setting,
    update_system_setting
)
import json
from typing import Dict, Any, List, Optional
from bson import ObjectId
import logging

logger = logging.getLogger(__name__)

DEFAULT_FILTER_WORDS = (
    'touchofm_,Eileevalencia,Record_spot1,brill_writers,essayzpro,'
    'primewriters23a,essaygirl01,EssayNasrah,Sharifwriter1,'
    'EssaysAstute,queentinabrown,analytics'
)

# Default filtering windows (can be different from extraction)
DEFAULT_FILTERING_WINDOWS = {
    "enabled": True,
    "morning_window": "06:00-10:00",
    "evening_window": "18:00-22:00",
}

DAYS_ORDER = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']


# ============================================================================
# DAG EXECUTION CONFIGURATION
# ============================================================================

def render_simplified_dag_execution_in_form(current_config: dict = None, key_prefix: str = ""):
    st.markdown("---")
    st.markdown("### 🎯 DAG Triggers")
    st.caption("Configure which DAG to trigger after filtering completes")

    if current_config is None:
        current_config = get_system_setting('filter_links_execution_settings', {
            'selected_dag': 'report_with_workflows'
        })

    current_selection = current_config.get('selected_dag', 'report_with_workflows')
    valid_options = ['executor', 'report', 'report_with_workflows']
    if current_selection not in valid_options:
        current_selection = 'report_with_workflows'

    selected_dag = st.radio(
        "Select DAG to trigger:",
        options=valid_options,
        index=valid_options.index(current_selection),
        key=f'{key_prefix}selected_dag',
        format_func=lambda x: {
            'executor':              '🤖 Local Executor - Executes workflows with Chrome profiles',
            'report':                '📊 Filter Report - Generates statistics and emails report only',
            'report_with_workflows': '📦 Filter Report + Workflows - Emails report AND workflow ZIP attachment',
        }[x],
    )

    st.markdown("---")
    summaries = {
        'executor':              "✅ **Selected:** Local Executor DAG will be triggered after filtering",
        'report':                "✅ **Selected:** Filter Report DAG will be triggered (report email only)",
        'report_with_workflows': "✅ **Selected:** Filter Report + Workflows DAG will be triggered (report + ZIP)",
    }
    st.success(summaries[selected_dag])

    return {
        'selected_dag':                  selected_dag,
        'trigger_executor':              selected_dag == 'executor',
        'trigger_report':                selected_dag == 'report',
        'trigger_report_with_workflows': selected_dag == 'report_with_workflows',
        'executor_wait':                 False,
        'report_wait':                   False,
        'executor_timeout_hours':        2,
        'report_timeout_minutes':        10,
    }


def save_dag_execution_config(config: dict):
    try:
        update_system_setting('filter_links_execution_settings', {
            **config,
            'updated_at': datetime.now().isoformat(),
            'updated_by': 'streamlit_ui_filtering_tab',
        })
        return True
    except Exception as e:
        st.error(f"Error saving DAG execution configuration: {e}")
        return False


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
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    return days[target_date.weekday()]


def _ensure_filter_words_seeded():
    try:
        existing      = get_system_setting('extraction_processing_settings', {})
        current_words = existing.get('words_to_filter', '')
        if not current_words or not current_words.strip():
            logger.info("Seeding default filter words into MongoDB")
            update_system_setting('extraction_processing_settings', {
                'words_to_filter': DEFAULT_FILTER_WORDS,
                'seeded_at':       datetime.now().isoformat(),
                'seeded_by':       'filtering_configuration_auto_seed',
            })
    except Exception as e:
        logger.error(f"Failed to seed filter words: {e}")


# ============================================================================
# AUTOMA CONFIG READER
# ============================================================================

def get_automa_config_for_day(day_key: str) -> dict:
    """
    Return the automa_workflow_config for a given day_key, or {} if not set.
    """
    try:
        weekly = get_system_setting('weekly_workflow_settings', {})
        return weekly.get(day_key, {}).get('automa_workflow_config', {})
    except Exception:
        return {}


def get_automa_config_for_today() -> dict:
    today_key = datetime.now().strftime('%A').lower()
    return get_automa_config_for_day(today_key)


def _automa_config_summary(automa_cfg: dict) -> dict:
    """Extract just the fields we display/use from an automa_workflow_config."""
    return {
        'category':        automa_cfg.get('destination_category', ''),
        'workflow_type':   automa_cfg.get('workflow_type', ''),
        'collection_name': automa_cfg.get('collection_name', ''),
        'configured':      bool(automa_cfg.get('destination_category')),
    }


def _render_automa_info_box(automa_cfg: dict, day_label: str = "today"):
    """
    Show a read-only info box with the category/type/collection pulled from
    automa_workflow_config. Returns True if configured, False if missing.
    """
    summary = _automa_config_summary(automa_cfg)

    if not summary['configured']:
        st.warning(
            f"⚠️ No Automa Workflow Config found for **{day_label}**. "
            f"You can still save the filter amount, windows, and DAG settings — "
            f"category/type/collection will be auto-linked once you configure "
            f"the **Automa Workflow Config** page."
        )
        return False

    st.info(
        f"🔗 **Auto-linked from Automa Workflow Config** \n\n"
        f"&nbsp;&nbsp;📁 **Category:** `{summary['category']}`  \n"
        f"&nbsp;&nbsp;🔄 **Workflow Type:** `{summary['workflow_type']}`  \n"
        f"&nbsp;&nbsp;🗄️ **Collection:** `{summary['collection_name'] or '(not set)'}`"
    )
    return True


# ============================================================================
# BUILD FILTERING_CONFIG DICT FOR ONE DAY
# ============================================================================

def _build_filtering_day_config(
    existing_day_cfg: dict,
    day_key: str,
    day_name: str,
    config_date_iso: str,
    filter_amount: int,
    morning_window: str,
    evening_window: str,
    windows_enabled: bool,
) -> dict:
    """
    Build a filtering_config for one day.
    Includes separate time windows for filtering.
    """
    automa_cfg = get_automa_config_for_day(day_key)
    summary    = _automa_config_summary(automa_cfg)

    day_cfg = dict(existing_day_cfg)
    day_cfg['day_name'] = day_name
    day_cfg['day_key']  = day_key
    day_cfg['filtering_config'] = {
        'config_date':          config_date_iso,
        'day_name':             day_name,
        'day_key':              day_key,
        'filter_date':          config_date_iso,
        'filter_amount':        filter_amount,
        'enabled':              True,
        'updated_at':           datetime.now().isoformat(),
        # Time windows for filtering (can be different from extraction)
        'morning_window':       morning_window,
        'evening_window':       evening_window,
        'windows_enabled':      windows_enabled,
        # pulled from automa_workflow_config if available
        'destination_category': summary['category'],
        'workflow_type_name':   summary['workflow_type'],
        'collection_name':      summary['collection_name'],
        'source':               'automa_workflow_config' if summary['configured'] else 'manual_partial',
    }
    return day_cfg


# ============================================================================
# PARSE TIME WINDOW HELPER
# ============================================================================

def _parse_time_window(window_str: str) -> tuple:
    """Parse time window string and return (start, end, duration_hours)"""
    try:
        s, e = window_str.split('-')
        start = datetime.strptime(s.strip(), '%H:%M')
        end = datetime.strptime(e.strip(), '%H:%M')
        
        # Calculate duration
        if start <= end:
            duration = (end - start).seconds / 3600
        else:
            # Window crosses midnight
            duration = (24 - start.hour + end.hour)
        
        return start.time(), end.time(), duration
    except Exception as e:
        return None, None, None


# ============================================================================
# MAIN RENDERING
# ============================================================================

def render_filtering_config():
    st.header("🎯 Filtering Configuration")
    st.markdown("*Category, workflow type and collection are inherited automatically from the Automa Workflow Config page when available.*")
    st.markdown("*⏱️ Filtering windows can be set independently from extraction windows.*")

    _ensure_filter_words_seeded()

    tab1, tab2 = st.tabs(["⚙️ Filtering Settings", "🔍 Content Filtering"])
    with tab1:
        render_filtering_settings_tab()
    with tab2:
        render_content_filtering_tab()


def render_content_filtering_tab():
    st.markdown("### 🔍 Content Filtering Settings")

    processing_settings = get_system_setting('extraction_processing_settings', {})
    current_words       = processing_settings.get('words_to_filter', DEFAULT_FILTER_WORDS)

    word_list_preview = [w.strip() for w in current_words.split(',') if w.strip()]
    if word_list_preview:
        st.success(f"✅ **{len(word_list_preview)} filter words active**")
    else:
        st.warning("⚠️ No filter words configured — DAG will use hardcoded fallback list")

    with st.form("content_filtering_form"):
        st.markdown("#### Filter Words/Phrases")
        words_to_filter = st.text_area(
            "Words to filter (comma-separated)",
            value=current_words,
            height=150,
        )
        word_list = [w.strip() for w in words_to_filter.split(',') if w.strip()]
        if word_list:
            st.write(f"**{len(word_list)} words configured**")
            with st.expander("Preview word list"):
                for i, w in enumerate(word_list, 1):
                    st.write(f"{i}. `{w}`")

        st.markdown("---")
        if st.form_submit_button("💾 Save Settings", type="primary", use_container_width=True):
            try:
                update_system_setting('extraction_processing_settings', {
                    'words_to_filter': words_to_filter.strip(),
                    'updated_at':      datetime.now().isoformat(),
                })
                st.success("✅ Content filtering settings saved!")
                st.info(f"📝 {len(word_list)} filter words saved")
                st.balloons()
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error saving settings: {e}")


def render_filtering_settings_tab():
    try:
        weekly_settings = get_system_setting('weekly_workflow_settings', {})
        if not isinstance(weekly_settings, dict):
            weekly_settings = {}

        with st.form("filtering_config_form"):
            st.markdown("#### 📅 Schedule Date")
            st.caption("Used only for single-day saves. Ignored when saving to all 7 days.")

            available_dates   = get_future_dates(8)
            date_options      = [ds for _, ds in available_dates]
            date_values       = [d  for d, _  in available_dates]
            selected_date_str = st.selectbox("Schedule Date", options=date_options, index=0)
            selected_index    = date_options.index(selected_date_str)
            scheduled_date    = date_values[selected_index]

            day_key  = get_day_key_from_date(scheduled_date)
            day_name = scheduled_date.strftime('%A')

            # Check existing filtering config
            existing_filter_cfg = (
                weekly_settings.get(day_key, {}).get('filtering_config', {})
            )
            if existing_filter_cfg:
                st.success(f"✅ Filtering config exists for {day_name}")
            else:
                st.info(f"ℹ️ No filtering config for {day_name} yet")

            st.markdown("---")

            # Auto-linked source info
            st.markdown("### 🔗 Auto-Linked from Automa Workflow Config")
            st.caption(
                "These values are read automatically from the Automa Workflow Config you already set."
            )

            automa_selected = get_automa_config_for_day(day_key)
            automa_ok       = _render_automa_info_box(automa_selected, day_label=day_name)

            st.markdown("---")

            # =================================================================
            # FILTERING TIME WINDOWS (Independent from Extraction)
            # =================================================================
            st.markdown("### 🕐 Filtering Time Windows")
            st.caption(
                "Configure when filtering should run. These can be completely different "
                "from extraction windows. Filtering will only run during these times, "
                "even if triggered by extraction outside these windows."
            )

            # Get existing windows or use defaults
            existing_windows = existing_filter_cfg if existing_filter_cfg else {}
            
            col1, col2 = st.columns(2)
            
            with col1:
                windows_enabled = st.checkbox(
                    "Enable Filtering Windows",
                    value=existing_windows.get('windows_enabled', True),
                    help="If disabled, filtering can run anytime (not recommended)"
                )
            
            with col2:
                if not windows_enabled:
                    st.warning("⚠️ Windows disabled - filtering can run anytime")
            
            st.markdown("#### Morning Window")
            col_m1, col_m2 = st.columns(2)
            
            with col_m1:
                morning_window = st.text_input(
                    "Morning (HH:MM-HH:MM)",
                    value=existing_windows.get('morning_window', DEFAULT_FILTERING_WINDOWS['morning_window']),
                    placeholder="06:00-10:00",
                    disabled=not windows_enabled,
                    key="filtering_morning_window"
                )
            
            with col_m2:
                if morning_window and windows_enabled:
                    start, end, duration = _parse_time_window(morning_window)
                    if start and end:
                        if start <= end:
                            st.caption(f"✓ {duration:.1f}h window")
                        else:
                            st.caption(f"✓ {duration:.1f}h (crosses midnight)")
                    else:
                        st.caption("❌ Invalid format")
            
            st.markdown("#### Evening Window")
            col_e1, col_e2 = st.columns(2)
            
            with col_e1:
                evening_window = st.text_input(
                    "Evening (HH:MM-HH:MM)",
                    value=existing_windows.get('evening_window', DEFAULT_FILTERING_WINDOWS['evening_window']),
                    placeholder="18:00-22:00",
                    disabled=not windows_enabled,
                    key="filtering_evening_window"
                )
            
            with col_e2:
                if evening_window and windows_enabled:
                    start, end, duration = _parse_time_window(evening_window)
                    if start and end:
                        if start <= end:
                            st.caption(f"✓ {duration:.1f}h window")
                        else:
                            st.caption(f"✓ {duration:.1f}h (crosses midnight)")
                    else:
                        st.caption("❌ Invalid format")
            
            # Show preview of filtering schedule
            if windows_enabled and morning_window and evening_window:
                st.info(f"""
                📋 **Filtering Schedule for {day_name}:**
                
                Filtering will only run during:
                - 🌅 Morning: {morning_window}
                - 🌃 Evening: {evening_window}
                
                Even if extraction finishes outside these windows, filtering won't trigger.
                """)
            
            st.markdown("---")

            # Filter amount
            st.markdown("### ⚙️ Filter Settings")

            default_amount = int(existing_filter_cfg.get('filter_amount', 5)) if existing_filter_cfg else 5

            filter_amount = st.number_input(
                "Amount to Filter",
                min_value=1,
                max_value=10000,
                value=default_amount,
                help="Maximum number of links to process per DAG run.",
            )

            st.info("⏱️ No time limit — all links processed regardless of age")

            st.markdown("---")

            # DAG trigger
            saved_dag_config = get_system_setting('filter_links_execution_settings', {
                'selected_dag': 'report_with_workflows'
            })
            if saved_dag_config.get('selected_dag') not in ['executor', 'report', 'report_with_workflows']:
                saved_dag_config['selected_dag'] = 'report_with_workflows'

            dag_execution_config = render_simplified_dag_execution_in_form(
                current_config=saved_dag_config
            )

            # Save buttons
            st.markdown("---")
            st.markdown("""
                <div style='background: linear-gradient(135deg,rgba(103,58,183,.12),rgba(33,150,243,.12));
                            padding:18px 20px; border-radius:10px;
                            border-left:5px solid #7c4dff; margin:10px 0 12px 0;'>
                    <h3 style='color:#4a148c;margin:0 0 6px 0;'>📅 Two ways to save</h3>
                    <p style='margin:0;color:#555;font-size:14px;'>
                        &nbsp;&nbsp;• <strong>💾 Save to Selected Day</strong> — applies only to the date chosen above<br>
                        &nbsp;&nbsp;• <strong>🗓️ Save to ALL 7 Days</strong> — copies filter_amount and windows to every day
                          (each day keeps its own automa_workflow_config values)
                    </p>
                </div>
            """, unsafe_allow_html=True)

            btn_col1, btn_col2 = st.columns(2)
            with btn_col1:
                submit_single = st.form_submit_button(
                    "💾 Save to Selected Day",
                    type="primary",
                    use_container_width=True,
                )
            with btn_col2:
                submit_all_week = st.form_submit_button(
                    "🗓️ Save to ALL 7 Days",
                    use_container_width=True,
                )

        # =====================================================================
        # SAVE HANDLERS
        # =====================================================================
        if submit_single or submit_all_week:
            try:
                fresh_weekly = get_system_setting('weekly_workflow_settings', {})

                if submit_single:
                    fresh_weekly[day_key] = _build_filtering_day_config(
                        existing_day_cfg = fresh_weekly.get(day_key, {}),
                        day_key          = day_key,
                        day_name         = day_name,
                        config_date_iso  = scheduled_date.isoformat(),
                        filter_amount    = filter_amount,
                        morning_window   = morning_window,
                        evening_window   = evening_window,
                        windows_enabled  = windows_enabled,
                    )
                    update_system_setting('weekly_workflow_settings', fresh_weekly)
                    save_dag_execution_config(dag_execution_config)

                    s = _automa_config_summary(get_automa_config_for_day(day_key))
                    st.success(f"✅ Filtering config saved for **{day_name}**!")

                    if windows_enabled:
                        st.info(f"🕐 Filtering windows: {morning_window} / {evening_window}")
                    
                    if s['configured']:
                        st.info(
                            f"🔗 Using automa config — "
                            f"Category: **{s['category']}** | "
                            f"Type: **{s['workflow_type']}** | "
                            f"Collection: **{s['collection_name'] or 'N/A'}**"
                        )
                    else:
                        st.warning(
                            "⚠️ Automa Workflow Config not set for this day — "
                            "filter_amount, windows and DAG settings saved. "
                            "Category/type/collection will auto-populate once you configure it."
                        )

                    st.info(f"🔢 Filter amount: **{filter_amount}**")
                    st.balloons()
                    st.rerun()

                elif submit_all_week:
                    today         = datetime.now().date()
                    weekday_today = today.weekday()
                    saved_days    = []
                    no_automa_days = []

                    for offset, dk in enumerate(DAYS_ORDER):
                        days_until = (offset - weekday_today) % 7
                        target_dt  = today + timedelta(days=days_until)
                        dn         = target_dt.strftime('%A')

                        fresh_weekly[dk] = _build_filtering_day_config(
                            existing_day_cfg = fresh_weekly.get(dk, {}),
                            day_key          = dk,
                            day_name         = dn,
                            config_date_iso  = target_dt.isoformat(),
                            filter_amount    = filter_amount,
                            morning_window   = morning_window,
                            evening_window   = evening_window,
                            windows_enabled  = windows_enabled,
                        )

                        a_cfg = get_automa_config_for_day(dk)
                        a_sum = _automa_config_summary(a_cfg)

                        day_info = f"**{dn}** ({target_dt.strftime('%b %d')})"
                        if a_sum['configured']:
                            saved_days.append(
                                f"✅ {day_info} — "
                                f"`{a_sum['category']}` / `{a_sum['workflow_type']}`"
                            )
                        else:
                            no_automa_days.append(
                                f"⚠️ {day_info} — "
                                f"saved (no automa config yet)"
                            )

                    update_system_setting('weekly_workflow_settings', fresh_weekly)
                    save_dag_execution_config(dag_execution_config)

                    st.success(f"🎉 Filtering config saved for **all 7 days**!")

                    if windows_enabled:
                        st.info(f"🕐 Filtering windows applied to all days: {morning_window} / {evening_window}")

                    if saved_days:
                        st.markdown("**Days with Automa Config linked:**")
                        for sd in saved_days:
                            st.markdown(sd)

                    if no_automa_days:
                        st.markdown("**Days saved without Automa Config (will auto-link later):**")
                        for nd in no_automa_days:
                            st.markdown(nd)

                    st.info(f"🔢 Filter amount applied to all days: **{filter_amount}**")
                    st.balloons()
                    st.rerun()

            except Exception as e:
                st.error(f"❌ Error: {e}")
                import traceback
                st.error(traceback.format_exc())

        # =====================================================================
        # CONFIGURED SETTINGS DISPLAY
        # =====================================================================
        if weekly_settings:
            st.markdown("---")
            st.markdown("### 📋 Configured Days")

            any_shown = False
            for dk in DAYS_ORDER:
                if dk not in weekly_settings:
                    continue

                day_config    = weekly_settings[dk]
                filter_config = day_config.get('filtering_config')
                if not filter_config:
                    continue

                any_shown   = True
                config_date = filter_config.get('config_date', 'N/A')

                # Get live automa values for display
                live_automa = get_automa_config_for_day(dk)
                live_sum    = _automa_config_summary(live_automa)

                with st.expander(
                    f"🎯 {day_config.get('day_name', dk.capitalize())} — {config_date}",
                    expanded=False,
                ):
                    col1, col2, col3 = st.columns([3, 1, 1])

                    with col1:
                        st.write(f"**Filter Amount:** {filter_config.get('filter_amount', 'N/A')}")
                        
                        # Show filtering windows
                        if filter_config.get('windows_enabled', True):
                            st.write(f"**Morning Window:** {filter_config.get('morning_window', 'Not set')}")
                            st.write(f"**Evening Window:** {filter_config.get('evening_window', 'Not set')}")
                        else:
                            st.write("**Windows:** Disabled (runs anytime)")
                        
                        st.write("**Enabled:** ✅ Always On")

                        if live_sum['configured']:
                            st.markdown(
                                f"🔗 *Live from Automa Config:* "
                                f"`{live_sum['category']}` / `{live_sum['workflow_type']}` / "
                                f"`{live_sum['collection_name'] or '—'}`"
                            )
                        else:
                            st.caption("⚠️ Automa config not yet set for this day")

                        if filter_config.get('updated_at'):
                            st.caption(f"Last updated: {filter_config['updated_at']}")

                    with col2:
                        if st.button("✏️ Edit", key=f"edit_btn_{dk}", use_container_width=True):
                            st.session_state[f'editing_filter_{dk}'] = True
                            st.rerun()

                    with col3:
                        if st.button("🗑️ Delete", key=f"del_{dk}", use_container_width=True):
                            try:
                                fw = get_system_setting('weekly_workflow_settings', {})
                                if 'filtering_config' in fw.get(dk, {}):
                                    del fw[dk]['filtering_config']
                                    update_system_setting('weekly_workflow_settings', fw)
                                    st.success("✅ Deleted!")
                                    st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error: {e}")

                    # Inline edit
                    if st.session_state.get(f'editing_filter_{dk}'):
                        st.markdown("---")
                        st.markdown(f"**✏️ Editing {dk.title()}**")
                        st.info("Use the main form above to edit this day's settings")
                        if st.button("❌ Close", key=f"close_edit_{dk}"):
                            st.session_state[f'editing_filter_{dk}'] = False
                            st.rerun()

            if not any_shown:
                st.info("No filtering configurations saved yet.")

    except Exception as e:
        st.error(f"❌ Error: {e}")
        import traceback
        st.error(traceback.format_exc())
