import streamlit as st
from ...settings.settings_manager import get_system_setting
from typing import Dict, Any, List, Tuple
import pandas as pd
from datetime import date, timedelta


def get_future_dates_schedule(num_days: int = 14) -> List[Tuple[date, str, str]]:
    """Get future dates for the next N days with day names and keys."""
    dates = []
    today = date.today()
    days_keys = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

    for i in range(num_days):
        future_date = today + timedelta(days=i)
        day_name = future_date.strftime('%A')
        day_key = days_keys[future_date.weekday()]
        dates.append((future_date, day_name, day_key))

    return dates


def get_config_for_date(weekly_settings: Dict, target_date: date, day_key: str) -> Dict[str, Any]:
    """
    Get all configurations for a specific date.

    Configs are stored per-weekday (monday, tuesday, …) and each sub-config
    carries a `config_date` that records *when* it was last saved, not which
    future occurrence it applies to.  Matching strictly on config_date == target_date
    would only ever show a config on the exact calendar date it was saved, making
    it invisible for every future occurrence of that weekday.

    The correct behaviour for a weekly scheduler is:
      - If a config exists for this weekday → show it for every occurrence of
        that weekday in the 2-week window.
      - config_date is shown as metadata ("last configured on …") but is NOT
        used as a gate.
    """
    day_config = weekly_settings.get(day_key, {})

    if not day_config:
        return {}

    configs = {}

    # Content config lives directly on the day dict (has workflow_type, content_name, etc.)
    # Distinguish it from a bare day dict that only has time-window keys by checking
    # for at least one content-specific field.
    CONTENT_FIELDS = {'workflow_type', 'content_name', 'content_amount'}
    if CONTENT_FIELDS.intersection(day_config.keys()):
        configs['content'] = day_config

    # Automa workflow config
    if 'automa_workflow_config' in day_config:
        configs['automa'] = day_config['automa_workflow_config']

    # Execution config
    if 'execution_config' in day_config:
        configs['execution'] = day_config['execution_config']

    # Filtering config
    if 'filtering_config' in day_config:
        configs['filtering'] = day_config['filtering_config']

    # Extraction config — time windows live directly on the day dict.
    # A day has extraction config if morning_window or evening_window is set.
    # time_filter and testing_config are global (not per-day) but pulled in for display.
    if 'morning_window' in day_config or 'evening_window' in day_config:
        configs['extraction'] = {
            'enabled':        day_config.get('enabled', True),
            'morning_window': day_config.get('morning_window', 'N/A'),
            'evening_window': day_config.get('evening_window', 'N/A'),
            'time_filter':    weekly_settings.get('time_filter', {}),
            'testing_config': weekly_settings.get('testing_config', {}),
        }

    return configs


def render_day_card(target_date: date, day_name: str, configs: Dict[str, Any], is_today: bool = False):
    """Render a card for a single day's configuration."""

    has_any_config = len(configs) > 0

    if is_today:
        card_color = "rgba(28, 131, 225, 0.15)"
        border_color = "#1c83e1"
        title_emoji = "⭐"
    elif has_any_config:
        card_color = "rgba(46, 204, 113, 0.1)"
        border_color = "#2ecc71"
        title_emoji = "✅"
    else:
        card_color = "rgba(149, 165, 166, 0.1)"
        border_color = "#95a5a6"
        title_emoji = "❌"

    st.markdown(f"""
        <div style='background-color: {card_color};
                    padding: 15px;
                    border-radius: 8px;
                    margin: 10px 0;
                    border-left: 4px solid {border_color};'>
            <h4 style='margin: 0; color: {border_color};'>
                {title_emoji} {day_name} - {target_date.strftime('%B %d, %Y')}
            </h4>
        </div>
    """, unsafe_allow_html=True)

    if not has_any_config:
        st.warning("⚠️ No configuration set for this day")
        st.caption("💡 Configure this day in: Create Content, Automa Workflow, Execution, or Filtering pages")
        return

    config_cols = st.columns(5)

    # Content Configuration
    with config_cols[0]:
        if 'content' in configs:
            content = configs['content']
            st.markdown("**📝 Content**")
            st.write("✅ Configured")
            st.caption(f"Type: {content.get('workflow_type', 'N/A').capitalize()}")
            st.caption(f"Amount: {content.get('content_amount', 0)}")
            st.caption(f"Name: {content.get('content_name', 'N/A')}")
        else:
            st.markdown("**📝 Content**")
            st.write("❌ Not set")

    # Automa Configuration
    with config_cols[1]:
        if 'automa' in configs:
            automa = configs['automa']
            st.markdown("**🤖 Automa**")
            st.write("✅ Configured")

            content_items = automa.get('content_items', [])
            if content_items:
                st.caption(f"Items: {len(content_items)}")
                first_item = content_items[0]
                st.caption(f"First: {first_item.get('content_name', 'N/A')}")
            else:
                # Backward compatibility
                st.caption(f"Content: {automa.get('content_name', 'N/A')}")

            st.caption(f"Category: {automa.get('destination_category', 'N/A').title()}")
        else:
            st.markdown("**🤖 Automa**")
            st.write("❌ Not set")

    # Execution Configuration
    # Schema: workflow_category, workflow_type, collection_name,
    #         max_workflows_to_process, delay_min_milliseconds, delay_max_milliseconds, wait_margin_ms
    with config_cols[2]:
        if 'execution' in configs:
            execution = configs['execution']
            st.markdown("**⚡ Execution**")
            st.write("✅ Configured")
            st.caption(f"Category: {execution.get('workflow_category', 'N/A').title()}")
            st.caption(f"Type: {execution.get('workflow_type', 'N/A')}")
            max_wf = execution.get('max_workflows_to_process', 0)
            st.caption(f"Max: {'All' if max_wf == 0 else max_wf}")
        else:
            st.markdown("**⚡ Execution**")
            st.write("❌ Not set")

    # Filtering Configuration
    # Schema: filter_amount, enabled, destination_category, workflow_type_name, collection_name
    # Note: hours_limit was removed — time limit no longer applies
    with config_cols[3]:
        if 'filtering' in configs:
            filtering = configs['filtering']
            st.markdown("**🎯 Filtering**")
            enabled = filtering.get('enabled', False)
            st.write("✅ Configured")
            st.caption(f"Status: {'Enabled' if enabled else 'Disabled'}")
            st.caption(f"Amount: {filtering.get('filter_amount', 0)}")
            st.caption(f"Category: {filtering.get('destination_category', 'N/A').title()}")
        else:
            st.markdown("**🎯 Filtering**")
            st.write("❌ Not set")

    # Extraction Configuration
    # Schema: enabled, morning_window, evening_window (per-day)
    #         + global time_filter and testing_config
    with config_cols[4]:
        if 'extraction' in configs:
            extraction = configs['extraction']
            st.markdown("**🔍 Extraction**")
            day_enabled = extraction.get('enabled', True)
            st.write("✅ Configured")
            st.caption(f"Day: {'Enabled' if day_enabled else 'Disabled'}")
            st.caption(f"🌅 {extraction.get('morning_window', 'N/A')}")
            st.caption(f"🌃 {extraction.get('evening_window', 'N/A')}")
        else:
            st.markdown("**🔍 Extraction**")
            st.write("❌ Not set")

    # Detailed expandable section
    with st.expander("📋 View Full Details"):
        detail_tabs = st.tabs(["📝 Content", "🤖 Automa", "⚡ Execution", "🎯 Filtering", "🔍 Extraction"])

        # Content Details
        with detail_tabs[0]:
            if 'content' in configs:
                content = configs['content']
                st.markdown("#### Content Generation Settings")

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Workflow Type", content.get('workflow_type', 'N/A').capitalize())
                    st.metric("Content Name", content.get('content_name', 'N/A'))
                with col2:
                    st.metric("Content Amount", content.get('content_amount', 0))
                    st.metric("Filter Amount", content.get('filter_amount', 0))
                with col3:
                    st.metric("Extraction Window", f"{content.get('extraction_window', 0)}h")
                    st.metric("Gap (minutes)", content.get('gap_between_workflows', 0))

                if content.get('words_to_filter'):
                    words = [w.strip() for w in content['words_to_filter'].split(',') if w.strip()]
                    st.info(f"🏷️ **Filter Keywords:** {len(words)} configured")
                    st.caption(", ".join(words[:10]) + ("..." if len(words) > 10 else ""))
            else:
                st.info("ℹ️ Content generation not configured for this day")

        # Automa Details
        with detail_tabs[1]:
            if 'automa' in configs:
                automa = configs['automa']
                st.markdown("#### Automa Workflow Settings")

                st.markdown("**📝 Content Items:**")
                content_items = automa.get('content_items', [])
                if content_items:
                    for item in content_items:
                        st.write(f"• **{item.get('label', 'N/A')}:** {item.get('content_type', 'N/A')} - {item.get('content_name', 'N/A')}")
                else:
                    # Backward compatibility
                    st.write(f"• Type: {automa.get('content_type', 'N/A')}")
                    st.write(f"• Name: {automa.get('content_name', 'N/A')}")

                st.markdown("---")

                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**📂 Source:**")
                    st.write(f"• Category: {automa.get('source_category', 'N/A').title()}")
                    st.write(f"• Template: {automa.get('workflow_source_name', 'N/A')}")

                    st.markdown("**💾 Destination:**")
                    st.write(f"• Category: {automa.get('destination_category', 'N/A').title()}")
                    st.write(f"• Type: {automa.get('workflow_type', 'N/A')}")
                    st.write(f"• Collection: {automa.get('collection_name', 'N/A')}")

                with col2:
                    st.markdown("**⏱️ Timing:**")
                    delays = automa.get('delays', {})
                    st.write(f"• Delay Count: {delays.get('count', 0)}")
                    st.write(f"• Delay Range: {delays.get('min_milliseconds', 0)/1000:.2f}s - {delays.get('max_milliseconds', 0)/1000:.2f}s")

                    press = automa.get('press_keys', {})
                    st.write(f"• Press Keys: {press.get('min_milliseconds', 0)/1000:.2f}s - {press.get('max_milliseconds', 0)/1000:.2f}s")

                    click = automa.get('click_elements', {})
                    st.write(f"• Click Elements: {click.get('min_milliseconds', 0)/1000:.2f}s - {click.get('max_milliseconds', 0)/1000:.2f}s")
            else:
                st.info("ℹ️ Automa workflow not configured for this day")

        # Execution Details
        # Schema: workflow_category, workflow_type, collection_name,
        #         max_workflows_to_process (0 = all), delay_min_milliseconds,
        #         delay_max_milliseconds, wait_margin_ms
        with detail_tabs[2]:
            if 'execution' in configs:
                execution = configs['execution']
                st.markdown("#### Execution Settings")

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Category", execution.get('workflow_category', 'N/A').title())
                    st.metric("Workflow Type", execution.get('workflow_type', 'N/A'))
                with col2:
                    st.metric("Collection", execution.get('collection_name', 'N/A'))
                    max_wf = execution.get('max_workflows_to_process', 0)
                    st.metric("Max Workflows", "All" if max_wf == 0 else max_wf)
                with col3:
                    delay_min = execution.get('delay_min_milliseconds', 0)
                    delay_max = execution.get('delay_max_milliseconds', 0)
                    st.metric("Min Delay", f"{delay_min / 1000:.1f}s")
                    st.metric("Max Delay", f"{delay_max / 1000:.1f}s")

                wait_margin = execution.get('wait_margin_ms', 0)
                if wait_margin:
                    st.caption(f"🛡️ Wait Margin: {wait_margin / 60000:.1f} min ({wait_margin:,} ms)")
            else:
                st.info("ℹ️ Execution settings not configured for this day")

        # Filtering Details
        # Schema: filter_amount, enabled, destination_category, workflow_type_name, collection_name
        # Note: hours_limit removed — time limit no longer applied
        with detail_tabs[3]:
            if 'filtering' in configs:
                filtering = configs['filtering']
                st.markdown("#### Filtering Settings")

                col1, col2, col3 = st.columns(3)
                with col1:
                    enabled = filtering.get('enabled', False)
                    st.metric("Status", "✅ Enabled" if enabled else "❌ Disabled")
                    st.metric("Filter Amount", filtering.get('filter_amount', 0))
                with col2:
                    st.metric("Category", filtering.get('destination_category', 'N/A').title())
                    st.metric("Workflow Type", filtering.get('workflow_type_name', 'N/A'))
                with col3:
                    st.metric("Collection", filtering.get('collection_name', 'N/A'))
                    st.caption("⏱️ Time limit: Removed — all ages processed")
            else:
                st.info("ℹ️ Filtering settings not configured for this day")

        # Extraction Details
        # Per-day: enabled, morning_window, evening_window
        # Global: time_filter (enabled, hours_back, fast_mode), testing_config
        with detail_tabs[4]:
            if 'extraction' in configs:
                extraction = configs['extraction']
                st.markdown("#### Extraction Settings")

                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**🕐 Time Windows**")
                    day_enabled = extraction.get('enabled', True)
                    st.metric("Day Status", "✅ Enabled" if day_enabled else "❌ Disabled")
                    st.metric("🌅 Morning Window", extraction.get('morning_window', 'N/A'))
                    st.metric("🌃 Evening Window", extraction.get('evening_window', 'N/A'))

                with col2:
                    st.markdown("**⏱️ Time Filter (Global)**")
                    tf = extraction.get('time_filter', {})
                    if tf:
                        tf_enabled = tf.get('enabled', False)
                        st.metric("Time Filter", "✅ Active" if tf_enabled else "❌ Inactive")
                        if tf_enabled:
                            st.metric("Hours Back", f"{tf.get('hours_back', 24)}h")
                            st.caption(f"Fast Mode: {'✅' if tf.get('fast_mode') else '❌'}")
                            st.caption(f"Skip Enhanced: {'✅' if tf.get('skip_enhanced_extraction') else '❌'}")
                        else:
                            st.caption("All tweets extracted regardless of age")
                    else:
                        st.info("No time filter configured")

                    st.markdown("**🧪 Testing Mode (Global)**")
                    tc = extraction.get('testing_config', {})
                    if tc:
                        test_enabled = tc.get('testing_mode_enabled', False)
                        st.metric("Testing Mode", "🧪 Active" if test_enabled else "⏰ Production")
                        if test_enabled:
                            st.caption(f"Offset: +{tc.get('time_offset_minutes', 0)} min")
                    else:
                        st.info("No testing config found")
            else:
                st.info("ℹ️ Extraction not configured for this day — set time windows in Extraction & Processing Config")


def render_view_settings_overview() -> None:
    """Render a read-only overview of all weekly settings for the next 2 weeks."""
    st.header("📊 Settings Overview - Next 2 Weeks")
    st.markdown("*View all configured workflow settings from today forward*")

    col1, col2, col3 = st.columns([1, 2, 2])
    with col1:
        if st.button("🔄 Refresh", type="secondary"):
            st.rerun()

    with col2:
        today = date.today()
        st.info(f"📅 **Today:** {today.strftime('%A, %B %d, %Y')}")

    with col3:
        two_weeks = today + timedelta(days=13)
        st.info(f"📆 **Through:** {two_weeks.strftime('%A, %B %d, %Y')}")

    try:
        weekly_settings: Dict[str, Dict[str, Any]] = get_system_setting(
            "weekly_workflow_settings", {}
        )
    except Exception as e:
        st.error(f"❌ Error loading weekly settings: {e}")
        return

    if not weekly_settings:
        st.warning("⚠️ No weekly settings configured yet. Go to configuration pages to set up your workflows.")
        st.info("💡 **Available Configuration Pages:**\n"
                "- Create Content Configuration\n"
                "- Automa Workflow Configuration\n"
                "- Execution Configuration\n"
                "- Filtering Configuration")
        return

    schedule = get_future_dates_schedule(14)

    # ============================================
    # SUMMARY STATISTICS
    # ============================================
    st.markdown("### 📈 2-Week Summary")

    total_days = len(schedule)
    configured_days = 0
    configs_by_type = {'content': 0, 'automa': 0, 'execution': 0, 'filtering': 0, 'extraction': 0}

    for target_date, day_name, day_key in schedule:
        configs = get_config_for_date(weekly_settings, target_date, day_key)
        if configs:
            configured_days += 1
            for config_type in configs.keys():
                configs_by_type[config_type] += 1

    metric_col1, metric_col2, metric_col3, metric_col4, metric_col5, metric_col6, metric_col7 = st.columns(7)

    with metric_col1:
        st.metric("Total Days", total_days)
    with metric_col2:
        st.metric("Configured Days", configured_days)
    with metric_col3:
        st.metric("Unconfigured", total_days - configured_days)
    with metric_col4:
        st.metric("📝 Content", configs_by_type['content'])
    with metric_col5:
        st.metric("🤖 Automa", configs_by_type['automa'])
    with metric_col6:
        exec_filter = configs_by_type['execution'] + configs_by_type['filtering']
        st.metric("⚡🎯 Exec+Filter", exec_filter)
    with metric_col7:
        st.metric("🔍 Extraction", configs_by_type['extraction'])

    st.markdown("---")

    # ============================================
    # WEEKLY BREAKDOWN
    # ============================================
    st.markdown("### 📅 Weekly Breakdown")

    week_tabs = st.tabs(["📅 Week 1 (Days 1-7)", "📅 Week 2 (Days 8-14)"])

    with week_tabs[0]:
        st.markdown("#### Week 1: Next 7 Days")
        for i, (target_date, day_name, day_key) in enumerate(schedule[:7]):
            is_today = (i == 0)
            configs = get_config_for_date(weekly_settings, target_date, day_key)
            render_day_card(target_date, day_name, configs, is_today)

    with week_tabs[1]:
        st.markdown("#### Week 2: Days 8-14")
        for target_date, day_name, day_key in schedule[7:14]:
            configs = get_config_for_date(weekly_settings, target_date, day_key)
            render_day_card(target_date, day_name, configs, is_today=False)

    st.markdown("---")

    # ============================================
    # CONFIGURATION GAPS ANALYSIS
    # ============================================
    st.markdown("### ⚠️ Configuration Gaps")
    st.caption("Days without complete configuration")

    gaps_found = False
    gap_data = []

    for target_date, day_name, day_key in schedule:
        configs = get_config_for_date(weekly_settings, target_date, day_key)

        missing = []
        if 'content' not in configs:
            missing.append("📝 Content")
        if 'automa' not in configs:
            missing.append("🤖 Automa")
        if 'execution' not in configs:
            missing.append("⚡ Execution")
        if 'filtering' not in configs:
            missing.append("🎯 Filtering")
        if 'extraction' not in configs:
            missing.append("🔍 Extraction")

        if missing:
            gaps_found = True
            gap_data.append({
                'Date': target_date.strftime('%b %d'),
                'Day': day_name,
                'Missing': ", ".join(missing),
                'Count': len(missing)
            })

    if gaps_found:
        gap_df = pd.DataFrame(gap_data)

        def highlight_severity(row):
            count = row['Count']
            if count == 4:
                return ['background-color: #e74c3c; color: white'] * len(row)
            elif count == 3:
                return ['background-color: #e67e22; color: white'] * len(row)
            elif count == 2:
                return ['background-color: #f39c12; color: white'] * len(row)
            else:
                return ['background-color: #f1c40f; color: black'] * len(row)

        styled_gap_df = gap_df.style.apply(highlight_severity, axis=1)
        st.dataframe(styled_gap_df, use_container_width=True, hide_index=True)
        st.caption("🔴 **4 missing** | 🟠 **3 missing** | 🟡 **2 missing** | 🟡 **1 missing**")
    else:
        st.success("✅ All days in the next 2 weeks are fully configured!")

    st.markdown("---")

    # ============================================
    # CONFIGURATION MATRIX
    # ============================================
    st.markdown("### 📊 Configuration Matrix")
    st.caption("Quick overview of what's configured for each day")

    matrix_data = []
    for target_date, day_name, day_key in schedule:
        configs = get_config_for_date(weekly_settings, target_date, day_key)

        is_today = target_date == date.today()
        display_date = f"{target_date.strftime('%b %d')} {'⭐' if is_today else ''}"

        matrix_data.append({
            'Date': display_date,
            'Day': day_name,
            '📝 Content': '✅' if 'content' in configs else '❌',
            '🤖 Automa': '✅' if 'automa' in configs else '❌',
            '⚡ Execution': '✅' if 'execution' in configs else '❌',
            '🎯 Filtering': '✅' if 'filtering' in configs else '❌',
            '🔍 Extraction': '✅' if 'extraction' in configs else '❌',
            'Total': len(configs)
        })

    matrix_df = pd.DataFrame(matrix_data)

    def highlight_today(row):
        if '⭐' in str(row['Date']):
            return ['background-color: #1e3a5f; color: #ffffff; font-weight: bold'] * len(row)
        return [''] * len(row)

    styled_matrix = matrix_df.style.apply(highlight_today, axis=1)
    st.dataframe(styled_matrix, use_container_width=True, hide_index=True)

    st.markdown("---")

    # ============================================
    # EXPORT OPTIONS
    # ============================================
    st.markdown("### 💾 Export Configuration Data")

    export_col1, export_col2, export_col3 = st.columns(3)

    with export_col1:
        if st.button("📥 Export Matrix as CSV", type="secondary", use_container_width=True):
            csv_data = matrix_df.to_csv(index=False)
            st.download_button(
                label="Download Matrix CSV",
                data=csv_data,
                file_name=f"config_matrix_{date.today().isoformat()}.csv",
                mime="text/csv",
                use_container_width=True
            )

    with export_col2:
        if gap_data:
            if st.button("⚠️ Export Gaps Report", type="secondary", use_container_width=True):
                gap_csv = gap_df.to_csv(index=False)
                st.download_button(
                    label="Download Gaps CSV",
                    data=gap_csv,
                    file_name=f"config_gaps_{date.today().isoformat()}.csv",
                    mime="text/csv",
                    use_container_width=True
                )

    with export_col3:
        if st.button("📋 Export All Settings JSON", type="secondary", use_container_width=True):
            import json
            settings_json = json.dumps(weekly_settings, indent=2)
            st.download_button(
                label="Download JSON",
                data=settings_json,
                file_name=f"weekly_settings_{date.today().isoformat()}.json",
                mime="application/json",
                use_container_width=True
            )

    # ============================================
    # HELP
    # ============================================
    st.markdown("---")
    with st.expander("💡 Understanding This View"):
        st.markdown("""
        ### How to Read This Overview

        **📊 2-Week Summary:**
        - Shows how many days are configured vs unconfigured
        - Breaks down by configuration type (Content, Automa, Execution, Filtering)

        **📅 Weekly Breakdown:**
        - **Week 1:** Today through next 6 days
        - **Week 2:** Days 8-14 from today
        - Each day shows all 4 configuration types

        **Day Card Colors:**
        - 🔵 **Blue (⭐):** Today
        - 🟢 **Green (✅):** Has configuration(s)
        - ⚪ **Gray (❌):** No configuration

        **⚠️ Configuration Gaps:**
        - Lists days missing any configuration type
        - Color-coded by severity (red = all missing, yellow = one missing)

        **📊 Configuration Matrix:**
        - Quick grid showing what's configured for each day
        - ✅ = Configured, ❌ = Not configured

        ### Configuration Types

        1. **📝 Content:** What to generate (messages, replies, etc.)
        2. **🤖 Automa:** How to automate the workflow
        3. **⚡ Execution:** When and how to execute (category, type, collection, delay range, wait margin)
        4. **🎯 Filtering:** What content to filter (amount, category, type, collection)

        ### Notes on Execution Fields
        - **Max Workflows:** 0 means "process all available" — shown as "All"
        - **Delay Range:** Min/max milliseconds between workflow executions
        - **Wait Margin:** Safety buffer added on top of the auto-estimated execution time

        ### Notes on Filtering Fields
        - **Time limit has been removed** — all links are processed regardless of age
        - **Amount:** Number of links to filter per run

        ### Tips
        - Configure at least 2 weeks in advance for smooth operations
        - Check gaps regularly and fill in missing configurations
        - Today's date is always highlighted with ⭐
        - Use export options to backup your configurations
        """)
