"""
Enhanced Filtering Settings Tab with Integrated DAG Execution Configuration
This module combines filtering configuration with downstream DAG execution options
"""

import streamlit as st
from datetime import date, datetime, timedelta


def render_dag_execution_section(current_config: dict = None):
    """
    Render the DAG execution configuration section within the filtering settings.

    Args:
        current_config: Current DAG execution configuration (if any)
    """
    st.markdown("---")
    st.markdown("### 🔀 Downstream DAG Execution")
    st.caption("Configure which DAGs to trigger after filtering completes")

    # Load current settings or use defaults
    if current_config is None:
        from ...settings.settings_manager import get_system_setting
        current_config = get_system_setting('filter_links_execution_settings', {
            'trigger_executor': True,
            'trigger_report': False,
            'executor_wait': True,
            'report_wait': False,
            'executor_timeout_hours': 2,
            'report_timeout_minutes': 10
        })

    # Quick preset buttons
    st.markdown("#### ⚡ Quick Presets")

    preset_cols = st.columns(4)

    preset_selected = None

    with preset_cols[0]:
        if st.form_submit_button("🤖 Executor Only", use_container_width=True):
            preset_selected = 'executor_only'

    with preset_cols[1]:
        if st.form_submit_button("📊 Report Only", use_container_width=True):
            preset_selected = 'report_only'

    with preset_cols[2]:
        if st.form_submit_button("🔀 Both", use_container_width=True):
            preset_selected = 'both'

    with preset_cols[3]:
        if st.form_submit_button("⏹️ Neither", use_container_width=True):
            preset_selected = 'neither'

    # Apply preset if selected
    if preset_selected:
        if preset_selected == 'executor_only':
            current_config = {
                'trigger_executor': True,
                'trigger_report': False,
                'executor_wait': True,
                'report_wait': False,
                'executor_timeout_hours': 2,
                'report_timeout_minutes': 10
            }
        elif preset_selected == 'report_only':
            current_config = {
                'trigger_executor': False,
                'trigger_report': True,
                'executor_wait': False,
                'report_wait': False,
                'executor_timeout_hours': 2,
                'report_timeout_minutes': 10
            }
        elif preset_selected == 'both':
            current_config = {
                'trigger_executor': True,
                'trigger_report': True,
                'executor_wait': True,
                'report_wait': False,
                'executor_timeout_hours': 2,
                'report_timeout_minutes': 10
            }
        elif preset_selected == 'neither':
            current_config = {
                'trigger_executor': False,
                'trigger_report': False,
                'executor_wait': False,
                'report_wait': False,
                'executor_timeout_hours': 2,
                'report_timeout_minutes': 10
            }

    st.markdown("---")
    st.markdown("#### 🎯 DAG Triggers")

    col1, col2 = st.columns(2)

    # ===== EXECUTOR CONFIGURATION =====
    with col1:
        st.markdown("##### 🤖 Local Executor")
        st.caption("Executes workflows with Chrome profiles")

        trigger_executor = st.checkbox(
            "Trigger local_executor DAG",
            value=current_config.get('trigger_executor', True),
            key='dag_trigger_executor',
            help="Enable to execute workflows after filtering"
        )

        if trigger_executor:
            executor_wait = st.checkbox(
                "Wait for completion",
                value=current_config.get('executor_wait', True),
                key='dag_executor_wait',
                help="Wait for executor to finish before marking filter_links as complete"
            )

            if executor_wait:
                executor_timeout = st.number_input(
                    "Timeout (hours)",
                    min_value=1,
                    max_value=12,
                    value=current_config.get('executor_timeout_hours', 2),
                    key='dag_executor_timeout',
                    help="Maximum time to wait for executor"
                )
            else:
                executor_timeout = current_config.get('executor_timeout_hours', 2)
                st.info("⚡ Will run asynchronously")
        else:
            executor_wait = False
            executor_timeout = 2
            st.info("⏭️ Will be skipped")

    # ===== REPORT CONFIGURATION =====
    with col2:
        st.markdown("##### 📊 Filter Report")
        st.caption("Generates statistics and emails report")

        trigger_report = st.checkbox(
            "Trigger filter_links_report DAG",
            value=current_config.get('trigger_report', False),
            key='dag_trigger_report',
            help="Enable to generate and send filtering reports"
        )

        if trigger_report:
            report_wait = st.checkbox(
                "Wait for completion",
                value=current_config.get('report_wait', False),
                key='dag_report_wait',
                help="Wait for report to finish before marking filter_links as complete"
            )

            if report_wait:
                report_timeout = st.number_input(
                    "Timeout (minutes)",
                    min_value=1,
                    max_value=60,
                    value=current_config.get('report_timeout_minutes', 10),
                    key='dag_report_timeout',
                    help="Maximum time to wait for report"
                )
            else:
                report_timeout = current_config.get('report_timeout_minutes', 10)
                st.info("⚡ Will run asynchronously")
        else:
            report_wait = False
            report_timeout = 10
            st.info("⏭️ Will be skipped")

    # ===== CONFIGURATION SUMMARY =====
    st.markdown("---")
    st.markdown("#### 📋 Execution Flow Summary")

    # Determine mode
    if trigger_executor and trigger_report:
        mode_icon = "🔀"
        mode_text = "**Both DAGs Enabled**"
        flow_text = "filter_links → local_executor → filter_links_report → end"
    elif trigger_executor:
        mode_icon = "🤖"
        mode_text = "**Executor Only**"
        flow_text = "filter_links → local_executor → end"
    elif trigger_report:
        mode_icon = "📊"
        mode_text = "**Report Only**"
        flow_text = "filter_links → filter_links_report → end"
    else:
        mode_icon = "⏹️"
        mode_text = "**No Downstream DAGs**"
        flow_text = "filter_links → end"

    st.markdown(f"{mode_icon} {mode_text}")
    st.code(flow_text, language=None)

    # Show details
    details_col1, details_col2 = st.columns(2)

    with details_col1:
        if trigger_executor:
            wait_mode = "⏳ Synchronous" if executor_wait else "🚀 Asynchronous"
            st.write(f"• Executor: ✅ {wait_mode}")
            if executor_wait:
                st.write(f"  └─ Timeout: {executor_timeout}h")
        else:
            st.write("• Executor: ⏭️ Skipped")

    with details_col2:
        if trigger_report:
            wait_mode = "⏳ Synchronous" if report_wait else "🚀 Asynchronous"
            st.write(f"• Report: ✅ {wait_mode}")
            if report_wait:
                st.write(f"  └─ Timeout: {report_timeout}m")
        else:
            st.write("• Report: ⏭️ Skipped")

    # Help expandable
    with st.expander("ℹ️ What do these settings mean?"):
        st.markdown("""
        ### DAG Execution Modes

        **🤖 Local Executor**
        - Runs workflows using Chrome profiles
        - Records videos and takes screenshots
        - Use when: You want workflows to actually execute

        **📊 Filter Report**
        - Generates filtering statistics
        - Sends email with filtering results
        - Use when: You want visibility into filter performance

        **⏳ Synchronous (Wait = True)**
        - filter_links waits for downstream DAG to complete
        - Pipeline shows as "running" until downstream finishes
        - Use when: Need confirmation before continuing

        **🚀 Asynchronous (Wait = False)**
        - filter_links triggers downstream and immediately completes
        - Downstream runs independently
        - Use when: Don't need immediate confirmation

        ### Common Scenarios

        **Production** (Executor Only):
        ```
        Executor: ✅ Wait
        Report: ❌ Skip
        ```

        **Monitoring** (Report Only):
        ```
        Executor: ❌ Skip
        Report: ✅ No Wait
        ```

        **Full Pipeline** (Both):
        ```
        Executor: ✅ Wait
        Report: ✅ No Wait
        ```

        **Testing** (Neither):
        ```
        Executor: ❌ Skip
        Report: ❌ Skip
        ```
        """)

    # Return configuration dict
    return {
        'trigger_executor': trigger_executor,
        'trigger_report': trigger_report,
        'executor_wait': executor_wait if trigger_executor else False,
        'report_wait': report_wait if trigger_report else False,
        'executor_timeout_hours': executor_timeout if trigger_executor else 2,
        'report_timeout_minutes': report_timeout if trigger_report else 10
    }


def save_dag_execution_config(config: dict):
    """
    Save DAG execution configuration to MongoDB.

    Args:
        config: Dictionary containing DAG execution configuration
    """
    try:
        from ...settings.settings_manager import update_system_setting

        update_system_setting('filter_links_execution_settings', {
            **config,
            'updated_at': datetime.now().isoformat(),
            'updated_by': 'streamlit_ui_filtering_tab'
        })

        return True
    except Exception as e:
        st.error(f"Error saving DAG execution configuration: {e}")
        return False
