"""
Extraction Configuration - Main Settings
=========================================
- Configurable schedule interval (cron expression)
- Flexible time windows (any duration)
- Independent from filtering windows
"""

import streamlit as st
from datetime import datetime, timedelta, date
from src.streamlit.ui.settings.settings_manager import (
    get_system_setting,
    update_system_setting,
    get_extraction_setting,
    update_extraction_setting
)
import re
import logging

logger = logging.getLogger(__name__)

# Default extraction windows
DEFAULT_EXTRACTION_WINDOWS = {
    "enabled": True,
    "morning_window": "05:00-09:00",
    "evening_window": "17:00-21:00",
}

# Default schedule options
SCHEDULE_OPTIONS = {
    "Every 5 minutes": "*/5 * * * *",
    "Every 10 minutes": "*/10 * * * *",
    "Every 15 minutes": "*/15 * * * *",
    "Every 20 minutes": "*/20 * * * *",
    "Every 30 minutes": "*/30 * * * *",
    "Every hour": "0 * * * *",
    "Every 2 hours": "0 */2 * * *",
    "Every 3 hours": "0 */3 * * *",
    "Every 4 hours": "0 */4 * * *",
    "Every 6 hours": "0 */6 * * *",
    "Every 12 hours": "0 */12 * * *",
    "Custom cron": "custom",
}


def validate_cron_expression(cron_expr: str) -> bool:
    """Basic validation for cron expressions"""
    if not cron_expr or not isinstance(cron_expr, str):
        return False
    
    parts = cron_expr.split()
    if len(parts) != 5:
        return False
    
    # Simple validation - check each part has valid characters
    valid_chars = set('0123456789*-/,')
    for part in parts:
        if not all(c in valid_chars or c.isdigit() for c in part):
            return False
    
    return True


def parse_time_window(window_str: str) -> tuple:
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
        logger.error(f"Error parsing time window '{window_str}': {e}")
        return None, None, None


def _is_time_in_window(check_time: datetime, window_str: str) -> bool:
    """Helper to check if time is within a window"""
    if not window_str:
        return False
    
    try:
        s, e = window_str.split('-')
        start = datetime.strptime(s.strip(), '%H:%M').time()
        end = datetime.strptime(e.strip(), '%H:%M').time()
        current = check_time.time()
        
        if start <= end:
            return start <= current <= end
        else:
            return current >= start or current <= end
    except:
        return False


def render_extraction_processing_config():
    """Main extraction configuration UI"""
    st.header("⏰ Extraction Processing Configuration")
    st.markdown("*Configure schedule frequency and time windows for extraction*")
    st.markdown("*These settings are independent from filtering windows*")

    # Get existing settings from both system and extraction categories
    system_cfg = get_system_setting('extraction_schedule_settings', {})
    
    # Try to get from extraction category first (new structure)
    extraction_cfg = get_extraction_setting('extraction_schedule_settings', {})
    
    # Merge, with extraction category taking precedence
    if extraction_cfg:
        system_cfg.update(extraction_cfg)
    
    # Get weekly workflow settings for day-specific windows
    weekly_cfg = get_system_setting('weekly_workflow_settings', {})

    with st.form("extraction_config_form"):
        # =====================================================================
        # SCHEDULE INTERVAL
        # =====================================================================
        st.markdown("### ⏱️ Schedule Frequency")
        st.caption("How often should the extraction DAG run? (It will still check time windows)")

        current_schedule = system_cfg.get('schedule_interval', '0 * * * *')
        
        # Find which preset matches current schedule
        current_preset = "Custom cron"
        for name, expr in SCHEDULE_OPTIONS.items():
            if expr == current_schedule:
                current_preset = name
                break

        selected_preset = st.selectbox(
            "Run frequency",
            options=list(SCHEDULE_OPTIONS.keys()),
            index=list(SCHEDULE_OPTIONS.keys()).index(current_preset) if current_preset in SCHEDULE_OPTIONS else 0,
            help="Select how often the DAG should be triggered"
        )

        custom_cron = current_schedule
        if selected_preset == "Custom cron":
            custom_cron = st.text_input(
                "Custom cron expression",
                value=current_schedule if current_preset == "Custom cron" else "0 * * * *",
                help="Enter a valid cron expression (e.g., '*/15 * * * *' for every 15 minutes)"
            )
            
            if custom_cron and not validate_cron_expression(custom_cron):
                st.error("❌ Invalid cron expression")
            else:
                st.code(f"Current schedule: {custom_cron}", language="text")
                
                # Show human-readable description
                if custom_cron == "*/5 * * * *":
                    st.info("📅 Runs every 5 minutes")
                elif custom_cron == "*/10 * * * *":
                    st.info("📅 Runs every 10 minutes")
                elif custom_cron == "*/15 * * * *":
                    st.info("📅 Runs every 15 minutes")
                elif custom_cron == "*/20 * * * *":
                    st.info("📅 Runs every 20 minutes")
                elif custom_cron == "*/30 * * * *":
                    st.info("📅 Runs every 30 minutes")
                elif custom_cron == "0 * * * *":
                    st.info("📅 Runs every hour")
                elif custom_cron == "0 */2 * * *":
                    st.info("📅 Runs every 2 hours")
                else:
                    st.info(f"📅 Custom schedule: {custom_cron}")
        else:
            custom_cron = SCHEDULE_OPTIONS[selected_preset]
            schedule_desc = selected_preset
            st.info(f"📅 Schedule: **{schedule_desc}**")
            st.code(f"Cron: {custom_cron}", language="text")

        st.markdown("---")

        # =====================================================================
        # EXTRACTION TIME WINDOWS
        # =====================================================================
        st.markdown("### 🕐 Extraction Time Windows")
        st.caption(
            "Configure when extraction should actually run. "
            "The DAG will run at the frequency above, but only execute if current time is within these windows."
        )

        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        
        # Use columns for better layout
        col1, col2, col3, col4 = st.columns([1, 1, 2, 2])
        with col1:
            st.markdown("**Day**")
        with col2:
            st.markdown("**Enabled**")
        with col3:
            st.markdown("**Morning Window**")
        with col4:
            st.markdown("**Evening Window**")
        
        st.markdown("---")

        updated_windows = {}
        window_errors = []

        for day in days:
            day_cfg = weekly_cfg.get(day, {})
            current_windows = {
                "enabled": day_cfg.get("enabled", True),
                "morning_window": day_cfg.get("morning_window", DEFAULT_EXTRACTION_WINDOWS["morning_window"]),
                "evening_window": day_cfg.get("evening_window", DEFAULT_EXTRACTION_WINDOWS["evening_window"]),
            }

            col1, col2, col3, col4 = st.columns([1, 1, 2, 2])
            
            with col1:
                st.markdown(f"**{day.title()}**")
            
            with col2:
                enabled = st.checkbox(
                    "Enable",
                    value=current_windows["enabled"],
                    key=f"ext_enabled_{day}",
                    label_visibility="collapsed",
                )
            
            with col3:
                morning = st.text_input(
                    f"Morning {day}",
                    value=current_windows["morning_window"],
                    key=f"ext_morning_{day}",
                    label_visibility="collapsed",
                    disabled=not enabled,
                    placeholder="05:00-09:00",
                )
                
                if morning and enabled:
                    start, end, duration = parse_time_window(morning)
                    if start and end:
                        if start <= end:
                            st.caption(f"✓ {duration:.1f}h window")
                        else:
                            st.caption(f"✓ {duration:.1f}h (crosses midnight)")
                    else:
                        st.caption("❌ Invalid format")
                        window_errors.append(day)
            
            with col4:
                evening = st.text_input(
                    f"Evening {day}",
                    value=current_windows["evening_window"],
                    key=f"ext_evening_{day}",
                    label_visibility="collapsed",
                    disabled=not enabled,
                    placeholder="17:00-21:00",
                )
                
                if evening and enabled:
                    start, end, duration = parse_time_window(evening)
                    if start and end:
                        if start <= end:
                            st.caption(f"✓ {duration:.1f}h window")
                        else:
                            st.caption(f"✓ {duration:.1f}h (crosses midnight)")
                    else:
                        st.caption("❌ Invalid format")
                        window_errors.append(day)

            updated_windows[day] = {
                "enabled": enabled,
                "morning_window": morning if enabled else "",
                "evening_window": evening if enabled else "",
            }

        st.markdown("---")

        # =====================================================================
        # PREVIEW SCHEDULE
        # =====================================================================
        if not window_errors:
            st.markdown("### 📋 Schedule Preview")
            
            # Show next few runs based on cron
            try:
                from croniter import croniter
                base_time = datetime.now()
                cron = croniter(custom_cron, base_time)
                
                st.markdown("**Next scheduled DAG triggers:**")
                next_runs = []
                for i in range(5):
                    next_time = cron.get_next(datetime)
                    day_name = next_time.strftime('%A').lower()
                    
                    # Check if this trigger time is within windows
                    day_windows = updated_windows.get(day_name, {})
                    if day_windows.get("enabled", True):
                        in_morning = _is_time_in_window(next_time, day_windows.get("morning_window", ""))
                        in_evening = _is_time_in_window(next_time, day_windows.get("evening_window", ""))
                        
                        if in_morning or in_evening:
                            status = "✅ Will execute"
                        else:
                            status = "⏸️ Will skip (outside windows)"
                    else:
                        status = "⏸️ Day disabled"
                    
                    next_runs.append(f"  • {next_time.strftime('%Y-%m-%d %H:%M')} - {status}")
                
                for run in next_runs:
                    st.write(run)
                    
            except Exception as e:
                st.warning(f"Could not generate preview: {e}")

        st.markdown("---")

        # =====================================================================
        # SAVE BUTTON
        # =====================================================================
        if st.form_submit_button("💾 Save Extraction Configuration", type="primary", use_container_width=True):
            if window_errors:
                st.error(f"❌ Fix errors on: {', '.join(window_errors)}")
            else:
                try:
                    # Save schedule settings to both locations for compatibility
                    schedule_settings = {
                        'schedule_interval': custom_cron,
                        'schedule_description': selected_preset if selected_preset != "Custom cron" else f"Custom: {custom_cron}",
                        'updated_at': datetime.now().isoformat(),
                    }
                    
                    # Save to system category (for DAG)
                    update_system_setting('extraction_schedule_settings', schedule_settings)
                    
                    # Save to extraction category (for UI)
                    update_extraction_setting('extraction_schedule_settings', schedule_settings)
                    
                    # Save window settings to weekly_workflow_settings
                    current_weekly = get_system_setting('weekly_workflow_settings', {})
                    for day, windows in updated_windows.items():
                        if day not in current_weekly:
                            current_weekly[day] = {}
                        current_weekly[day].update(windows)
                    
                    update_system_setting('weekly_workflow_settings', current_weekly)
                    
                    st.success("✅ Extraction configuration saved successfully!")
                    st.info(f"Schedule: {schedule_settings['schedule_description']}")
                    st.balloons()
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Failed to save: {e}")

    # =========================================================================
    # CURRENT SETTINGS DISPLAY
    # =========================================================================
    st.markdown("---")
    st.markdown("### 📊 Current Configuration")

    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Schedule Settings**")
        current_schedule = system_cfg.get('schedule_interval', '0 * * * *')
        current_desc = system_cfg.get('schedule_description', 'Every hour')
        st.info(f"**Frequency:** {current_desc}")
        st.code(f"Cron: {current_schedule}")

    with col2:
        st.markdown("**Next 5 Trigger Times**")
        try:
            from croniter import croniter
            base_time = datetime.now()
            cron = croniter(current_schedule, base_time)
            for i in range(5):
                next_time = cron.get_next(datetime)
                st.write(f"{next_time.strftime('%H:%M %m/%d')}")
        except:
            st.write("Could not calculate")
    
    st.markdown("---")
    st.info("""
    ℹ️ **How Extraction Windows Work:**
    
    1. **Schedule Frequency**: The DAG triggers at your configured interval (e.g., every 15 min)
    2. **Time Windows**: Extraction only runs if current time is within a configured window
    3. **Filtering Windows**: After extraction completes, it checks filtering windows before triggering filter_links
    
    This means you can have:
    - Extraction checking for new tweets every 15 minutes
    - But only actually extracting during specific hours (e.g., 5-9am, 5-9pm)
    - Filtering running during completely different hours (e.g., 6-10am, 6-10pm)
    """)
