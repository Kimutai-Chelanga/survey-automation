import streamlit as st
from ...settings.settings_manager import get_system_setting, update_system_setting
from datetime import date, timedelta
from typing import Dict, Any, List, Tuple





# File: streamlit/ui/settings/weekly_settings.py
# ADDITIONAL HELPER METHODS NEEDED

def get_weekday_dates(target_weekday: int, num_past: int = 4, num_future: int = 4) -> List[Tuple[date, str]]:
    """
    Get past and future dates for a specific weekday.

    Args:
        target_weekday: 0=Monday, 1=Tuesday, ..., 6=Sunday
        num_past: Number of past occurrences to include
        num_future: Number of future occurrences to include

    Returns:
        List of (date, formatted_string) tuples
    """
    today = date.today()
    current_weekday = today.weekday()

    # Calculate days to the most recent occurrence of target weekday
    days_back = (current_weekday - target_weekday) % 7
    if days_back == 0:
        most_recent = today
    else:
        most_recent = today - timedelta(days=days_back)

    dates = []

    # Add past dates (excluding most recent if it's today)
    for i in range(num_past, 0, -1):
        past_date = most_recent - timedelta(weeks=i)
        dates.append((past_date, f"{past_date.strftime('%b %d, %Y')}"))

    # Add most recent/current
    dates.append((most_recent, f"{most_recent.strftime('%b %d, %Y')} {'(Today)' if most_recent == today else ''}"))

    # Add future dates
    for i in range(1, num_future + 1):
        future_date = most_recent + timedelta(weeks=i)
        dates.append((future_date, f"{future_date.strftime('%b %d, %Y')}"))

    return dates


def check_config_exists(weekly_settings: Dict, day_key: str, config_date: date) -> bool:
    """Check if configuration exists for a specific day and date."""
    day_config = weekly_settings.get(day_key, {})
    if not day_config:
        return False

    stored_date_str = day_config.get('config_date')
    if not stored_date_str:
        return False

    try:
        stored_date = date.fromisoformat(stored_date_str)
        return stored_date == config_date
    except:
        return False


# File: streamlit/ui/settings/weekly_settings.py
# Add this function at the top of the file (with other imports)

def get_available_workflow_types() -> List[str]:
    """
    Get available workflow types from database.
    NO FALLBACKS - raises error if database query fails.
    """
    try:
        from ...settings.settings_manager import get_postgres_prompt_types

        # Get types from database - this will raise RuntimeError if fails
        prompt_types = get_postgres_prompt_types()

        if not prompt_types:
            raise RuntimeError("CRITICAL: No prompt types found in database. Please create prompts first.")

        return prompt_types
    except RuntimeError as re:
        raise re  # Re-raise RuntimeError
    except Exception as e:
        raise RuntimeError(f"CRITICAL: Error loading prompt types: {e}")


# Also make sure you have these imports at the top of your weekly_settings.py file:


def render_weekly_settings_overview() -> None:
    """Render weekly-organized settings overview with day slider."""
    st.header("Weekly Settings Overview")
    st.markdown("*Configure your workflow settings by day of the week*")

    # ------------------------------------------------------------------ #
    # Refresh button
    # ------------------------------------------------------------------ #
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("Refresh", type="secondary"):
            st.cache_data.clear()  # Clear cache to reload prompt types
            st.rerun()

    # ------------------------------------------------------------------ #
    # Load current settings
    # ------------------------------------------------------------------ #
    try:
        weekly_settings: Dict[str, Dict[str, Any]] = get_system_setting(
            "weekly_workflow_settings", {}
        )
    except Exception as e:
        st.error(f"❌ CRITICAL: Error loading weekly settings: {e}")
        return

    # ------------------------------------------------------------------ #
    # Get available workflow types from database - NO FALLBACKS
    # ------------------------------------------------------------------ #
    try:
        available_workflow_types = get_available_workflow_types()

        if not available_workflow_types:
            st.error("❌ CRITICAL: Cannot configure weekly settings without content types. Please create prompts first.")
            return

    except RuntimeError as re:
        st.error(f"❌ CRITICAL: {re}")
        return
    except Exception as e:
        st.error(f"❌ CRITICAL: Failed to load workflow types: {e}")
        return

    # ------------------------------------------------------------------ #
    # Show content names information
    # ------------------------------------------------------------------ #
    with st.expander("📋 Available Content Names", expanded=False):
        try:
            content_name_stats = {}
            for content_type in available_workflow_types:
                try:
                    names = get_content_names_by_type(content_type)
                    if names:
                        content_name_stats[content_type] = {
                            'count': len(names),
                            'names': names[:5]  # Show first 5 only
                        }
                except RuntimeError:
                    # Skip if we can't load for this type
                    continue

            if content_name_stats:
                st.write("**Content names by type:**")
                for content_type, stats in content_name_stats.items():
                    with st.expander(f"{content_type} ({stats['count']} names)"):
                        st.write(f"Sample: {', '.join(stats['names'])}")
                        if stats['count'] > 5:
                            st.caption(f"... and {stats['count'] - 5} more")
            else:
                st.info("No content names found in database. You can create new ones.")

        except Exception as e:
            st.warning(f"Note: Could not load content names: {e}")
            st.info("Content names will be available after content is created.")

    # ------------------------------------------------------------------ #
    # Days definition
    # ------------------------------------------------------------------ #
    days_of_week = [
        ("monday", "Monday", "📅", 0),
        ("tuesday", "Tuesday", "📅", 1),
        ("wednesday", "Wednesday", "📅", 2),
        ("thursday", "Thursday", "📅", 3),
        ("friday", "Friday", "📅", 4),
        ("saturday", "Saturday", "📅", 5),
        ("sunday", "Sunday", "📅", 6),
    ]

    # ------------------------------------------------------------------ #
    # Day tabs
    # ------------------------------------------------------------------ #
    st.markdown("#### Select Day to Configure")
    day_tabs = st.tabs([f"{emoji} {name}" for _, name, emoji, _ in days_of_week])

    # ------------------------------------------------------------------ #
    # Render each day inside its tab
    # ------------------------------------------------------------------ #
    for tab_index, (day_key, day_name, day_emoji, weekday_num) in enumerate(days_of_week):
        with day_tabs[tab_index]:
            day_config = weekly_settings.get(day_key, {})

            st.markdown(f"### {day_emoji} {day_name} Configuration")

            # Get available dates for this weekday
            available_dates = get_weekday_dates(weekday_num)

            with st.form(f"day_config_{day_key}"):
                # Date selection dropdown
                date_options = [date_str for _, date_str in available_dates]
                date_values = [d for d, _ in available_dates]

                # Try to find current config date in the list
                current_date = day_config.get('config_date')
                if current_date:
                    try:
                        current_date_obj = date.fromisoformat(current_date)
                        if current_date_obj in date_values:
                            default_index = date_values.index(current_date_obj)
                        else:
                            default_index = next((i for i, d in enumerate(date_values) if d >= date.today()), 0)
                    except:
                        default_index = next((i for i, d in enumerate(date_values) if d >= date.today()), 0)
                else:
                    default_index = next((i for i, d in enumerate(date_values) if d >= date.today()), 0)

                selected_date_str = st.selectbox(
                    "Configuration Date",
                    options=date_options,
                    index=default_index,
                    help=f"Select a {day_name} to configure (showing last 4 and next 4 occurrences)"
                )

                # Get the actual date object
                selected_index = date_options.index(selected_date_str)
                config_date = date_values[selected_index]

                # Check if configuration exists for this date
                config_exists = check_config_exists(weekly_settings, day_key, config_date)

                if config_exists:
                    st.success(f"✅ Configuration exists for {config_date.strftime('%B %d, %Y')}")
                else:
                    st.info(f"ℹ️ No configuration found for {config_date.strftime('%B %d, %Y')}")

                st.markdown("---")

                # ============================================
                # CONTENT CREATION SETTINGS WITH CONTENT NAME
                # ============================================
                st.markdown("### Content Creation")
                content_col1, content_col2, content_col3 = st.columns(3)

                with content_col1:
                    # Get current workflow type and validate it exists
                    current_workflow_type = day_config.get('workflow_type', available_workflow_types[0])

                    # If stored type doesn't exist in database, use first available
                    if current_workflow_type not in available_workflow_types:
                        current_workflow_type = available_workflow_types[0]

                    # Get index safely
                    try:
                        workflow_type_index = available_workflow_types.index(current_workflow_type)
                    except ValueError:
                        workflow_type_index = 0

                    workflow_type = st.selectbox(
                        "Content Type",
                        options=available_workflow_types,
                        index=workflow_type_index,
                        help="Select content type to generate",
                        key=f"workflow_type_{day_key}"
                    )

                with content_col2:
                    # CONTENT NAME SELECTION - NO FALLBACKS
                    try:
                        # Get existing content names for the selected type
                        existing_names = get_content_names_by_type(workflow_type)

                        # Get current content name
                        current_content_name = day_config.get('content_name', '')

                        if existing_names:
                            name_options = ["➕ New Content Name"] + existing_names

                            content_name_selection = st.selectbox(
                                "Content Name",
                                options=name_options,
                                index=0 if not current_content_name or current_content_name not in existing_names else name_options.index(current_content_name),
                                help="Select existing content name or create new",
                                key=f"content_name_select_{day_key}"
                            )

                            if content_name_selection == "➕ New Content Name":
                                content_name = st.text_input(
                                    "New Content Name",
                                    value=current_content_name if current_content_name not in existing_names else "",
                                    help="Enter name for this content",
                                    key=f"content_name_input_{day_key}"
                                )
                            else:
                                content_name = content_name_selection
                                st.info(f"📋 Using: {content_name}")
                        else:
                            content_name = st.text_input(
                                "Content Name",
                                value=current_content_name,
                                help="Enter name for this content",
                                key=f"content_name_{day_key}"
                            )

                    except RuntimeError as re:
                        st.error(f"❌ CRITICAL: {re}")
                        content_name = st.text_input(
                            "Content Name",
                            value=day_config.get('content_name', ''),
                            help="Enter name for this content",
                            key=f"content_name_fallback_{day_key}"
                        )
                    except Exception as e:
                        st.error(f"❌ Error loading content names: {e}")
                        return

                with content_col3:
                    content_amount = st.number_input(
                        f"Number of {workflow_type.capitalize()}",
                        min_value=1,
                        max_value=100,
                        value=day_config.get('content_amount', 5),
                        step=1,
                        help=f"Number of {workflow_type} to create",
                        key=f"content_amount_{day_key}"
                    )

                st.markdown("---")

                # ============================================
                # FILTERING SETTINGS
                # ============================================
                st.markdown("### Filtering Configuration")
                filter_amount = st.number_input(
                    "Amount to Filter",
                    min_value=1,
                    max_value=10000,
                    value=day_config.get('filter_amount', 100),
                    step=1,
                    help="Number of items to filter",
                    key=f"filter_amount_{day_key}"
                )

                st.markdown("---")

                # ============================================
                # EXTRACTION & PROCESSING SETTINGS
                # ============================================
                st.markdown("### Extraction & Processing")
                extract_col1, extract_col2 = st.columns(2)

                with extract_col1:
                    extraction_window = st.number_input(
                        "Extraction Window (hours)",
                        min_value=1,
                        max_value=168,
                        value=day_config.get('extraction_window', 24),
                        step=1,
                        help="How far back to look for content",
                        key=f"extraction_window_{day_key}"
                    )

                    content_to_process = st.number_input(
                        "Content Items to Process",
                        min_value=1,
                        max_value=100,
                        value=day_config.get('content_to_process', 19),
                        step=1,
                        help="Maximum items to process",
                        key=f"content_to_process_{day_key}"
                    )

                with extract_col2:
                    # Gap between workflows
                    saved_gap = day_config.get('gap_between_workflows', 15)
                    gap_default = min(saved_gap, 120)
                    if saved_gap > 120:
                        st.warning(f"Saved gap ({saved_gap} min) exceeds UI limit. Capped at 120 min.")

                    gap_between_workflows = st.number_input(
                        "Gap Between Workflows (minutes)",
                        min_value=1,
                        max_value=120,
                        value=gap_default,
                        step=1,
                        help="Time delay between workflows",
                        key=f"gap_workflows_{day_key}"
                    )

                    words_to_filter = st.text_input(
                        "Filter Keywords (comma-separated)",
                        value=day_config.get('words_to_filter', ''),
                        help="Keywords to filter out",
                        key=f"words_filter_{day_key}"
                    )

                st.markdown("---")

                # ============================================
                # EXECUTION SETTINGS
                # ============================================
                st.markdown("### Execution Configuration")
                exec_col1, exec_col2 = st.columns(2)

                with exec_col1:
                    time_between_workflows = st.number_input(
                        "Time Between Workflows (seconds)",
                        min_value=1,
                        max_value=300,
                        value=day_config.get('time_between_workflows', 15),
                        step=1,
                        help="Delay between workflow executions",
                        key=f"time_workflows_{day_key}"
                    )

                    daily_limit = st.number_input(
                        "Daily Limit",
                        min_value=1,
                        max_value=5000,
                        value=day_config.get('daily_limit', 200),
                        step=10,
                        help="Maximum executions per day",
                        key=f"daily_limit_{day_key}"
                    )

                with exec_col2:
                    saved_time_limit = day_config.get('time_limit', 2.0)
                    time_limit = st.slider(
                        "Time Limit (hours)",
                        min_value=0.1,
                        max_value=24.0,
                        value=float(saved_time_limit),
                        step=0.1,
                        help="Maximum execution time per day",
                        key=f"time_limit_{day_key}"
                    )

                st.markdown("---")

                # ============================================
                # CONFIGURATION PREVIEW WITH CONTENT NAME
                # ============================================
                st.markdown("### Configuration Summary")
                summary_col1, summary_col2, summary_col3 = st.columns(3)

                with summary_col1:
                    st.markdown("**Content & Filtering**")
                    st.write(f"• Type: {workflow_type.capitalize()}")
                    st.write(f"• Name: {content_name}")
                    st.write(f"• Amount: {content_amount}")
                    st.write(f"• Filter: {filter_amount}")

                with summary_col2:
                    st.markdown("**Extraction**")
                    st.write(f"• Window: {extraction_window}h")
                    st.write(f"• Process: {content_to_process}")
                    st.write(f"• Gap: {gap_between_workflows}min")
                    if words_to_filter:
                        st.write(f"• Keywords: {words_to_filter}")

                with summary_col3:
                    st.markdown("**Execution**")
                    st.write(f"• Workflow Gap: {time_between_workflows}s")
                    st.write(f"• Daily Limit: {daily_limit}")
                    st.write(f"• Time Limit: {time_limit}h")

                # Submit button
                if st.form_submit_button(
                    f"Save {day_name} Configuration",
                    type="primary",
                    use_container_width=True,
                ):
                    try:
                        # VALIDATE BEFORE SAVING - NO FALLBACKS
                        validate_content_configuration(
                            workflow_type,
                            content_name,
                            content_amount,
                            available_workflow_types
                        )

                        # Build day settings with content name
                        day_settings: Dict[str, Any] = {
                            'config_date': config_date.isoformat(),
                            'day_name': day_name,
                            'day_key': day_key,

                            # Content Creation
                            'workflow_type': workflow_type,
                            'content_name': content_name.strip(),
                            'content_amount': content_amount,

                            # Filtering
                            'filter_amount': filter_amount,

                            # Extraction & Processing
                            'extraction_window': extraction_window,
                            'content_to_process': content_to_process,
                            'gap_between_workflows': gap_between_workflows,
                            'gap_between_workflows_seconds': gap_between_workflows * 60,
                            'words_to_filter': words_to_filter.strip(),

                            # Execution
                            'time_between_workflows': time_between_workflows,
                            'daily_limit': daily_limit,
                            'time_limit': float(time_limit),

                            # Timestamps
                            'updated_at': datetime.now().isoformat()
                        }

                        weekly_settings[day_key] = day_settings
                        update_system_setting("weekly_workflow_settings", weekly_settings)

                        st.success(f"{day_name} configuration updated successfully for {config_date.strftime('%B %d, %Y')}!")
                        st.info(f"📝 Content: {content_name} ({content_amount} {workflow_type})")
                        st.balloons()

                        # Clear cache to refresh types
                        st.cache_data.clear()

                    except ValueError as ve:
                        st.error(f"❌ Validation Error: {ve}")
                    except Exception as e:
                        st.error(f"❌ CRITICAL: Error updating {day_name} configuration: {e}")
                        import traceback
                        st.error(traceback.format_exc())
