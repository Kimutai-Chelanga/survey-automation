# File: streamlit/ui/settings/create_content.py
# REFACTORED VERSION - Choose number of content types FIRST, then show form with PROMPT SELECTION
# UPDATED: Added run schedule configuration (number of runs per day + time picker per run)
# UPDATED: Added "Apply to All Week" — save same config to all 7 days in one click

import streamlit as st
from datetime import datetime, timedelta, date
from typing import Dict, Any, List

# Import database functions
from ...settings.settings_manager import (
    get_system_setting,
    update_system_setting,
    get_postgres_prompt_types,
    get_cached_prompt_types
)


def get_future_dates(num_dates: int = 8) -> list:
    """Get future dates starting from today."""
    dates = []
    today = datetime.now().date()
    for i in range(num_dates):
        future_date = today + timedelta(days=i)
        date_str = future_date.strftime('%b %d, %Y')
        if i == 0:
            date_str += " (Today)"
        dates.append((future_date, date_str))
    return dates


def get_day_key_from_date(target_date: date) -> str:
    """Get the day key (monday, tuesday, etc.) from a date."""
    weekday = target_date.weekday()
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    return days[weekday]


def check_schedule_exists(weekly_settings: Dict, target_date: date) -> tuple:
    """Check if a schedule exists for the given date."""
    day_key = get_day_key_from_date(target_date)
    day_config = weekly_settings.get(day_key, {})

    if not day_config:
        return (False, day_key, {})

    stored_date_str = day_config.get('config_date')
    if not stored_date_str:
        return (False, day_key, day_config)

    try:
        stored_date = date.fromisoformat(stored_date_str)
        if stored_date == target_date:
            return (True, day_key, day_config)
        else:
            return (False, day_key, {})
    except:
        return (False, day_key, {})


def get_prompts_by_type(content_type: str) -> List[Dict[str, Any]]:
    """Get all prompts for a specific content type with their details."""
    try:
        from src.core.database.postgres.connection import get_postgres_connection
        from psycopg2.extras import RealDictCursor

        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        p.prompt_id,
                        p.name as prompt_name,
                        p.account_id,
                        a.username,
                        p.content,
                        p.is_active
                    FROM prompts p
                    LEFT JOIN accounts a ON p.account_id = a.account_id
                    WHERE p.prompt_type = %s
                    AND p.is_active = TRUE
                    ORDER BY a.username, p.name
                """, (content_type,))

                results = cursor.fetchall()
                return [dict(row) for row in results]

    except Exception as e:
        st.error(f"Error fetching prompts for type '{content_type}': {e}")
        return []


def get_content_names_by_type(content_type: str) -> List[str]:
    """Get content names for a specific content type."""
    try:
        from src.core.database.postgres.connection import get_postgres_connection
        from psycopg2.extras import RealDictCursor

        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT DISTINCT content_name
                    FROM content
                    WHERE content_type = %s
                    AND content_name IS NOT NULL
                    AND content_name != ''
                    ORDER BY content_name
                """, (content_type,))

                results = cursor.fetchall()
                content_names = [row['content_name'] for row in results if row['content_name']]
                return sorted(list(set(content_names)))

    except Exception as e:
        st.error(f"Error fetching content names: {e}")
        return []


def validate_content_configuration(
    workflow_type: str,
    content_name: str,
    content_amount: int,
    prompt_name: str = None
) -> bool:
    """Validate content configuration before saving."""
    all_types = get_postgres_prompt_types()

    if not all_types:
        raise ValueError("CRITICAL: No content types found in database")
    if workflow_type not in all_types:
        raise ValueError(f"CRITICAL: Content type '{workflow_type}' not found in database.")
    if not content_name or not content_name.strip():
        raise ValueError("CRITICAL: Content name cannot be empty")
    if len(content_name.strip()) < 2:
        raise ValueError("CRITICAL: Content name must be at least 2 characters")
    if len(content_name.strip()) > 255:
        raise ValueError("CRITICAL: Content name cannot exceed 255 characters")
    if not isinstance(content_amount, int):
        raise ValueError("CRITICAL: Content amount must be an integer")
    if content_amount < 1:
        raise ValueError("CRITICAL: Content amount must be at least 1")
    if content_amount > 100:
        raise ValueError("CRITICAL: Content amount cannot exceed 100")
    if prompt_name and not prompt_name.strip():
        raise ValueError("CRITICAL: Prompt name cannot be empty")

    return True


def get_run_schedule_setting() -> Dict[str, Any]:
    """Get the current run schedule from the database."""
    return get_system_setting('content_run_schedule', {
        'num_runs':  2,
        'run_times': ['02:00', '03:00']
    })


def save_run_schedule_setting(schedule: Dict[str, Any]):
    """Persist the run schedule to the database."""
    update_system_setting('content_run_schedule', schedule)


# ============================================================================
# RUN SCHEDULE SECTION
# ============================================================================

def render_run_schedule_section():
    """
    Let the user choose how many times per day the content DAG runs
    and pick an exact HH:MM time for each run.
    Saves to 'content_run_schedule' system setting.
    Returns the current saved schedule dict.
    """
    st.markdown("---")
    st.markdown("""
        <div style='background-color: rgba(33, 150, 243, 0.1);
                    padding: 15px;
                    border-radius: 8px;
                    border-left: 4px solid #2196F3;
                    margin: 20px 0;'>
            <h3 style='color: #1565C0; margin: 0;'>⏰ Run Schedule</h3>
            <p style='margin: 5px 0 0 0; color: #555;'>
                Choose how many times per day the content DAG runs, then set a time for each run.
            </p>
        </div>
    """, unsafe_allow_html=True)

    current_schedule  = get_run_schedule_setting()
    current_num_runs  = int(current_schedule.get('num_runs', 1))
    current_run_times = current_schedule.get('run_times', ['08:00'])

    with st.form("run_schedule_form", clear_on_submit=False):

        num_runs = st.number_input(
            "Number of runs per day",
            min_value=1,
            max_value=6,
            value=current_num_runs,
            step=1,
            help="How many times per day should the content DAG run? (max 6)"
        )

        st.markdown(f"**Set a time for each of your {int(num_runs)} run(s):**")
        st.caption("Enter any hour (0–23) and minute (0–59) — full 24-hour control.")

        run_times = []

        for i in range(int(num_runs)):
            # Parse saved time or spread defaults evenly through the day
            if i < len(current_run_times):
                try:
                    saved_h, saved_m = current_run_times[i].split(':')
                    default_h, default_m = int(saved_h), int(saved_m)
                except Exception:
                    default_h, default_m = min(8 + i * 4, 22), 0
            else:
                default_h, default_m = min(8 + i * 4, 22), 0

            st.markdown(f"""
                <div style='background: rgba(33,150,243,0.08);
                            border-radius: 8px;
                            padding: 10px 14px 6px 14px;
                            margin: 10px 0 4px 0;
                            border: 1px solid rgba(33,150,243,0.25);'>
                    <span style='font-size:13px; font-weight:600; color:#1565C0;'>
                        🕐 Run {i + 1}
                    </span>
                </div>
            """, unsafe_allow_html=True)

            col_h, col_sep, col_m, col_preview = st.columns([2, 0.3, 2, 3])

            with col_h:
                hour = st.number_input(
                    "Hour (0–23)",
                    min_value=0,
                    max_value=23,
                    value=default_h,
                    step=1,
                    key=f"run_hour_{i}",
                    help="24-hour format: 0 = midnight, 13 = 1pm, 23 = 11pm"
                )

            with col_sep:
                st.markdown("<div style='padding-top:32px; text-align:center; font-size:22px; font-weight:700; color:#1565C0;'>:</div>", unsafe_allow_html=True)

            with col_m:
                minute = st.number_input(
                    "Minute (0–59)",
                    min_value=0,
                    max_value=59,
                    value=default_m,
                    step=1,
                    key=f"run_minute_{i}",
                    help="Any minute from 0 to 59"
                )

            formatted = f"{int(hour):02d}:{int(minute):02d}"
            run_times.append(formatted)

            with col_preview:
                # Show a friendly 12-hour preview
                h24 = int(hour)
                period = "AM" if h24 < 12 else "PM"
                h12 = h24 % 12 or 12
                st.markdown(
                    f"<div style='padding-top:28px; font-size:15px; font-weight:600; color:#2e7d32;'>"
                    f"⏰ {formatted} &nbsp;·&nbsp; {h12}:{int(minute):02d} {period}"
                    f"</div>",
                    unsafe_allow_html=True
                )

        # Cron preview
        if run_times:
            st.markdown("---")
            st.markdown("**📋 Cron Preview** *(what will be set on the DAG)*")
            prev_cols = st.columns(len(run_times))
            for i, rt in enumerate(run_times):
                h, m = rt.split(':')
                with prev_cols[i]:
                    st.code(f"Run {i+1}  {rt}\n{m} {h} * * *", language=None)

        saved = st.form_submit_button(
            "💾 Save Run Schedule",
            type="primary",
            use_container_width=True
        )

        if saved:
            if len(run_times) != len(set(run_times)):
                st.error("❌ Each run must have a unique time. Please fix duplicate times.")
            else:
                sorted_times = sorted(run_times)
                new_schedule = {
                    'num_runs':   int(num_runs),
                    'run_times':  sorted_times,
                    'updated_at': datetime.now().isoformat()
                }
                save_run_schedule_setting(new_schedule)
                st.success(
                    f"✅ Schedule saved: **{int(num_runs)}** run(s) per day "
                    f"at **{', '.join(sorted_times)}**"
                )
                st.info(
                    "ℹ️ The DAG file reads this setting on startup. "
                    "Re-deploy or restart the DAG for cron changes to take effect in Airflow."
                )
                st.rerun()

    return get_run_schedule_setting()


# ============================================================================
# HELPER: Save config to a single day key
# ============================================================================

def _build_day_config(
    existing_fresh: Dict,
    day_key: str,
    day_name: str,
    config_date_iso: str,
    content_configurations: List[Dict],
) -> Dict:
    """Build a day config dict, preserving created_at if it already exists."""
    dc = existing_fresh.get(day_key, {})
    dc.update({
        'config_date':   config_date_iso,
        'day_name':      day_name,
        'day_key':       day_key,
        'content_types': content_configurations,
        'updated_at':    datetime.now().isoformat(),
    })
    if 'created_at' not in dc:
        dc['created_at'] = datetime.now().isoformat()
    return dc


# ============================================================================
# MAIN RENDER
# ============================================================================

def render_create_content_config():
    """Render Create Content configuration."""
    st.header("📝 Create Content Configuration")
    st.markdown("*Configure multiple content types for a single date — or apply to the whole week at once*")

    # ── Run Schedule — always at the top ─────────────────────────────────────
    current_schedule = render_run_schedule_section()

    active_times = current_schedule.get('run_times', [])
    if active_times:
        st.info(
            f"🕐 **Active Schedule:** "
            f"**{len(active_times)}** run(s) per day — "
            f"{' · '.join(active_times)}"
        )

    st.markdown("---")

    # ── Content config ────────────────────────────────────────────────────────
    st.session_state.num_content_types_confirmed = 3

    emoji_map = {
        'messages': '💬',
        'replies':  '📝',
        'retweets': '🔄',
        'tweets':   '🐦',
        'posts':    '📮',
        'twitter':  '🐦',
    }

    DAYS_ORDER = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    DAY_NAMES  = {d: d.capitalize() for d in DAYS_ORDER}

    try:
        try:
            all_content_types = get_postgres_prompt_types()
            if not all_content_types:
                st.error("❌ CRITICAL: No content types found in database")
                st.info(
                    "💡 Go to **Prompts** → **Create New Prompt** "
                    "to add your first content type."
                )
                return
            st.success(f"✅ Found **{len(all_content_types)} content type(s)** in database")
        except Exception as e:
            st.error(f"❌ CRITICAL: Failed to load content types: {e}")
            return

        def get_all_prompts_across_types() -> List[Dict[str, Any]]:
            all_prompts = []
            for ct in all_content_types:
                for p in get_prompts_by_type(ct):
                    p['_content_type'] = ct
                    all_prompts.append(p)
            return all_prompts

        all_prompts_flat = get_all_prompts_across_types()

        def build_slot_defaults(slot_index: int) -> Dict[str, Any]:
            if not all_prompts_flat:
                return {
                    'workflow_type':  all_content_types[0],
                    'prompt_name':    '',
                    'content_name':   '',
                    'content_amount': 25
                }
            p = all_prompts_flat[slot_index % len(all_prompts_flat)]
            return {
                'workflow_type':  p['_content_type'],
                'prompt_name':    p['prompt_name'],
                'content_name':   p['prompt_name'],
                'content_amount': 25
            }

        # ── Apply to All Week banner ──────────────────────────────────────────
        st.markdown("""
            <div style='background: linear-gradient(135deg, rgba(103,58,183,0.12), rgba(33,150,243,0.12));
                        padding: 18px 20px;
                        border-radius: 10px;
                        border-left: 5px solid #7c4dff;
                        margin: 10px 0 20px 0;'>
                <h3 style='color: #4a148c; margin: 0 0 6px 0;'>📅 Configure Content Types</h3>
                <p style='margin: 0; color: #555; font-size: 14px;'>
                    Fill in the content types below, then either:<br>
                    &nbsp;&nbsp;• <strong>💾 Save to Selected Day</strong> — applies only to the chosen date<br>
                    &nbsp;&nbsp;• <strong>🗓️ Save to ALL 7 Days</strong> — applies the exact same settings to every day of the week
                </p>
            </div>
        """, unsafe_allow_html=True)

        weekly_settings = get_system_setting('weekly_workflow_settings', {})
        if not isinstance(weekly_settings, dict):
            weekly_settings = {}

        with st.form("create_content_form", clear_on_submit=False):
            st.markdown("#### 📅 Schedule Date")
            st.markdown("*Used for single-day saves. Ignored when saving to all 7 days.*")

            try:
                available_dates = get_future_dates(8)
                date_options = [ds for _, ds in available_dates]
                date_values  = [d  for d, _  in available_dates]

                selected_date_str = st.selectbox(
                    "Schedule Date",
                    options=date_options,
                    index=0,
                    help="Select the date for content generation (next 8 days)"
                )
                selected_index = date_options.index(selected_date_str)
                scheduled_date = date_values[selected_index]

                schedule_exists, day_key, existing_config = check_schedule_exists(
                    weekly_settings, scheduled_date
                )
                day_name = scheduled_date.strftime('%A')

                if schedule_exists:
                    existing_cts = existing_config.get('content_types', [])
                    st.success(
                        f"✅ Config exists for {day_name}, "
                        f"{scheduled_date.strftime('%B %d, %Y')} "
                        f"({len(existing_cts)} type(s))"
                    )
                else:
                    st.info(f"ℹ️ No config for {day_name}, {scheduled_date.strftime('%B %d, %Y')}")

            except Exception as e:
                st.error(f"❌ Date selection error: {e}")
                return

            st.markdown("---")
            st.markdown("### ⚙️ Configure Each Content Type")

            def format_wf(wf):
                return f"{emoji_map.get(wf.lower(), '📄')} {wf.capitalize()}"

            existing_cts_list = existing_config.get('content_types', []) if existing_config else []
            content_configurations = []

            for i in range(3):
                label = f"TYPE{i+1}"
                st.markdown(f"""
                    <div style='background-color: rgba(255, 107, 107, 0.15);
                                padding: 15px; border-radius: 8px;
                                margin: 15px 0; border-left: 4px solid #ff6b6b;'>
                        <h4 style='margin: 0; color: #c92a2a;'>📄 {label}</h4>
                    </div>
                """, unsafe_allow_html=True)

                ex = existing_cts_list[i] if i < len(existing_cts_list) else {}
                defaults = build_slot_defaults(i)

                curr_wf = ex.get('workflow_type') or defaults['workflow_type']
                if curr_wf not in all_content_types:
                    curr_wf = all_content_types[0]

                col1, col2 = st.columns(2)

                with col1:
                    wf_idx = all_content_types.index(curr_wf)
                    workflow_type = st.selectbox(
                        "Content Type",
                        options=all_content_types,
                        format_func=format_wf,
                        index=wf_idx,
                        key=f"workflow_type_{i}"
                    )

                    st.markdown("**Select Prompt to Use:**")
                    try:
                        avail_prompts = get_prompts_by_type(workflow_type)
                        if avail_prompts:
                            p_options = [f"{p['username']} - {p['prompt_name']}" for p in avail_prompts]
                            p_lookup  = {f"{p['username']} - {p['prompt_name']}": p['prompt_name'] for p in avail_prompts}

                            curr_pname = ex.get('prompt_name', '') or defaults['prompt_name']
                            p_idx = 0
                            for pi, pd in enumerate(p_options):
                                if p_lookup[pd] == curr_pname:
                                    p_idx = pi
                                    break

                            sel_p = st.selectbox(
                                "Available Prompts",
                                options=p_options,
                                index=p_idx,
                                key=f"prompt_select_{i}"
                            )
                            prompt_name = p_lookup[sel_p]

                            sel_data = next(p for p in avail_prompts if p['prompt_name'] == prompt_name)
                            with st.expander("📄 Preview Selected Prompt", expanded=False):
                                st.caption(f"**Owner:** {sel_data['username']}")
                                preview = sel_data['content']
                                st.text_area(
                                    "Prompt Content",
                                    value=preview[:500] + "..." if len(preview) > 500 else preview,
                                    height=150,
                                    disabled=True,
                                    key=f"prompt_preview_{i}"
                                )
                        else:
                            st.warning(f"⚠️ No prompts for '{workflow_type}'. Create one first.")
                            prompt_name = ""
                    except Exception as e:
                        st.error(f"❌ Error loading prompts: {e}")
                        prompt_name = ""

                    # Content name
                    default_cname = ex.get('content_name', '') or prompt_name or defaults['content_name']
                    try:
                        type_cnames = get_content_names_by_type(workflow_type)
                        if type_cnames:
                            opts = ["➕ New Content Name"] + type_cnames
                            sidx = opts.index(default_cname) if default_cname in type_cnames else 0
                            sel_cname = st.selectbox(
                                "Content Name",
                                options=opts,
                                index=sidx,
                                key=f"content_name_select_{i}"
                            )
                            if sel_cname == "➕ New Content Name":
                                content_name = st.text_input(
                                    "Enter New Content Name",
                                    value=default_cname if default_cname not in type_cnames else "",
                                    key=f"content_name_input_{i}"
                                )
                            else:
                                content_name = sel_cname
                        else:
                            content_name = st.text_input(
                                "Content Name",
                                value=default_cname,
                                key=f"content_name_{i}"
                            )
                    except Exception as e:
                        st.error(f"❌ Error loading content names: {e}")
                        content_name = default_cname

                with col2:
                    default_amt = int(ex.get('content_amount', 25))
                    content_amount = st.number_input(
                        f"Number of {workflow_type.capitalize()}",
                        min_value=1,
                        max_value=100,
                        value=default_amt,
                        step=1,
                        key=f"content_amount_{i}"
                    )
                    disp_emoji = emoji_map.get(workflow_type.lower(), '📄')
                    st.info(
                        f"{disp_emoji} **{content_amount}** {workflow_type}\n"
                        f"📝 Prompt: **{prompt_name}**\n"
                        f"🏷️ Name: **{content_name}**"
                    )

                content_configurations.append({
                    'workflow_type':  workflow_type,
                    'prompt_name':    prompt_name,
                    'content_name':   (content_name or prompt_name or '').strip(),
                    'content_amount': int(content_amount)
                })

                if i < 2:
                    st.markdown("---")

            # ── Summary ──────────────────────────────────────────────────────
            st.markdown("---")
            st.markdown("### 📊 Configuration Summary")
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**📅 Schedule Info**")
                st.write(f"• Date: {day_name}, {scheduled_date.strftime('%B %d, %Y')}")
                st.write(f"• Total Types: {len(content_configurations)}")
                st.write(f"• Run Times: {', '.join(active_times) if active_times else 'Not set'}")
            with c2:
                st.markdown("**📝 Content Types**")
                for idx, cfg in enumerate(content_configurations):
                    e = emoji_map.get(cfg['workflow_type'].lower(), '📄')
                    st.write(f"• TYPE{idx+1}: {e} {cfg['workflow_type']} ({cfg['content_amount']})")
                    st.caption(f"  Prompt: {cfg['prompt_name']}, Name: {cfg['content_name']}")

            # ── Dual submit buttons ───────────────────────────────────────────
            st.markdown("---")
            st.markdown("""
                <div style='background: rgba(103,58,183,0.07);
                            border-radius: 8px;
                            padding: 12px 16px;
                            margin-bottom: 12px;
                            border: 1px solid rgba(103,58,183,0.2);'>
                    <strong>💡 Two ways to save:</strong><br>
                    <span style='font-size:13px; color:#555;'>
                        <b>Save to Selected Day</b> — saves only to the date chosen above.<br>
                        <b>Save to ALL 7 Days</b> — overwrites every day of the week with these settings.
                    </span>
                </div>
            """, unsafe_allow_html=True)

            btn_col1, btn_col2 = st.columns(2)
            with btn_col1:
                submit_single = st.form_submit_button(
                    "💾 Save to Selected Day",
                    type="primary",
                    use_container_width=True
                )
            with btn_col2:
                submit_all_week = st.form_submit_button(
                    "🗓️ Save to ALL 7 Days",
                    use_container_width=True
                )

            # ── Shared validation ─────────────────────────────────────────────
            if submit_single or submit_all_week:
                try:
                    errors = []
                    for idx, cfg in enumerate(content_configurations):
                        try:
                            validate_content_configuration(
                                cfg['workflow_type'],
                                cfg['content_name'],
                                cfg['content_amount'],
                                cfg['prompt_name']
                            )
                            if not cfg['prompt_name']:
                                errors.append(f"TYPE{idx+1}: No prompt selected")
                        except ValueError as ve:
                            errors.append(f"TYPE{idx+1}: {ve}")

                    if errors:
                        for err in errors:
                            st.error(f"❌ {err}")
                        st.stop()

                    fresh = get_system_setting('weekly_workflow_settings', {})

                    if submit_single:
                        # ── Save to single day ────────────────────────────────
                        fresh[day_key] = _build_day_config(
                            fresh, day_key, day_name,
                            scheduled_date.isoformat(),
                            content_configurations
                        )
                        update_system_setting('weekly_workflow_settings', fresh)

                        st.success("✅ Content configuration saved successfully!")
                        st.info(f"📅 {day_name}, {scheduled_date.strftime('%B %d, %Y')}")
                        for idx, cfg in enumerate(content_configurations):
                            e = emoji_map.get(cfg['workflow_type'].lower(), '📄')
                            st.success(
                                f"TYPE{idx+1}: {e} {cfg['workflow_type'].capitalize()} — "
                                f"**{cfg['content_amount']}** items — "
                                f"Prompt: **{cfg['prompt_name']}** — "
                                f"Name: **{cfg['content_name']}**"
                            )
                        st.balloons()

                    elif submit_all_week:
                        # ── Save to ALL 7 days ────────────────────────────────
                        today = datetime.now().date()
                        weekday_today = today.weekday()       # 0=Monday
                        days_list = ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']
                        saved_days = []

                        for offset, dk in enumerate(days_list):
                            # Compute the upcoming date for this weekday
                            days_until = (offset - weekday_today) % 7
                            target_dt  = today + timedelta(days=days_until)
                            dn         = target_dt.strftime('%A')

                            fresh[dk] = _build_day_config(
                                fresh, dk, dn,
                                target_dt.isoformat(),
                                content_configurations
                            )
                            saved_days.append(f"{dn} ({target_dt.strftime('%b %d')})")

                        update_system_setting('weekly_workflow_settings', fresh)

                        st.success("🎉 Settings applied to **all 7 days** of the week!")
                        st.markdown("**Days updated:**")
                        for sd in saved_days:
                            st.write(f"✅ {sd}")
                        for idx, cfg in enumerate(content_configurations):
                            e = emoji_map.get(cfg['workflow_type'].lower(), '📄')
                            st.success(
                                f"TYPE{idx+1}: {e} {cfg['workflow_type'].capitalize()} — "
                                f"**{cfg['content_amount']}** items — "
                                f"Prompt: **{cfg['prompt_name']}** — "
                                f"Name: **{cfg['content_name']}**"
                            )
                        st.balloons()

                    st.cache_data.clear()
                    st.rerun()

                except Exception as e:
                    st.error(f"❌ CRITICAL: Error saving: {e}")
                    import traceback
                    st.error(traceback.format_exc())

        # ── All configured settings ───────────────────────────────────────────
        weekly_settings = get_system_setting('weekly_workflow_settings', {})
        if weekly_settings:
            st.markdown("---")
            st.markdown("### 📋 All Configured Settings")
            days_order = ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']
            configured_days = 0

            for dk in days_order:
                if dk not in weekly_settings:
                    continue
                cfg = weekly_settings[dk]
                if 'config_date' not in cfg or 'content_types' not in cfg:
                    continue
                cts = cfg.get('content_types', [])
                if not cts:
                    continue

                configured_days += 1
                parts = []
                for ct in cts:
                    e = emoji_map.get(ct['workflow_type'].lower(), '📄')
                    parts.append(f"{e} [{ct.get('prompt_name','?')}] {ct['content_name']} ({ct['content_amount']})")

                with st.expander(
                    f"📅 {cfg.get('day_name', dk.capitalize())} — {cfg.get('config_date')} | {len(cts)} type(s)",
                    expanded=False
                ):
                    st.markdown(f"**{'  |  '.join(parts)}**")

                    for idx, ct in enumerate(cts):
                        e = emoji_map.get(ct['workflow_type'].lower(), '📄')
                        c1, c2, c3, c4 = st.columns(4)
                        with c1: st.metric(f"Type {idx+1}", f"{e} {ct['workflow_type'].capitalize()}")
                        with c2: st.metric("Prompt", ct.get('prompt_name', 'N/A'))
                        with c3: st.metric("Content Name", ct['content_name'])
                        with c4: st.metric("Amount", ct['content_amount'])

                    if cfg.get('updated_at'):
                        st.caption(f"Last updated: {cfg['updated_at']}")

                    bc1, bc2 = st.columns(2)
                    with bc1:
                        if st.button("✏️ Edit", key=f"edit_content_{dk}", use_container_width=True):
                            st.session_state[f'editing_content_{dk}'] = True
                            st.rerun()
                    with bc2:
                        if st.button(
                            "🗑️ Delete",
                            key=f"delete_{dk}_{cfg.get('config_date')}",
                            use_container_width=True
                        ):
                            try:
                                fresh = get_system_setting('weekly_workflow_settings', {})
                                if dk in fresh:
                                    for k in ['config_date','content_types','updated_at','created_at']:
                                        fresh[dk].pop(k, None)
                                    update_system_setting('weekly_workflow_settings', fresh)
                                    st.success(f"✅ Deleted config for {dk}")
                                    st.rerun()
                            except Exception as e:
                                st.error(f"❌ {e}")

                    # Inline edit
                    if st.session_state.get(f'editing_content_{dk}'):
                        st.markdown("---")
                        st.markdown(f"**✏️ Editing {dk.title()} configuration**")
                        try:
                            all_types_edit = get_postgres_prompt_types() or all_content_types
                        except:
                            all_types_edit = all_content_types

                        with st.form(key=f"edit_content_form_{dk}"):
                            edit_configs = []
                            for i, ct in enumerate(cts):
                                st.markdown(f"**TYPE{i+1}**")
                                ec1, ec2 = st.columns(2)
                                with ec1:
                                    cw = ct.get('workflow_type', all_types_edit[0])
                                    wi = all_types_edit.index(cw) if cw in all_types_edit else 0
                                    ewf = st.selectbox("Content Type", all_types_edit, index=wi, key=f"edit_wf_{dk}_{i}")

                                    ep = get_prompts_by_type(ewf)
                                    cp = ct.get('prompt_name', '')
                                    if ep:
                                        epo = [f"{p['username']} - {p['prompt_name']}" for p in ep]
                                        epl = {f"{p['username']} - {p['prompt_name']}": p['prompt_name'] for p in ep}
                                        epi = 0
                                        for ei, ed in enumerate(epo):
                                            if epl[ed] == cp:
                                                epi = ei
                                                break
                                        sel = st.selectbox("Prompt", epo, index=epi, key=f"edit_prompt_{dk}_{i}")
                                        eprompt = epl[sel]
                                    else:
                                        eprompt = st.text_input("Prompt Name", value=cp, key=f"edit_prompt_text_{dk}_{i}")

                                    ecn = st.text_input("Content Name", value=ct.get('content_name',''), key=f"edit_cname_{dk}_{i}")

                                with ec2:
                                    ea = st.number_input("Amount", min_value=1, max_value=100, value=int(ct.get('content_amount',25)), key=f"edit_amount_{dk}_{i}")

                                edit_configs.append({
                                    'workflow_type':  ewf,
                                    'prompt_name':    eprompt,
                                    'content_name':   ecn.strip(),
                                    'content_amount': int(ea)
                                })
                                if i < len(cts) - 1:
                                    st.markdown("---")

                            sc, cc = st.columns(2)
                            with sc:
                                save_edit = st.form_submit_button("💾 Save Changes", type="primary", use_container_width=True)
                            with cc:
                                cancel_edit = st.form_submit_button("❌ Cancel", use_container_width=True)

                        if save_edit:
                            try:
                                fresh = get_system_setting('weekly_workflow_settings', {})
                                fresh[dk]['content_types'] = edit_configs
                                fresh[dk]['updated_at'] = datetime.now().isoformat()
                                update_system_setting('weekly_workflow_settings', fresh)
                                st.session_state[f'editing_content_{dk}'] = False
                                st.success(f"✅ {dk.title()} updated!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ {e}")

                        if cancel_edit:
                            st.session_state[f'editing_content_{dk}'] = False
                            st.rerun()

            if configured_days == 0:
                st.info("ℹ️ No complete content configurations found")

    except Exception as e:
        st.error(f"❌ CRITICAL: {e}")
        import traceback
        st.error(traceback.format_exc())
