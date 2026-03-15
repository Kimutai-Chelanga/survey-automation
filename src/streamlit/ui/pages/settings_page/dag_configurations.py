import streamlit as st
import subprocess
from ...settings.settings_manager import get_system_setting, update_system_setting
from ...settings.settings import DEFAULT_SETTINGS

def render_dag_configurations(settings_manager):
    """Render DAG configurations section."""
    st.header("⚙️ DAG Schedules and Timezones")
    content_tab, extraction_tab = st.tabs(["Create Content DAGs", "Extraction & Processing DAGs"])
    
    with content_tab:
        _render_create_content_dags(settings_manager)
    
    with extraction_tab:
        _render_extraction_processing_dags(settings_manager)
    
    st.markdown("---")
    if st.button("🔄 Restart Airflow Scheduler", key="restart_scheduler", type="secondary"):
        try:
            subprocess.run(["docker", "compose", "restart", "airflow-scheduler"], check=True)
            st.success("✅ Airflow scheduler restarted successfully!")
        except Exception as e:
            st.error(f"❌ Failed to restart Airflow scheduler: {e}")

def _render_extraction_processing_dags(settings_manager):
    """Render extraction and processing DAGs section with workflow type selection."""
    st.subheader("🔍 Data Extraction & Processing DAGs")
    st.markdown("""
    **Extraction & Processing DAGs:**
    1. 📥 Extraction DAG
    2. 🐦 Tweet DAG  
    3. 💉 Injection DAG
    """)
    
    with st.form("extraction_processing"):
        # Workflow Type Selection Section
        st.markdown("#### 🎯 Workflow Type Selection")
        st.markdown("Select which workflow types to include in extraction and processing:")
        
        current_workflow_types = get_system_setting('extraction_workflow_types', {
            'replies': True,
            'messages': True,
            'retweets': True
        })
        
        workflow_col1, workflow_col2, workflow_col3 = st.columns(3)
        
        with workflow_col1:
            replies_enabled = st.checkbox(
                "📝 Replies Workflow",
                value=current_workflow_types.get('replies', True),
                key="replies_enable",
                help="Enable extraction and processing of reply workflows"
            )
            st.markdown("*Process Twitter reply interactions*")
        
        with workflow_col2:
            messages_enabled = st.checkbox(
                "💬 Messages Workflow", 
                value=current_workflow_types.get('messages', True),
                key="messages_enable",
                help="Enable extraction and processing of message workflows"
            )
            st.markdown("*Process Twitter direct messages*")
        
        with workflow_col3:
            retweets_enabled = st.checkbox(
                "🔄 Retweets Workflow",
                value=current_workflow_types.get('retweets', True),
                key="retweets_enable",
                help="Enable extraction and processing of retweet workflows"
            )
            st.markdown("*Process Twitter retweet interactions*")
        
        # Priority Order Configuration
        st.markdown("##### 🏆 Execution Priority Order")
        st.markdown("Configure the order in which workflow types will be processed:")
        
        current_priority = get_system_setting('extraction_priority_order', ['replies', 'messages', 'retweets'])
        
        priority_col1, priority_col2 = st.columns(2)
        
        with priority_col1:
            st.markdown("**Current Priority Order:**")
            for i, workflow_type in enumerate(current_priority, 1):
                priority_emoji = {"replies": "📝", "messages": "💬", "retweets": "🔄"}
                st.write(f"{i}. {priority_emoji.get(workflow_type, '🔹')} {workflow_type.title()}")
        
        with priority_col2:
            st.markdown("**Reorder Priority:**")
            available_types = []
            if replies_enabled:
                available_types.append('replies')
            if messages_enabled:
                available_types.append('messages')
            if retweets_enabled:
                available_types.append('retweets')
            
            if available_types:
                # Create ordered list based on current priority, filtered by enabled types
                ordered_priority = [wf for wf in current_priority if wf in available_types]
                # Add any missing enabled types to the end
                for wf in available_types:
                    if wf not in ordered_priority:
                        ordered_priority.append(wf)
                
                new_priority_order = st.multiselect(
                    "Drag to reorder priority",
                    options=available_types,
                    default=ordered_priority,
                    key="priority",
                    help="First selected = highest priority. Drag to reorder."
                )
            else:
                new_priority_order = []
                st.warning("⚠️ No workflow types selected!")
        
        # Workflow Type Statistics
        if st.checkbox("📊 Show Workflow Type Statistics", key="show_workflow_stat"):
            st.markdown("##### 📈 Current Workflow Statistics")
            
            # Mock statistics - replace with actual data retrieval
            stats_col1, stats_col2, stats_col3 = st.columns(3)
            
            with stats_col1:
                if replies_enabled:
                    st.metric("Replies Pending", "47", "↗️ +5")
                    st.metric("Replies Success Rate", "94.2%", "↗️ +2.1%")
            
            with stats_col2:
                if messages_enabled:
                    st.metric("Messages Pending", "23", "↘️ -3")
                    st.metric("Messages Success Rate", "96.8%", "↗️ +0.5%")
            
            with stats_col3:
                if retweets_enabled:
                    st.metric("Retweets Pending", "31", "↗️ +8")
                    st.metric("Retweets Success Rate", "92.1%", "↘️ -1.2%")
        
        st.markdown("---")
        
        # Time Settings Configuration (existing code continues...)
        st.markdown("#### ⏰ Time Settings Configuration")
        time_settings_config = get_system_setting('extraction_time_settings', {
            'morning': {
                'start_time': '09:00',
                'number_of_batches': 3,
                'gap_between_batches': 30
            },
            'evening': {
                'start_time': '18:00',
                'number_of_batches': 2,
                'gap_between_batches': 45
            }
        })
        
        morning_col, evening_col = st.columns(2)
        
        with morning_col:
            st.markdown("##### 🌅 Morning Settings")
            
            current_morning_time = time_settings_config['morning'].get('start_time', '09:00')
            try:
                morning_hour, morning_minute = map(int, current_morning_time.split(':'))
            except:
                morning_hour, morning_minute = 9, 0
            
            morning_time_col1, morning_time_col2 = st.columns(2)
            with morning_time_col1:
                morning_hour_input = st.number_input(
                    "Hour (24-hour format)",
                    min_value=0,
                    max_value=23,
                    value=morning_hour,
                    key="morning_hour",
                    help="Enter hour in 24-hour format (0-23)"
                )
            with morning_time_col2:
                morning_minute_input = st.number_input(
                    "Minute",
                    min_value=0,
                    max_value=59,
                    value=morning_minute,
                    step=5,
                    key="morning_minute",
                    help="Enter minute (0-59)"
                )
            
            morning_formatted_time = f"{morning_hour_input:02d}:{morning_minute_input:02d}"
            morning_cron_expression = f"{morning_minute_input} {morning_hour_input} * * *"
            
            st.info(f"**Time:** {morning_formatted_time} (24-hour)")
            st.code(f"Cron: {morning_cron_expression}", language="bash")
            
            morning_batches = st.number_input(
                "Number of Batches (Morning)",
                min_value=1,
                max_value=10,
                value=time_settings_config['morning'].get('number_of_batches', 3),
                key="morning_batches",
                help="Number of processing batches in the morning"
            )
            morning_gap = st.number_input(
                "Gap Between Batches (minutes)",
                min_value=5,
                max_value=120,
                value=time_settings_config['morning'].get('gap_between_batches', 30),
                key="morning_gap",
                help="Time gap between morning batches in minutes"
            )
            
            total_duration = morning_batches * morning_gap
            end_hour = morning_hour_input + (morning_minute_input + total_duration) // 60
            end_minute = (morning_minute_input + total_duration) % 60
            end_time_note = " (next day)" if end_hour >= 24 else ""
            if end_hour >= 24:
                end_hour -= 24
            
            st.success(f"""
            **Morning Schedule Preview:**
            - Start: {morning_formatted_time}
            - Batches: {morning_batches}
            - Gap: {morning_gap} minutes
            - Duration: ~{total_duration} minutes
            - Est. End: ~{end_hour:02d}:{end_minute:02d}{end_time_note}
            """)
        
        with evening_col:
            st.markdown("##### 🌆 Evening Settings")
            
            current_evening_time = time_settings_config['evening'].get('start_time', '18:00')
            try:
                evening_hour, evening_minute = map(int, current_evening_time.split(':'))
            except:
                evening_hour, evening_minute = 18, 0
            
            evening_time_col1, evening_time_col2 = st.columns(2)
            with evening_time_col1:
                evening_hour_input = st.number_input(
                    "Hour (24-hour format)",
                    min_value=0,
                    max_value=23,
                    value=evening_hour,
                    key="evening_hour",
                    help="Enter hour in 24-hour format (0-23)"
                )
            with evening_time_col2:
                evening_minute_input = st.number_input(
                    "Minute",
                    min_value=0,
                    max_value=59,
                    value=evening_minute,
                    step=5,
                    key="evening_minute",
                    help="Enter minute (0-59)"
                )
            
            evening_formatted_time = f"{evening_hour_input:02d}:{evening_minute_input:02d}"
            evening_cron_expression = f"{evening_minute_input} {evening_hour_input} * * *"
            
            st.info(f"**Time:** {evening_formatted_time} (24-hour)")
            st.code(f"Cron: {evening_cron_expression}", language="bash")
            
            evening_batche = st.number_input(
                "Number of Batches (Evening)",
                min_value=1,
                max_value=10,
                value=time_settings_config['evening'].get('number_of_batches', 2),
                key="evening_batche",
                help="Number of processing batches in the evening"
            )
            evening_gap = st.number_input(
                "Gap Between Batches (minutes)",
                min_value=5,
                max_value=120,
                value=time_settings_config['evening'].get('gap_between_batches', 45),
                key="evening_gap",
                help="Time gap between evening batches in minutes"
            )
            
            total_duration = evening_batche * evening_gap
            end_hour = evening_hour_input + (evening_minute_input + total_duration) // 60
            end_minute = (evening_minute_input + total_duration) % 60
            end_time_note = " (next day)" if end_hour >= 24 else ""
            if end_hour >= 24:
                end_hour -= 24
            
            st.success(f"""
            **Evening Schedule Preview:**
            - Start: {evening_formatted_time}
            - Batches: {evening_batche}
            - Gap: {evening_gap} minutes
            - Duration: ~{total_duration} minutes
            - Est. End: ~{end_hour:02d}:{end_minute:02d}{end_time_note}
            """)
        
        with st.expander("🔧 Advanced Cron Settings (Optional)", expanded=False):
            st.markdown("##### 📅 Additional Scheduling Options")
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Morning Advanced Settings**")
                morning_day_of_week = st.multiselect(
                    "Days of Week (Morning)",
                    options=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
                    default=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
                    key="morning_days",
                    help="Select which days to run morning batches"
                )
                morning_day_of_month = st.text_input(
                    "Day of Month (Morning)",
                    value="*",
                    key="morning_do",
                    help="Specific day of month (* for all, 1-31 for specific day)"
                )
                morning_month = st.text_input(
                    "Month (Morning)",
                    value="*",
                    key="morning_month",
                    help="Specific month (* for all, 1-12 for specific month)"
                )
                
                day_mapping = {
                    'Monday': '1', 'Tuesday': '2', 'Wednesday': '3', 'Thursday': '4',
                    'Friday': '5', 'Saturday': '6', 'Sunday': '0'
                }
                morning_dow = ','.join([day_mapping[day] for day in morning_day_of_week]) if morning_day_of_week else '*'
                morning_full_cron = f"{morning_minute_input} {morning_hour_input} {morning_day_of_month} {morning_month} {morning_dow}"
                st.code(f"Full Morning Cron: {morning_full_cron}", language="bash")
            
            with col2:
                st.markdown("**Evening Advanced Settings**")
                evening_day_of_week = st.multiselect(
                    "Days of Week (Evening)",
                    options=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
                    default=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
                    key="evening_days",
                    help="Select which days to run evening batches"
                )
                evening_day_of_month = st.text_input(
                    "Day of Month (Evening)",
                    value="*",
                    key="evening_dom",
                    help="Specific day of month (* for all, 1-31 for specific day)"
                )
                evening_month = st.text_input(
                    "Month (Evening)",
                    value="*",
                    key="evening_month",
                    help="Specific month (* for all, 1-12 for specific month)"
                )
                
                evening_dow = ','.join([day_mapping[day] for day in evening_day_of_week]) if evening_day_of_week else '*'
                evening_full_cron = f"{evening_minute_input} {evening_hour_input} {evening_day_of_month} {evening_month} {evening_dow}"
                st.code(f"Full Evening Cron: {evening_full_cron}", language="bash")
        
        st.markdown("---")
        st.markdown("#### 📊 Complete Schedule Summary")
        
        # Summary with workflow type information
        summary_col1, summary_col2, summary_col3 = st.columns(3)
        
        with summary_col1:
            st.markdown("**🎯 Workflow Types Configuration**")
            enabled_workflows = []
            if replies_enabled:
                enabled_workflows.append("📝 Replies")
            if messages_enabled:
                enabled_workflows.append("💬 Messages") 
            if retweets_enabled:
                enabled_workflows.append("🔄 Retweets")
            
            if enabled_workflows:
                for workflow in enabled_workflows:
                    st.write(f"• {workflow}")
                st.write(f"**Priority:** {' → '.join([wf.title() for wf in new_priority_order])}")
            else:
                st.error("⚠️ No workflow types enabled!")
        
        with summary_col2:
            st.markdown("**🌅 Morning Configuration**")
            st.write(f"• **Time:** {morning_formatted_time} (24-hour)")
            st.write(f"• **Cron:** `{morning_cron_expression}`")
            st.write(f"• **Batches:** {morning_batches}")
            st.write(f"• **Gap:** {morning_gap} minutes")
            st.write(f"• **Total Duration:** ~{morning_batches * morning_gap} minutes")
        
        with summary_col3:
            st.markdown("**🌆 Evening Configuration**")
            st.write(f"• **Time:** {evening_formatted_time} (24-hour)")
            st.write(f"• **Cron:** `{evening_cron_expression}`")
            st.write(f"• **Batches:** {evening_batche}")
            st.write(f"• **Gap:** {evening_gap} minutes")
            st.write(f"• **Total Duration:** ~{evening_batche * evening_gap} minutes")
        
        # Validation warnings
        if not any([replies_enabled, messages_enabled, retweets_enabled]):
            st.error("🚨 **Configuration Error:** At least one workflow type must be enabled!")
            submit_disabled = True
        elif len(new_priority_order) != len([w for w in [replies_enabled, messages_enabled, retweets_enabled] if w]):
            st.warning("⚠️ **Priority Warning:** Priority order should include all enabled workflow types.")
            submit_disabled = False
        else:
            submit_disabled = False
        
        if st.form_submit_button("🔧 Submit Extraction DAG Settings", type="primary", use_container_width=True, disabled=submit_disabled):
            # Save workflow type configuration
            workflow_types_config = {
                'replies': replies_enabled,
                'messages': messages_enabled, 
                'retweets': retweets_enabled
            }
            update_system_setting('extraction_workflow_types', workflow_types_config)
            
            # Save priority order
            update_system_setting('extraction_priority_order', new_priority_order)
            
            # Save time settings (existing logic)
            time_settings = {
                'morning': {
                    'start_time': morning_formatted_time,
                    'hour': morning_hour_input,
                    'minute': morning_minute_input,
                    'cron_expression': morning_cron_expression,
                    'number_of_batches': morning_batches,
                    'gap_between_batches': morning_gap
                },
                'evening': {
                    'start_time': evening_formatted_time,
                    'hour': evening_hour_input,
                    'minute': evening_minute_input,
                    'cron_expression': evening_cron_expression,
                    'number_of_batches': evening_batche,
                    'gap_between_batches': evening_gap
                }
            }
            
            if 'morning_days' in st.session_state and st.session_state.morning_days:
                time_settings['morning']['advanced_cron'] = morning_full_cron
                time_settings['morning']['days_of_week'] = morning_day_of_week
                time_settings['morning']['day_of_month'] = morning_day_of_month
                time_settings['morning']['month'] = morning_month
            
            if 'evening_days' in st.session_state and st.session_state.evening_days:
                time_settings['evening']['advanced_cron'] = evening_full_cron
                time_settings['evening']['days_of_week'] = evening_day_of_week
                time_settings['evening']['day_of_month'] = evening_day_of_month
                time_settings['evening']['month'] = evening_month
            
            # Add workflow type information to time settings
            time_settings['workflow_types'] = workflow_types_config
            time_settings['priority_order'] = new_priority_order
            
            update_system_setting('extraction_time_settings', time_settings)
            st.success("✅ Extraction DAG settings updated successfully!")
            
            enabled_types_text = ", ".join([wf.title() for wf in new_priority_order])
            st.info(f"""
            **Settings Saved:**
            - Workflow Types: {enabled_types_text}
            - Priority Order: {' → '.join([wf.title() for wf in new_priority_order])}
            - Morning: {morning_formatted_time} → `{morning_cron_expression}`
            - Evening: {evening_formatted_time} → `{evening_cron_expression}`
            """)
            st.balloons()

def _render_create_content_dags(settings_manager):
    """Render create content DAGs section with improved cron input."""
    st.subheader("📝 Content Generation DAGs")
    st.markdown("""
    **Content DAGs:**
    1. 🎯 Create Content DAG
    2. 🤖 Automa Workflow DAG
    """)
    
    with st.form("create_content_dag_form"):
        st.markdown("#### 🎯 Create Content DAG Configuration")
        create_content_config = get_system_setting('create_content_config', DEFAULT_SETTINGS.get('create_content', {}))
        
        current_schedule = create_content_config.get('schedule_interval', '0 9 * * *')
        try:
            parts = current_schedule.split()
            if len(parts) >= 2:
                create_minute = int(parts[0])
                create_hour = int(parts[1])
            else:
                create_minute, create_hour = 0, 9
        except:
            create_minute, create_hour = 0, 9
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**⏰ Create Content Schedule**")
            
            create_time_col1, create_time_col2 = st.columns(2)
            with create_time_col1:
                create_hour_input = st.number_input(
                    "Hour (24-hour)",
                    min_value=0,
                    max_value=23,
                    value=create_hour,
                    key="create_content_hour",
                    help="Hour in 24-hour format (0-23)"
                )
            with create_time_col2:
                create_minute_input = st.number_input(
                    "Minute",
                    min_value=0,
                    max_value=59,
                    value=create_minute,
                    step=5,
                    key="create_content_minute",
                    help="Minute (0-59)"
                )
            
            create_formatted_time = f"{create_hour_input:02d}:{create_minute_input:02d}"
            create_cron_basic = f"{create_minute_input} {create_hour_input} * * *"
            
            st.info(f"**Time:** {create_formatted_time}")
            st.code(f"Cron: {create_cron_basic}", language="bash")
            
            create_content_timezone = st.text_input(
                "Timezone",
                value=create_content_config.get('timezone', 'UTC'),
                key="create_content_timezone",
                help="Timezone for create content DAG"
            )
        
        with col2:
            st.markdown("**🤖 Automa Workflow Schedule**")
            
            automa_config = get_system_setting('automa_config', DEFAULT_SETTINGS.get('automa', {}))
            current_automa_schedule = automa_config.get('schedule_interval', '0 10 * * *')
            try:
                parts = current_automa_schedule.split()
                if len(parts) >= 2:
                    automa_minute = int(parts[0])
                    automa_hour = int(parts[1])
                else:
                    automa_minute, automa_hour = 0, 10
            except:
                automa_minute, automa_hour = 0, 10
            
            automa_time_col1, automa_time_col2 = st.columns(2)
            with automa_time_col1:
                automa_hour_input = st.number_input(
                    "Hour (24-hour)",
                    min_value=0,
                    max_value=23,
                    value=automa_hour,
                    key="automa_hour",
                    help="Hour in 24-hour format (0-23)"
                )
            with automa_time_col2:
                automa_minute_input = st.number_input(
                    "Minute",
                    min_value=0,
                    max_value=59,
                    value=automa_minute,
                    step=5,
                    key="automa_minute",
                    help="Minute (0-59)"
                )
            
            automa_formatted_time = f"{automa_hour_input:02d}:{automa_minute_input:02d}"
            automa_cron_basic = f"{automa_minute_input} {automa_hour_input} * * *"
            
            st.info(f"**Time:** {automa_formatted_time}")
            st.code(f"Cron: {automa_cron_basic}", language="bash")
            
            automa_timezone = st.text_input(
                "Timezone",
                value=automa_config.get('timezone', 'UTC'),
                key="automa_timezone",
                help="Timezone for automa workflow DAG"
            )
        
        with st.expander("🔧 Advanced Cron Settings", expanded=False):
            st.markdown("##### 📅 Advanced Scheduling Options")
            
            advanced_col1, advanced_col2 = st.columns(2)
            with advanced_col1:
                st.markdown("**Create Content Advanced**")
                create_custom_cron = st.text_input(
                    "Custom Cron Expression",
                    value=current_schedule,
                    key="create_custom_cron",
                    help="Full cron expression (minute hour day month dow)"
                )
                use_create_custom = st.checkbox(
                    "Use Custom Cron",
                    key="use_create_custom",
                    help="Use custom cron instead of time inputs"
                )
            
            with advanced_col2:
                st.markdown("**Automa Workflow Advanced**")
                automa_custom_cron = st.text_input(
                    "Custom Cron Expression",
                    value=current_automa_schedule,
                    key="automa_custom_cron",
                    help="Full cron expression (minute hour day month dow)"
                )
                use_automa_custom = st.checkbox(
                    "Use Custom Cron",
                    key="use_automa_custom",
                    help="Use custom cron instead of time inputs"
                )
        
        final_create_cron = create_custom_cron if use_create_custom else create_cron_basic
        final_automa_cron = automa_custom_cron if use_automa_custom else automa_cron_basic
        
        st.markdown("---")
        st.markdown("#### 📊 Final Configuration Summary")
        summary_col1, summary_col2 = st.columns(2)
        
        with summary_col1:
            st.markdown("**🎯 Create Content DAG**")
            st.write(f"• **Time:** {create_formatted_time}")
            st.write(f"• **Cron:** `{final_create_cron}`")
            st.write(f"• **Timezone:** {create_content_timezone}")
        
        with summary_col2:
            st.markdown("**🤖 Automa Workflow DAG**")
            st.write(f"• **Time:** {automa_formatted_time}")
            st.write(f"• **Cron:** `{final_automa_cron}`")
            st.write(f"• **Timezone:** {automa_timezone}")
        
        if st.form_submit_button("💾 Submit Content DAG Settings", type="primary", use_container_width=True):
            new_create_content_config = {
                'schedule_interval': final_create_cron,
                'timezone': create_content_timezone,
                'time_24h': create_formatted_time,
                'hour': create_hour_input,
                'minute': create_minute_input,
                'default_args': create_content_config.get('default_args', {})
            }
            update_system_setting('create_content_config', new_create_content_config)
            
            new_automa_config = {
                'schedule_interval': final_automa_cron,
                'timezone': automa_timezone,
                'time_24h': automa_formatted_time,
                'hour': automa_hour_input,
                'minute': automa_minute_input,
                'default_args': automa_config.get('default_args', {})
            }
            update_system_setting('automa_config', new_automa_config)
            
            st.success("✅ Content DAG settings updated successfully!")
            st.info(f"""
            **Cron Expressions Saved:**
            - Create Content: `{final_create_cron}`
            - Automa Workflow: `{final_automa_cron}`
            """)
            st.balloons()