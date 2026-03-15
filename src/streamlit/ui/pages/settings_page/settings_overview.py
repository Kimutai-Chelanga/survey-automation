import streamlit as st
import pandas as pd
from datetime import datetime
from ...settings.settings import DEFAULT_SETTINGS
from ...settings.settings_manager import get_system_setting
from .utils import mask_sensitive_uri, export_settings

def render_settings_overview(settings_manager):
    """Render comprehensive overview of all current settings."""
    st.header("📊 Complete Settings Overview")
    st.markdown("*Real-time view of all system configurations*")
    
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        if st.button("🔄 Refresh Settings", type="secondary"):
            st.rerun()
    
    with col2:
        export_settings_button = st.button("📥 Export Settings", type="secondary")
    
    if export_settings_button:
        export_settings(settings_manager)
    
    try:
        content_settings = get_system_setting('create_content_settings', {
            'number_of_messages': 10,
            'number_of_replies': 5,
            'number_of_retweets': 3
        })
        
        extraction_settings = get_system_setting('extraction_processing_settings', {
            'extraction_window': 24,
            'content_to_filter': 19,
            'words_to_filter': '',
            'gap_between_workflows': 15
        })
        
        time_settings = get_system_setting('extraction_time_settings', {
            'morning': {'start_time': '09:00', 'number_of_batches': 3, 'gap_between_batches': 30},
            'evening': {'start_time': '18:00', 'number_of_batches': 2, 'gap_between_batches': 45}
        })
        
        strategy_settings = get_system_setting('workflow_strategy_settings', {
            'strategy': 'all',
            'custom_order': 'messages,replies,retweets',
            'batch_size': 1,
            'max_workflows_per_run': 50,
            'trigger_after_upload': False,
            'trigger_delay_seconds': 5.0,
            'interval_between_batches': 3.0
        })
        
        create_content_config = get_system_setting('create_content_config', DEFAULT_SETTINGS.get('create_content', {}))
        automa_config = get_system_setting('automa_config', DEFAULT_SETTINGS.get('automa', {}))
        
        mongodb_uri = get_system_setting('mongodb_uri', 'Not configured')
        database_url = get_system_setting('database_url', 'Not configured')
        
        with st.expander("📝 Content Generation Settings", expanded=True):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("📧 Messages", content_settings.get('number_of_messages', 'N/A'))
            with col2:
                st.metric("💬 Replies", content_settings.get('number_of_replies', 'N/A'))
            with col3:
                st.metric("🔄 Retweets", content_settings.get('number_of_retweets', 'N/A'))
            with col4:
                total_content = (
                    content_settings.get('number_of_messages', 0) +
                    content_settings.get('number_of_replies', 0) +
                    content_settings.get('number_of_retweets', 0)
                )
                st.metric("📊 Total Items", total_content)
            st.json(content_settings)
        
        with st.expander("🔍 Extraction & Processing Settings", expanded=True):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                window_hours = extraction_settings.get('extraction_window', 0)
                days = window_hours // 24
                hours = window_hours % 24
                window_text = f"{days}d {hours}h" if days > 0 else f"{hours}h"
                st.metric("⏰ Extraction Window", window_text)
            with col2:
                st.metric("🔢 Content Limit", extraction_settings.get('content_to_filter', 'N/A'))
            with col3:
                st.metric("⏱️ Workflow Gap", f"{extraction_settings.get('gap_between_workflows', 'N/A')} min")
            with col4:
                filter_words = extraction_settings.get('words_to_filter', '')
                word_count = len([w.strip() for w in filter_words.split(',') if w.strip()]) if filter_words else 0
                st.metric("🏷️ Filter Words", word_count)
            if extraction_settings.get('words_to_filter'):
                st.markdown("**🚫 Current Filter Words:**")
                words = [w.strip() for w in extraction_settings.get('words_to_filter', '').split(',') if w.strip()]
                if words:
                    st.info(f"🏷️ {', '.join(words)}")
            st.json(extraction_settings)
        
        with st.expander("⏰ Batch Processing Schedule", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 🌅 Morning Schedule")
                morning = time_settings.get('morning', {})
                st.write(f"**Start Time:** {morning.get('start_time', 'N/A')}")
                st.write(f"**Batches:** {morning.get('number_of_batches', 'N/A')}")
                st.write(f"**Gap:** {morning.get('gap_between_batches', 'N/A')} minutes")
                batches = morning.get('number_of_batches', 0)
                gap = morning.get('gap_between_batches', 0)
                duration = batches * gap
                st.write(f"**Duration:** ~{duration} minutes")
            with col2:
                st.markdown("#### 🌆 Evening Schedule")
                evening = time_settings.get('evening', {})
                st.write(f"**Start Time:** {evening.get('start_time', 'N/A')}")
                st.write(f"**Batches:** {evening.get('number_of_batches', 'N/A')}")
                st.write(f"**Gap:** {evening.get('gap_between_batches', 'N/A')} minutes")
                batches = evening.get('number_of_batches', 0)
                gap = evening.get('gap_between_batches', 0)
                duration = batches * gap
                st.write(f"**Duration:** ~{duration} minutes")
            st.json(time_settings)
        
        with st.expander("🎯 Workflow Strategy Settings", expanded=True):
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Strategy", strategy_settings.get('strategy', 'N/A').title())
            with col2:
                st.metric("Batch Size", strategy_settings.get('batch_size', 'N/A'))
            with col3:
                st.metric("Max Workflows", strategy_settings.get('max_workflows_per_run', 'N/A'))
            st.markdown(f"**Custom Order:** {strategy_settings.get('custom_order', 'N/A')}")
            st.write(f"**Trigger After Upload:** {strategy_settings.get('trigger_after_upload', 'N/A')}")
            st.write(f"**Trigger Delay:** {strategy_settings.get('trigger_delay_seconds', 'N/A')}s")
            st.write(f"**Batch Interval:** {strategy_settings.get('interval_between_batches', 'N/A')}s")
            st.json(strategy_settings)
        
        with st.expander("📋 DAG Configurations", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 📝 Create Content DAG")
                st.write(f"**Schedule:** {create_content_config.get('schedule_interval', 'N/A')}")
                st.write(f"**Timezone:** {create_content_config.get('timezone', 'N/A')}")
                st.json(create_content_config)
            with col2:
                st.markdown("#### 🤖 Automa Workflow DAG")
                st.write(f"**Schedule:** {automa_config.get('schedule_interval', 'N/A')}")
                st.write(f"**Timezone:** {automa_config.get('timezone', 'N/A')}")
                st.json(automa_config)
        
        with st.expander("🔗 System Connections", expanded=False):
            st.markdown("#### 📊 Database Connections")
            mongodb_status = "🟢 Configured" if mongodb_uri != 'Not configured' else "🔴 Not configured"
            st.write(f"**MongoDB:** {mongodb_status}")
            if mongodb_uri != 'Not configured':
                masked_uri = mask_sensitive_uri(mongodb_uri)
                st.code(masked_uri)
            postgres_status = "🟢 Configured" if database_url != 'Not configured' else "🔴 Not configured"
            st.write(f"**PostgreSQL:** {postgres_status}")
            if database_url != 'Not configured':
                masked_url = mask_sensitive_uri(database_url)
                st.code(masked_url)
        
        with st.expander("ℹ️ Settings Metadata", expanded=False):
            st.markdown("#### 📊 Settings Information")
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.write(f"**Last Refreshed:** {current_time}")
            st.write(f"**Settings Source:** MongoDB (messages_db.settings)")
            st.write(f"**Fallback:** Environment variables + defaults")
            
            total_settings = 7
            configured_settings = sum([
                1 if content_settings else 0,
                1 if extraction_settings else 0,
                1 if time_settings else 0,
                1 if strategy_settings else 0,
                1 if create_content_config else 0,
                1 if automa_config else 0,
                1 if mongodb_uri != 'Not configured' else 0
            ])
            health_percentage = (configured_settings / total_settings) * 100
            st.progress(health_percentage / 100)
            st.write(f"**Configuration Health:** {configured_settings}/{total_settings} ({health_percentage:.0f}%)")
    
    except Exception as e:
        st.error(f"❌ Error loading settings: {str(e)}")
        st.write("**Error Details:**")
        st.code(str(e))