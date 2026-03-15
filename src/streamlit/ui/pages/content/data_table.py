import streamlit as st
import pandas as pd
from .data_formatter import format_content_dataframe, format_analytics_dataframe
from ui.components.data_filters import DataFilters
from src.core.database.postgres import account_prompt_utils as pg_utils

def render_data_table(section_name: str, db_module, filter_option: str, session_key: str, account_id=None):
    """Render data table for the given section with enhanced filtering options."""
    st.subheader(f"All {section_name}")
    
    use_filter = DataFilters.get_usage_filter(filter_option)
    
    # Initialize refresh_stats if not set
    if 'refresh_stats' not in st.session_state:
        st.session_state.refresh_stats = True
    
    # Add additional filtering options
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Workflow status filter
        workflow_status_filter = st.selectbox(
            "Filter by Workflow Status:",
            ["All", "pending", "processing", "completed", "failed"],
            key=f"workflow_status_{section_name.lower()}"
        )
    
    with col2:
        # Prompt filter (if account is selected)
        prompt_filter = None
        if account_id:
            try:
                prompts = pg_utils.get_prompts_by_account(account_id)
                if prompts:
                    prompt_options = ["All Prompts"] + [f"{p['name']} ({p['prompt_type']})" for p in prompts]
                    selected_prompt = st.selectbox(
                        "Filter by Prompt:",
                        prompt_options,
                        key=f"prompt_{section_name.lower()}"
                    )
                    if selected_prompt != "All Prompts":
                        # Extract prompt_id from selection
                        prompt_name = selected_prompt.split(" (")[0]
                        prompt_filter = next((p['prompt_id'] for p in prompts if p['name'] == prompt_name), None)
            except Exception as e:
                st.warning(f"Could not load prompts: {str(e)}")
    
    with col3:
        # Show relationship data toggle
        show_relationships = st.checkbox(
            "Show Relationship Data",
            value=True,
            key=f"show_rel_{section_name.lower()}"
        )
    
    # Fetch data if refresh is needed or session state is not set
    session_key_filtered = f"{session_key}_{account_id}_{workflow_status_filter}_{prompt_filter}"
    
    if st.session_state.refresh_stats or session_key_filtered not in st.session_state:
        try:
            if account_id and hasattr(db_module, f'get_{section_name.lower()}_by_account'):
                # Use account-specific method if available
                method_name = f'get_{section_name.lower()}_by_account'
                fetch_method = getattr(db_module, method_name)
                content_data = fetch_method(account_id, limit=100)
            elif show_relationships and hasattr(db_module, 'get_comprehensive_data'):
                # Use comprehensive method that includes relationship data
                content_data = db_module.get_comprehensive_data(limit=100, used=use_filter)
            else:
                # Fallback to basic method
                content_data = db_module.get_comprehensive_data(limit=100, used=use_filter)
            
            # Apply additional filters
            if workflow_status_filter != "All":
                content_data = [item for item in content_data 
                              if item.get('workflow_status') == workflow_status_filter]
            
            if prompt_filter:
                content_data = [item for item in content_data 
                              if item.get('prompt_id') == prompt_filter]
            
            st.session_state[session_key_filtered] = content_data
            
        except Exception as e:
            st.error(f"Error fetching {section_name.lower()}: {str(e)}")
            st.session_state[session_key_filtered] = []
        
        st.session_state.refresh_stats = False
    
    # Display data count
    data_count = len(st.session_state[session_key_filtered])
    st.info(f"Showing {data_count} {section_name.lower()} records")
    
    # Render data table
    if st.session_state[session_key_filtered]:
        try:
            df_data = format_content_dataframe(
                st.session_state[session_key_filtered], section_name.lower()
            )
            
            # Add export functionality
            col1, col2 = st.columns([3, 1])
            with col2:
                if st.button(f"Export {section_name} to CSV", key=f"export_{section_name.lower()}"):
                    csv = df_data.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"{section_name.lower()}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            
            # Display the table
            st.dataframe(df_data, use_container_width=True, hide_index=True)
            
            # Show additional insights if relationship data is available
            if show_relationships and any('username' in item for item in st.session_state[session_key_filtered]):
                render_relationship_insights(st.session_state[session_key_filtered], section_name)
                
        except Exception as e:
            st.error(f"Error rendering {section_name.lower()} table: {str(e)}")
    else:
        st.warning(f"No {filter_option.lower()} {section_name.lower()} found with the selected filters.")

def render_relationship_insights(content_data, section_name):
    """Render insights about relationships in the data."""
    st.subheader(f"📊 {section_name} Insights")
    
    try:
        df = pd.DataFrame(content_data)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Prompt performance
            if 'prompt_name' in df.columns and 'used' in df.columns:
                prompt_performance = df.groupby('prompt_name').agg({
                    'content': 'count',
                    'used': lambda x: x.sum() if x.dtype == 'bool' else (x == True).sum()
                }).rename(columns={'content': 'Total', 'used': 'Used'})
                
                if not prompt_performance.empty:
                    prompt_performance['Usage Rate %'] = (
                        prompt_performance['Used'] / prompt_performance['Total'] * 100
                    ).round(1)
                    st.write("**Prompt Performance:**")
                    st.dataframe(prompt_performance, use_container_width=True)
        
        with col2:
            # Workflow status distribution
            if 'workflow_status' in df.columns:
                status_dist = df['workflow_status'].value_counts()
                if not status_dist.empty:
                    st.write("**Workflow Status Distribution:**")
                    st.bar_chart(status_dist)
        
        # Account activity summary (if multiple accounts)
        if 'username' in df.columns:
            account_summary = df.groupby('username').agg({
                'content': 'count',
                'used': lambda x: x.sum() if x.dtype == 'bool' else (x == True).sum()
            }).rename(columns={'content': 'Total', 'used': 'Used'})
            
            if len(account_summary) > 1:  # Only show if multiple accounts
                account_summary['Usage Rate %'] = (
                    account_summary['Used'] / account_summary['Total'] * 100
                ).round(1)
                st.write(f"**Account Activity Summary:**")
                st.dataframe(account_summary, use_container_width=True)
                
    except Exception as e:
        st.error(f"Error generating insights: {str(e)}")

def render_analytics_table(analytics_data, title, data_type="general"):
    """Render analytics data in a formatted table."""
    if not analytics_data:
        st.warning(f"No data available for {title}")
        return
    
    st.subheader(title)
    try:
        df_analytics = format_analytics_dataframe(analytics_data, data_type)
        st.dataframe(df_analytics, use_container_width=True, hide_index=True)
        
        # Add export option for analytics
        if st.button(f"Export {title} to CSV", key=f"export_analytics_{data_type}"):
            csv = df_analytics.to_csv(index=False)
            st.download_button(
                label="Download Analytics CSV",
                data=csv,
                file_name=f"{title.lower().replace(' ', '_')}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key=f"download_analytics_{data_type}"
            )
    except Exception as e:
        st.error(f"Error rendering analytics table: {str(e)}")