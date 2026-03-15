"""Enhanced Section renderer with workflow linkage filtering"""
import logging
import pandas as pd
import streamlit as st
from .data_formatter import format_content_dataframe
from src.core.database.postgres import prompts as pg_utils

logger = logging.getLogger(__name__)


class SectionRenderer:
    """Enhanced renderer for content sections with workflow linkage filtering."""
    
    def __init__(self, section_name, icon, db_module, filter_key, session_key, account_id=None):
        self.section_name = section_name
        self.icon = icon
        self.db_module = db_module
        self.filter_key = filter_key
        self.session_key = session_key
        self.account_id = account_id
        self.id_field = self._get_id_field()
        self.mark_used_function = self._get_mark_used_function()
        self.get_data_function = self._get_data_function()
    
    def _get_id_field(self) -> str:
        """Get the appropriate ID field name for this content type."""
        return {
            "Replies": "replies_id",
            "Messages": "messages_id",
            "Retweets": "retweets_id"
        }.get(self.section_name, "id")
    
    def _get_mark_used_function(self):
        """Get the appropriate mark_as_used function for this content type."""
        try:
            function_map = {
                "Replies": "mark_reply_as_used",
                "Messages": "mark_message_as_used",
                "Retweets": "mark_retweet_as_used"
            }
            func_name = function_map.get(self.section_name)
            if func_name and hasattr(self.db_module, func_name):
                return getattr(self.db_module, func_name)
        except AttributeError:
            logger.warning(f"Mark used function not found for {self.section_name}")
        return None
    
    def _get_data_function(self):
        """Get the appropriate data retrieval function."""
        try:
            # Try to get comprehensive data function
            if hasattr(self.db_module, 'get_comprehensive_data'):
                return self.db_module.get_comprehensive_data
            # Fallback to basic retrieval
            elif hasattr(self.db_module, f'get_{self.section_name.lower()}'):
                return getattr(self.db_module, f'get_{self.section_name.lower()}')
        except AttributeError:
            logger.warning(f"Data function not found for {self.section_name}")
        return None
    
    def render_section(self):
        """Render the complete section with stats, filters, and data table."""
        st.subheader(f"{self.icon} {self.section_name}")
        
        # Display statistics
        self._display_stats()
        
        st.divider()
        
        # Render filter options
        self._render_filters()
        
        st.divider()
        
        # Render data table
        self._render_data_table()
    
    def _display_stats(self):
        """Display enhanced statistics including workflow linkage."""
        try:
            if self.account_id:
                stats_method_name = f'get_account_{self.section_name.lower()}_statistics'
                if hasattr(self.db_module, stats_method_name):
                    stats = getattr(self.db_module, stats_method_name)(self.account_id)
                else:
                    stats = self.db_module.get_detailed_stats()
            else:
                stats = self.db_module.get_detailed_stats()
            
            content_key = self.section_name.lower()
            
            # Create 5 columns for enhanced metrics
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric(
                    f"Total {self.section_name}",
                    stats.get(f'total_{content_key}', 0)
                )
            
            with col2:
                st.metric(
                    f"Used",
                    stats.get(f'used_{content_key}', 0)
                )
            
            with col3:
                st.metric(
                    f"Unused",
                    stats.get(f'unused_{content_key}', 0)
                )
            
            with col4:
                # Workflow linked metric
                workflow_linked = stats.get(f'workflow_linked_{content_key}', 0)
                st.metric("Workflow Linked", workflow_linked)
            
            with col5:
                # Usage rate
                if 'usage_rate' in stats:
                    st.metric("Usage Rate", f"{stats['usage_rate']:.1f}%")
                else:
                    # Calculate linkage rate
                    total = stats.get(f'total_{content_key}', 0)
                    linked = stats.get(f'workflow_linked_{content_key}', 0)
                    linkage_rate = (linked / total * 100) if total > 0 else 0
                    st.metric("Linkage Rate", f"{linkage_rate:.1f}%")
        
        except Exception as e:
            logger.error(f"Error fetching {self.section_name.lower()} stats: {e}")
            st.error(f"Error fetching stats: {str(e)}")
    
    def _render_filters(self):
        """Render enhanced filter options including workflow linkage."""
        st.write("**Filters:**")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            # Show only unused toggle
            show_unused_only = st.checkbox(
                "Unused Only",
                value=True,
                key=f"show_unused_{self.section_name.lower()}"
            )
        
        with col2:
            # Workflow linkage filter (NEW)
            workflow_filter_options = ["All", "Linked", "Unlinked"]
            workflow_linkage_filter = st.selectbox(
                "Workflow Status:",
                workflow_filter_options,
                key=f"workflow_status_{self.section_name.lower()}"
            )
        
        with col3:
            # Generated Workflow ID dropdown (NEW)
            workflow_id_filter = self._render_workflow_id_dropdown()
        
        with col4:
            # Prompt filter (if account is selected)
            prompt_filter = None
            if self.account_id:
                try:
                    prompts = pg_utils.get_prompts_by_account(self.account_id)
                    if prompts:
                        prompt_options = ["All Prompts"] + [
                            f"{p['prompt_name']} ({p['prompt_type']})" for p in prompts
                        ]
                        selected_prompt = st.selectbox(
                            "Prompt:",
                            prompt_options,
                            key=f"prompt_{self.section_name.lower()}"
                        )
                        if selected_prompt != "All Prompts":
                            prompt_name = selected_prompt.split(" (")[0]
                            prompt_filter = next(
                                (p['prompt_id'] for p in prompts if p['prompt_name'] == prompt_name), 
                                None
                            )
                except Exception as e:
                    logger.error(f"Error loading prompts: {e}")
        
        # Store filters in session state
        filters_key = f"{self.section_name.lower()}_filters"
        st.session_state[filters_key] = {
            "show_unused_only": show_unused_only,
            "workflow_linkage": workflow_linkage_filter,
            "workflow_id": workflow_id_filter,
            "prompt_id": prompt_filter
        }
    
    def _render_workflow_id_dropdown(self):
        """Render dropdown for selecting specific generated_workflow_id."""
        try:
            # Get all workflow IDs for this content type and account
            with st.spinner("Loading workflows..."):
                if self.get_data_function:
                    if self.account_id:
                        all_data = self.get_data_function(account_id=self.account_id, limit=500)
                    else:
                        all_data = self.get_data_function(limit=500)
                    
                    # Extract unique workflow IDs
                    workflow_ids = set()
                    for item in all_data:
                        wf_id = item.get('generated_workflow_id') or item.get('workflow_id')
                        if wf_id:
                            workflow_ids.add(str(wf_id))
                    
                    if workflow_ids:
                        workflow_options = ["All Workflows"] + sorted(list(workflow_ids))
                        selected_workflow = st.selectbox(
                            "Workflow ID:",
                            workflow_options,
                            key=f"workflow_id_dropdown_{self.section_name.lower()}"
                        )
                        
                        if selected_workflow != "All Workflows":
                            return selected_workflow
                    else:
                        st.info("No workflows found")
                else:
                    st.warning("Data function not available")
        except Exception as e:
            logger.error(f"Error loading workflow IDs: {e}")
            st.error(f"Error loading workflows: {str(e)}")
        
        return None
    
    def _render_data_table(self):
        """Render the data table with enhanced filtering."""
        st.subheader(f"Content ({self.section_name})")
        
        try:
            # Get filters from session state
            filters = st.session_state.get(f"{self.section_name.lower()}_filters", {})
            show_unused_only = filters.get("show_unused_only", True)
            workflow_linkage = filters.get("workflow_linkage", "All")
            workflow_id_filter = filters.get("workflow_id")
            prompt_filter = filters.get("prompt_id")
            
            # Fetch data
            content_data = self._fetch_content_data(
                show_unused_only, 
                workflow_linkage, 
                workflow_id_filter, 
                prompt_filter
            )
            
            # Store in session state
            session_key_filtered = f"{self.session_key}_{self.account_id}_{workflow_linkage}_{workflow_id_filter}_{prompt_filter}"
            st.session_state[session_key_filtered] = content_data
            
            # Display data count
            data_count = len(content_data)
            
            # Enhanced info with breakdown
            if workflow_linkage == "All":
                linked_count = sum(1 for item in content_data if item.get('workflow_linked'))
                unlinked_count = data_count - linked_count
                st.info(f"Showing {data_count} records: {linked_count} linked, {unlinked_count} unlinked")
            else:
                st.info(f"Showing {data_count} {workflow_linkage.lower()} records")
            
            if content_data:
                # Render data table with actions
                self._render_content_table(content_data)
                
                # Show relationship insights
                if any('username' in item for item in content_data):
                    st.divider()
                    self._render_relationship_insights(content_data)
            else:
                st.warning(f"No {self.section_name.lower()} found with the selected filters.")
        
        except Exception as e:
            logger.error(f"Error rendering {self.section_name.lower()} table: {e}")
            st.error(f"Error rendering table: {str(e)}")
    
    def _fetch_content_data(self, show_unused_only: bool, workflow_linkage: str, 
                           workflow_id_filter: str, prompt_filter) -> list:
        """Fetch content data with enhanced filtering."""
        try:
            # Get base data
            if self.get_data_function:
                if self.account_id:
                    content_data = self.get_data_function(account_id=self.account_id, limit=500)
                else:
                    content_data = self.get_data_function(limit=500)
            else:
                content_data = []
            
            # Apply used/unused filter
            if show_unused_only:
                content_data = [
                    item for item in content_data
                    if not item.get('used', False)
                ]
            
            # Apply workflow linkage filter (NEW)
            if workflow_linkage == "Linked":
                content_data = [
                    item for item in content_data
                    if item.get('workflow_linked', False)
                ]
            elif workflow_linkage == "Unlinked":
                content_data = [
                    item for item in content_data
                    if not item.get('workflow_linked', False)
                ]
            
            # Apply specific workflow_id filter (NEW)
            if workflow_id_filter:
                content_data = [
                    item for item in content_data
                    if str(item.get('generated_workflow_id')) == workflow_id_filter or 
                       str(item.get('workflow_id')) == workflow_id_filter
                ]
            
            # Apply prompt filter
            if prompt_filter:
                content_data = [
                    item for item in content_data
                    if item.get('prompt_id') == prompt_filter
                ]
            
            return content_data
        
        except Exception as e:
            logger.error(f"Error fetching content data: {e}")
            return []
    
    def _render_content_table(self, content_data: list):
        """Render the content table with action buttons."""
        try:
            # Create tabs
            view_tab, manage_tab, workflow_tab = st.tabs(["View Data", "Manage Content", "Workflow Info"])
            
            with view_tab:
                # Format and display dataframe
                df_data = format_content_dataframe(content_data, self.section_name.lower())
                
                # Add workflow linkage columns if not present
                if 'workflow_linked' not in df_data.columns and len(content_data) > 0:
                    df_data['workflow_linked'] = [item.get('workflow_linked', False) for item in content_data]
                
                if 'generated_workflow_id' not in df_data.columns and len(content_data) > 0:
                    df_data['generated_workflow_id'] = [
                        str(item.get('generated_workflow_id', 'N/A'))[:8] + '...' 
                        if item.get('generated_workflow_id') else 'N/A'
                        for item in content_data
                    ]
                
                # Add export button
                col1, col2 = st.columns([3, 1])
                with col2:
                    if st.button(f"Export to CSV", key=f"export_{self.section_name.lower()}"):
                        csv = df_data.to_csv(index=False)
                        st.download_button(
                            label="Download CSV",
                            data=csv,
                            file_name=f"{self.section_name.lower()}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                
                # Display table
                st.dataframe(df_data, use_container_width=True, hide_index=True)
            
            with manage_tab:
                self._render_manage_tab(content_data)
            
            with workflow_tab:
                self._render_workflow_info_tab(content_data)
        
        except Exception as e:
            logger.error(f"Error rendering content table: {e}")
            st.error(f"Error rendering table: {str(e)}")
    
    def _render_manage_tab(self, content_data: list):
        """Render the manage tab for marking content as used."""
        st.write("**Mark Content as Used:**")
        
        if not content_data:
            st.info("No content available to manage")
            return
        
        try:
            df = pd.DataFrame(content_data)
            
            # Create content preview options
            content_options = {}
            for _, row in df.iterrows():
                content_id = row[self.id_field]
                content_preview = row['content'][:60] + "..." if len(row['content']) > 60 else row['content']
                workflow_status = "✓ Linked" if row.get('workflow_linked') else "✗ Unlinked"
                label = f"{content_id}: {content_preview} | {workflow_status}"
                content_options[label] = content_id
            
            # Select content to mark as used
            selected_preview = st.selectbox(
                f"Select {self.section_name.lower()} to mark as used:",
                list(content_options.keys()),
                key=f"select_{self.section_name.lower()}_manage"
            )
            
            selected_id = content_options[selected_preview]
            selected_row = df[df[self.id_field] == selected_id].iloc[0]
            
            # Display content details
            st.write("**Selected Content Details:**")
            
            st.text_area(
                "Content:",
                value=selected_row['content'],
                height=100,
                disabled=True,
                key=f"preview_{self.section_name.lower()}"
            )
            
            # Display metadata in columns
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.caption(f"Status: {selected_row.get('workflow_status', 'unknown')}")
            with col2:
                st.caption(f"Used: {'Yes' if selected_row.get('used') else 'No'}")
            with col3:
                workflow_linked = selected_row.get('workflow_linked', False)
                st.caption(f"Linked: {'✓ Yes' if workflow_linked else '✗ No'}")
            with col4:
                gen_wf_id = selected_row.get('generated_workflow_id', 'N/A')
                display_id = f"{str(gen_wf_id)[:8]}..." if gen_wf_id != 'N/A' else 'N/A'
                st.caption(f"Gen ID: {display_id}")
            with col5:
                created = selected_row.get('created_time', 'unknown')
                st.caption(f"Created: {created}")
            
            # Mark as used button
            if st.button(
                "Mark as Used",
                key=f"mark_used_{self.section_name.lower()}_{selected_id}",
                use_container_width=True
            ):
                try:
                    if self.mark_used_function:
                        self.mark_used_function(selected_id)
                        st.success(f"{self.section_name.rstrip('s')} marked as used!")
                        st.info("Refreshing content list...")
                        st.rerun()
                    else:
                        st.error(f"Cannot mark {self.section_name.lower()} as used - function not available")
                except Exception as e:
                    logger.error(f"Error marking {self.section_name.lower()} as used: {e}")
                    st.error(f"Error marking as used: {str(e)}")
        
        except Exception as e:
            logger.error(f"Error in manage tab: {e}")
            st.error(f"Error in manage tab: {str(e)}")
    
    def _render_workflow_info_tab(self, content_data: list):
        """Render workflow information tab (NEW)."""
        st.write("**Workflow Linkage Information:**")
        
        if not content_data:
            st.info("No content available")
            return
        
        try:
            df = pd.DataFrame(content_data)
            
            # Overall workflow stats
            total = len(df)
            linked = df['workflow_linked'].sum() if 'workflow_linked' in df.columns else 0
            unlinked = total - linked
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Content", total)
            with col2:
                st.metric("Linked to Workflows", linked)
            with col3:
                st.metric("Unlinked", unlinked)
            
            # Workflow ID breakdown
            if 'generated_workflow_id' in df.columns:
                st.divider()
                st.write("**Workflow ID Distribution:**")
                
                workflow_counts = df[df['generated_workflow_id'].notna()]['generated_workflow_id'].value_counts()
                if not workflow_counts.empty:
                    workflow_df = pd.DataFrame({
                        'Workflow ID': [str(wf_id)[:16] + '...' for wf_id in workflow_counts.index],
                        'Content Count': workflow_counts.values
                    })
                    st.dataframe(workflow_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No workflow IDs assigned yet")
            
            # Prompt performance with workflow linkage
            if 'prompt_name' in df.columns and 'workflow_linked' in df.columns:
                st.divider()
                st.write("**Prompt Performance & Workflow Linkage:**")
                
                prompt_stats = df.groupby('prompt_name').agg({
                    'content': 'count',
                    'used': lambda x: (x == True).sum(),
                    'workflow_linked': lambda x: (x == True).sum()
                }).rename(columns={
                    'content': 'Total',
                    'used': 'Used',
                    'workflow_linked': 'Linked'
                })
                
                if not prompt_stats.empty:
                    prompt_stats['Linkage Rate %'] = (
                        prompt_stats['Linked'] / prompt_stats['Total'] * 100
                    ).round(1)
                    st.dataframe(prompt_stats, use_container_width=True)
        
        except Exception as e:
            logger.error(f"Error in workflow info tab: {e}")
            st.error(f"Error displaying workflow info: {str(e)}")
    
    def _render_relationship_insights(self, content_data: list):
        """Render insights about relationships in the data."""
        st.subheader(f"Insights")
        
        try:
            df = pd.DataFrame(content_data)
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Workflow linkage trends
                if 'workflow_linked' in df.columns and 'created_time' in df.columns:
                    st.write("**Workflow Linkage Trends:**")
                    
                    # Convert created_time to datetime if string
                    if df['created_time'].dtype == 'object':
                        df['created_time'] = pd.to_datetime(df['created_time'])
                    
                    # Group by date
                    df['date'] = df['created_time'].dt.date
                    linkage_trends = df.groupby('date').agg({
                        'content': 'count',
                        'workflow_linked': lambda x: (x == True).sum()
                    }).rename(columns={'content': 'Total', 'workflow_linked': 'Linked'})
                    
                    linkage_trends['Linkage %'] = (
                        linkage_trends['Linked'] / linkage_trends['Total'] * 100
                    ).round(1)
                    
                    st.dataframe(linkage_trends, use_container_width=True)
            
            with col2:
                # Prompt performance
                if 'prompt_name' in df.columns and 'used' in df.columns:
                    st.write("**Prompt Performance:**")
                    
                    prompt_performance = df.groupby('prompt_name').agg({
                        'content': 'count',
                        'used': lambda x: (x == True).sum()
                    }).rename(columns={'content': 'Total', 'used': 'Used'})
                    
                    if not prompt_performance.empty:
                        prompt_performance['Usage Rate %'] = (
                            prompt_performance['Used'] / prompt_performance['Total'] * 100
                        ).round(1)
                        st.dataframe(prompt_performance, use_container_width=True)
        
        except Exception as e:
            logger.error(f"Error generating insights: {e}")
            st.error(f"Error generating insights: {str(e)}")