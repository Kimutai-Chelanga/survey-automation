import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pymongo
from datetime import datetime, timedelta
import json
import os
from typing import Dict, List, Any, Optional
import numpy as np
import streamlit as st


class LogsPage:
    """Automa Workflow Log Viewer - Streamlit page class for viewing and analyzing workflow execution logs"""
    
  
    def __init__(self, db_manager):
        """Initialize the LogsPage with configuration and MongoDB connection"""
        self.db_manager = db_manager
        self.configure_page()
    def configure_page(self):
        """Configure Streamlit page settings"""
        st.set_page_config(
            page_title="Automa Workflow Log Viewer",
            page_icon="🔍",
            layout="wide",
            initial_sidebar_state="expanded"
        )
    
    @st.cache_resource
    def get_mongo_connection(_self):
        """Get MongoDB connection with caching"""
        mongo_uri = os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin')
        client = pymongo.MongoClient(mongo_uri)
        return client.messages_db

    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def load_workflow_executions(_self, days_back: int = 7) -> pd.DataFrame:
        """Load recent workflow executions"""
        db = _self.get_mongo_connection()
        
        # Get executions from last N days
        since_date = datetime.now() - timedelta(days=days_back)
        
        pipeline = [
            {
                "$match": {
                    "executed_at": {"$gte": since_date.isoformat()},
                    "executed": True
                }
            },
            {
                "$sort": {"executed_at": -1}
            }
        ]
        
        executions = list(db.workflow_executions.aggregate(pipeline))
        
        if not executions:
            return pd.DataFrame()
        
        df = pd.DataFrame(executions)
        df['executed_at'] = pd.to_datetime(df['executed_at'])
        
        return df

    @st.cache_data(ttl=300)
    def load_automa_execution_logs(_self, execution_ids: List[str] = None) -> pd.DataFrame:
        """Load Automa execution logs"""
        db = _self.get_mongo_connection()
        
        query = {}
        if execution_ids:
            query = {"execution_id": {"$in": execution_ids}}
        
        logs = list(db.automa_execution_logs.find(query).sort("captured_at", -1).limit(1000))
        
        if not logs:
            return pd.DataFrame()
        
        df = pd.DataFrame(logs)
        df['captured_at'] = pd.to_datetime(df['captured_at'])
        
        return df

    @st.cache_data(ttl=300)
    def load_performance_metrics(_self, days_back: int = 7) -> pd.DataFrame:
        """Load performance metrics"""
        db = _self.get_mongo_connection()
        
        since_date = datetime.now() - timedelta(days=days_back)
        
        metrics = list(db.automa_performance_metrics.find({
            "measured_at": {"$gte": since_date.isoformat()}
        }).sort("measured_at", -1))
        
        if not metrics:
            return pd.DataFrame()
        
        df = pd.DataFrame(metrics)
        df['measured_at'] = pd.to_datetime(df['measured_at'])
        
        return df

    def get_log_details(self, execution_id: str) -> Dict[str, Any]:
        """Get detailed log information for a specific execution"""
        db = self.get_mongo_connection()
        
        # Get main log entry
        main_log = db.automa_execution_logs.find_one({"execution_id": execution_id})
        
        if not main_log:
            return {}
        
        # Get related data
        log_items = list(db.automa_log_items.find({"execution_id": execution_id}))
        log_histories = list(db.automa_log_histories.find({"execution_id": execution_id}).sort("step_order", 1))
        context_data = list(db.automa_log_context_data.find({"execution_id": execution_id}))
        logs_data = list(db.automa_logs_data.find({"execution_id": execution_id}))
        
        return {
            "main_log": main_log,
            "log_items": log_items,
            "log_histories": log_histories,
            "context_data": context_data,
            "logs_data": logs_data
        }

    def render_sidebar(self) -> tuple:
        """Render sidebar with navigation and filters"""
        st.sidebar.title("🔍 Automa Log Viewer")

        # Time range selector
        days_back = st.sidebar.selectbox(
            "Time Range",
            options=[1, 3, 7, 14, 30],
            index=2,
            format_func=lambda x: f"Last {x} day{'s' if x > 1 else ''}"
        )

        # Navigation
        page = st.sidebar.selectbox(
            "Select View",
            ["Dashboard", "Workflow Executions", "Log Details", "Performance Metrics", "Raw Data Explorer"]
        )

        # Tools section
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 🔧 Tools")
        if st.sidebar.button("Refresh Data"):
            st.cache_data.clear()
            st.rerun()

        # Stats section
        st.sidebar.markdown("### 📊 Stats")
        try:
            db = self.get_mongo_connection()
            total_executions = db.workflow_executions.count_documents({"executed": True})
            total_logs = db.automa_execution_logs.count_documents({})
            st.sidebar.metric("Total Executions", total_executions)
            st.sidebar.metric("Total Logs", total_logs)
        except Exception as e:
            st.sidebar.error("Unable to load stats")

        return days_back, page

    def render_dashboard(self, executions_df: pd.DataFrame):
        """Render the main dashboard page"""
        st.title("📊 Automa Workflow Dashboard")
        
        if executions_df.empty:
            st.warning("No workflow executions found in the selected time range.")
            return
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_executions = len(executions_df)
            st.metric("Total Executions", total_executions)
        
        with col2:
            successful_executions = len(executions_df[executions_df.get('success', False) == True])
            success_rate = (successful_executions / total_executions * 100) if total_executions > 0 else 0
            st.metric("Success Rate", f"{success_rate:.1f}%")
        
        with col3:
            logs_captured = len(executions_df[executions_df.get('logs_captured', False) == True])
            capture_rate = (logs_captured / total_executions * 100) if total_executions > 0 else 0
            st.metric("Log Capture Rate", f"{capture_rate:.1f}%")
        
        with col4:
            if 'execution_duration' in executions_df.columns:
                avg_duration = executions_df['execution_duration'].mean()
                st.metric("Avg Duration", f"{avg_duration:.0f}ms")
            else:
                st.metric("Avg Duration", "N/A")
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Executions Over Time")
            if not executions_df.empty:
                # Group by hour for better visualization
                executions_df_copy = executions_df.copy()
                executions_df_copy['hour'] = executions_df_copy['executed_at'].dt.floor('H')
                hourly_counts = executions_df_copy.groupby('hour').size().reset_index(name='count')
                
                fig = px.line(hourly_counts, x='hour', y='count', 
                             title='Workflow Executions by Hour')
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Success Rate by Workflow Type")
            if 'workflow_type' in executions_df.columns:
                success_by_type = executions_df.groupby('workflow_type').agg({
                    'success': 'mean',
                    '_id': 'count'
                }).reset_index()
                success_by_type.columns = ['workflow_type', 'success_rate', 'count']
                success_by_type['success_rate'] *= 100
                
                fig = px.bar(success_by_type, x='workflow_type', y='success_rate',
                            title='Success Rate by Workflow Type (%)',
                            hover_data=['count'])
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
        
        # Recent executions table
        st.subheader("Recent Executions")
        display_columns = ['workflow_type', 'executed_at', 'success', 'logs_captured', 'execution_duration']
        available_columns = [col for col in display_columns if col in executions_df.columns]
        display_df = executions_df[available_columns].copy()
        if 'executed_at' in display_df.columns:
            display_df['executed_at'] = display_df['executed_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
        st.dataframe(display_df, use_container_width=True)

    def render_workflow_executions(self, executions_df: pd.DataFrame):
        """Render the workflow executions page"""
        st.title("📝 Workflow Executions")
        
        if executions_df.empty:
            st.warning("No workflow executions found.")
            return
        
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            workflow_types = ["All"] + list(executions_df['workflow_type'].unique()) if 'workflow_type' in executions_df.columns else ["All"]
            selected_type = st.selectbox("Workflow Type", workflow_types)
        
        with col2:
            success_filter = st.selectbox("Success Status", ["All", "Success", "Failed"])
        
        with col3:
            log_capture_filter = st.selectbox("Log Capture", ["All", "Captured", "Not Captured"])
        
        # Apply filters
        filtered_df = executions_df.copy()
        
        if selected_type != "All":
            filtered_df = filtered_df[filtered_df['workflow_type'] == selected_type]
        
        if success_filter == "Success":
            filtered_df = filtered_df[filtered_df.get('success', False) == True]
        elif success_filter == "Failed":
            filtered_df = filtered_df[filtered_df.get('success', False) == False]
        
        if log_capture_filter == "Captured":
            filtered_df = filtered_df[filtered_df.get('logs_captured', False) == True]
        elif log_capture_filter == "Not Captured":
            filtered_df = filtered_df[filtered_df.get('logs_captured', False) == False]
        
        # Display results
        st.write(f"Showing {len(filtered_df)} of {len(executions_df)} executions")
        
        # Detailed table with selection
        selected_columns = ['workflow_type', 'executed_at', 'success', 'logs_captured', 'execution_duration', 'execution_id']
        available_columns = [col for col in selected_columns if col in filtered_df.columns]
        
        display_df = filtered_df[available_columns].copy()
        if 'executed_at' in display_df.columns:
            display_df['executed_at'] = display_df['executed_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Add selection for log viewing
        selected_rows = st.dataframe(
            display_df, 
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row"
        )
        
        # Show log details for selected execution
        if selected_rows['selection']['rows']:
            selected_idx = selected_rows['selection']['rows'][0]
            selected_execution = filtered_df.iloc[selected_idx]
            execution_id = selected_execution.get('execution_id')
            
            if execution_id:
                with st.expander("View Log Details", expanded=True):
                    log_details = self.get_log_details(execution_id)
                    
                    if log_details:
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.subheader("Execution Summary")
                            main_log = log_details['main_log']
                            st.json({
                                "Workflow Name": main_log.get('workflow_name'),
                                "Execution Duration": f"{main_log.get('execution_duration')}ms",
                                "Captured At": main_log.get('captured_at'),
                                "Log Items": len(log_details['log_items']),
                                "Log Histories": len(log_details['log_histories']),
                            })
                        
                        with col2:
                            st.subheader("Log Data Summary")
                            log_summary = main_log.get('log_summary', {})
                            st.json(log_summary)
                        
                        # Step histories
                        if log_details['log_histories']:
                            st.subheader("Execution Steps")
                            steps_df = pd.DataFrame(log_details['log_histories'])
                            if not steps_df.empty:
                                st.dataframe(steps_df, use_container_width=True)

    def render_performance_metrics(self, performance_df: pd.DataFrame):
        """Render the performance metrics page"""
        st.title("⚡ Performance Metrics")
        
        if performance_df.empty:
            st.warning("No performance metrics found.")
            return
        
        # Performance overview
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_duration = performance_df['total_execution_duration'].mean()
            st.metric("Avg Execution Time", f"{avg_duration:.0f}ms")
        
        with col2:
            avg_injection = performance_df['workflow_injection_time'].mean()
            st.metric("Avg Injection Time", f"{avg_injection:.0f}ms")
        
        with col3:
            avg_trigger = performance_df['workflow_trigger_time'].mean()
            st.metric("Avg Trigger Time", f"{avg_trigger:.0f}ms")
        
        with col4:
            capture_success = performance_df['log_capture_success'].mean() * 100
            st.metric("Log Capture Success", f"{capture_success:.1f}%")
        
        # Performance trends
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Execution Time Trends")
            fig = px.scatter(performance_df, x='measured_at', y='total_execution_duration',
                            color='workflow_type', title='Execution Duration Over Time')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Performance Breakdown")
            avg_metrics = performance_df.groupby('workflow_type').agg({
                'chrome_connection_time': 'mean',
                'workflow_injection_time': 'mean', 
                'workflow_trigger_time': 'mean',
                'log_capture_time': 'mean'
            }).reset_index()
            
            # Melt for stacked bar chart
            melted = avg_metrics.melt(id_vars='workflow_type', var_name='metric', value_name='time')
            fig = px.bar(melted, x='workflow_type', y='time', color='metric',
                        title='Average Time by Component')
            st.plotly_chart(fig, use_container_width=True)

    def render_raw_data_explorer(self):
        """Render the raw data explorer page"""
        st.title("🔍 Raw Data Explorer")
        
        # Collection selector
        collections = [
            "workflow_executions",
            "automa_execution_logs", 
            "automa_log_items",
            "automa_log_histories",
            "automa_performance_metrics"
        ]
        
        selected_collection = st.selectbox("Select Collection", collections)
        
        # Load and display raw data
        db = self.get_mongo_connection()
        
        # Limit and query options
        col1, col2 = st.columns(2)
        
        with col1:
            limit = st.number_input("Limit", min_value=1, max_value=1000, value=100)
        
        with col2:
            sort_field = st.text_input("Sort Field (optional)", placeholder="e.g., captured_at")
        
        # Query input
        query_input = st.text_area("MongoDB Query (JSON)", placeholder='{"status": "completed"}')
        
        if st.button("Execute Query"):
            try:
                # Parse query
                query = json.loads(query_input) if query_input.strip() else {}
                
                # Execute query
                collection = db[selected_collection]
                cursor = collection.find(query)
                
                if sort_field:
                    cursor = cursor.sort(sort_field, -1)
                
                cursor = cursor.limit(limit)
                results = list(cursor)
                
                if results:
                    st.success(f"Found {len(results)} documents")
                    
                    # Convert to DataFrame for better display
                    df = pd.DataFrame(results)
                    
                    # Handle ObjectId and datetime columns
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            # Try to convert datetime strings
                            try:
                                df[col] = pd.to_datetime(df[col])
                            except:
                                # Convert ObjectId and other objects to strings
                                df[col] = df[col].astype(str)
                    
                    st.dataframe(df, use_container_width=True)
                    
                    # Export option
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="Download as CSV",
                        data=csv,
                        file_name=f"{selected_collection}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                    
                else:
                    st.info("No documents found matching the query")
                    
            except json.JSONDecodeError:
                st.error("Invalid JSON query format")
            except Exception as e:
                st.error(f"Query error: {str(e)}")

    def render(self):
        """Main render method - call this to display the entire logs page"""
        # Configure page settings if not already done
        if not hasattr(st.session_state, 'logs_page_configured'):
            self.configure_page()
            st.session_state.logs_page_configured = True
        
        # Render sidebar and get user selections
        days_back, page = self.render_sidebar()
        
        # Load data based on user selections
        with st.spinner("Loading workflow data..."):
            executions_df = self.load_workflow_executions(days_back)
            automa_logs_df = self.load_automa_execution_logs()
            performance_df = self.load_performance_metrics(days_back)
        
        # Render selected page
        if page == "Dashboard":
            self.render_dashboard(executions_df)
        elif page == "Workflow Executions":
            self.render_workflow_executions(executions_df)
        elif page == "Performance Metrics":
            self.render_performance_metrics(performance_df)
        elif page == "Raw Data Explorer":
            self.render_raw_data_explorer()
        else:
            st.error(f"Unknown page: {page}")

    def configure_page(self):
        """Configure Streamlit page settings - only call once"""
        # Note: This might already be configured by your main app
        # so we'll make it conditional
        try:
            st.set_page_config(
                page_title="Automa Workflow Log Viewer",
                page_icon="🔍",
                layout="wide",
                initial_sidebar_state="expanded"
            )
        except st.errors.StreamlitAPIException:
            # Page config already set, ignore
            pass