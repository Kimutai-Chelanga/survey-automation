"""
FILE: ui/components/workflow_filters.py
Complete updated WorkflowFilters class with actual_content support
"""

import streamlit as st
from datetime import datetime, timedelta
from typing import Dict, Any, List
from src.core.database.mongodb.connection import get_mongo_collection


class WorkflowFilters:
    """Enhanced class to handle hierarchical workflow filters for separated schema architecture."""
    
    def __init__(self, workflow_type: str):
        self.workflow_type = workflow_type
        self.status_filter = None
        self.executed_filter = None
        self.success_filter = None
        self.has_link_filter = None
        self.has_content_filter = None
        self.user_id_filter = None
        self.workflow_name_filter = None
        self.date_filter = None
        self.content_id_filter = None
        self.sort_by_filter = None
        
        # New hierarchical filters
        self.link_date_filter = None
        self.specific_link_filter = None
        self.content_type_filter = None
        self.execution_result_filter = None
        self.account_filter = None
        
        # NEW: Content preview filter
        self.content_preview_filter = None
        self.actual_content_filter = None
    
    def render_filters(self):
        """Render enhanced hierarchical filters with dependencies."""
        st.subheader("Filter Workflows")
        
        tab_key = self.workflow_type.lower()
        
        # ============= PRIMARY FILTERS =============
        st.markdown("**🎯 Primary Filters**")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            self.has_link_filter = st.checkbox(
                "Has Link", 
                key=f"workflow_has_link_filter_{tab_key}",
                help="Filter workflows that have associated links"
            )
        
        with col2:
            self.has_content_filter = st.checkbox(
                "Has Content", 
                key=f"workflow_has_content_filter_{tab_key}",
                help="Filter workflows with content integrated"
            )
        
        with col3:
            self.executed_filter = st.checkbox(
                "Executed", 
                key=f"workflow_executed_filter_{tab_key}",
                help="Filter workflows that have been executed"
            )
        
        with col4:
            show_additional = st.checkbox(
                "Show More Filters",
                key=f"workflow_show_additional_{tab_key}",
                help="Show additional filtering options"
            )
        
        # ============= HIERARCHICAL: HAS LINK OPTIONS =============
        if self.has_link_filter:
            st.markdown("---")
            st.markdown("**🔗 Link Filter Options**")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Date filter for links
                date_filter_type = st.selectbox(
                    "Filter by Date:",
                    ["None", "Specific Date", "Date Range", "Last 7 Days", "Last 30 Days"],
                    key=f"link_date_filter_type_{tab_key}"
                )
                
                if date_filter_type == "Specific Date":
                    self.link_date_filter = {
                        "type": "specific",
                        "date": st.date_input(
                            "Select Date:",
                            key=f"link_specific_date_{tab_key}"
                        )
                    }
                elif date_filter_type == "Date Range":
                    date_col1, date_col2 = st.columns(2)
                    with date_col1:
                        start_date = st.date_input(
                            "Start Date:",
                            key=f"link_start_date_{tab_key}"
                        )
                    with date_col2:
                        end_date = st.date_input(
                            "End Date:",
                            key=f"link_end_date_{tab_key}"
                        )
                    self.link_date_filter = {
                        "type": "range",
                        "start": start_date,
                        "end": end_date
                    }
                elif date_filter_type == "Last 7 Days":
                    self.link_date_filter = {
                        "type": "last_7_days",
                        "date": datetime.now() - timedelta(days=7)
                    }
                elif date_filter_type == "Last 30 Days":
                    self.link_date_filter = {
                        "type": "last_30_days",
                        "date": datetime.now() - timedelta(days=30)
                    }
            
            with col2:
                # Specific link filter
                self.specific_link_filter = st.text_input(
                    "Filter by Specific Link:",
                    placeholder="Enter link URL or part of URL",
                    key=f"specific_link_filter_{tab_key}",
                    help="Search for workflows containing this specific link"
                )
        
        # ============= HIERARCHICAL: HAS CONTENT OPTIONS =============
        if self.has_content_filter:
            st.markdown("---")
            st.markdown("**📝 Content Filter Options**")
            
            # Content type multi-select
            self.content_type_filter = st.multiselect(
                "Filter by Content Types:",
                ["Replies", "Messages", "Retweets"],
                default=[],
                key=f"content_type_filter_{tab_key}",
                help="Select one or more content types"
            )
        
        # ============= HIERARCHICAL: EXECUTED OPTIONS =============
        if self.executed_filter:
            st.markdown("---")
            st.markdown("**⚙️ Execution Filter Options**")
            
            self.execution_result_filter = st.radio(
                "Execution Result:",
                ["All Executed", "Successful Only", "Failed Only"],
                key=f"execution_result_filter_{tab_key}",
                horizontal=True,
                help="Filter by execution outcome"
            )
        
        # ============= ACCOUNT FILTER & ADDITIONAL =============
        if show_additional:
            st.markdown("---")
            st.markdown("**👤 Account Filter**")
            
            # Get available accounts
            available_accounts = self._get_available_accounts()
            
            self.account_filter = st.multiselect(
                "Filter by Account(s):",
                options=available_accounts,
                default=[],
                key=f"account_filter_{tab_key}",
                help="Select one or more accounts to filter workflows"
            )
            
            # ============= ADDITIONAL FILTERS =============
            st.markdown("---")
            st.markdown("**🔍 Additional Filters**")
            
            col1, col2 = st.columns(2)
            
            with col1:
                self.workflow_name_filter = st.text_input(
                    "Workflow Name:",
                    key=f"workflow_name_filter_{tab_key}",
                    placeholder="Search by workflow name",
                    help="Partial name matching supported"
                )
            
            with col2:
                pass  # Empty column for spacing
            
            # ============= NEW: CONTENT FILTER SECTION =============
            st.markdown("---")
            st.markdown("**📝 Content Filter**")
            
            content_filter_type = st.radio(
                "Filter by Content:",
                ["No Filter", "By Content ID", "By Content Preview"],
                key=f"content_filter_type_{tab_key}",
                horizontal=True
            )
            
            if content_filter_type == "By Content ID":
                self.content_id_filter = st.number_input(
                    "Enter Content ID:",
                    min_value=1,
                    value=None,
                    key=f"content_id_input_{tab_key}"
                )
            elif content_filter_type == "By Content Preview":
                actual_contents = self._get_available_content_preview()
                if actual_contents:
                    selected_content = st.selectbox(
                        "Select Content:",
                        options=[""] + actual_contents,
                        key=f"content_preview_{tab_key}",
                        help="Select from available content previews"
                    )
                    if selected_content:
                        self.content_preview_filter = selected_content
                        # Store the full content for filtering
                        self.actual_content_filter = selected_content
                else:
                    st.info("No content available for filtering")
        
        # ============= SORTING (Always visible) =============
        st.markdown("---")
        st.markdown("**📊 Sort Results**")
        
        self.sort_by_filter = st.selectbox(
            "Sort By:",
            ["Name (A-Z)", "Name (Z-A)", "Date (Newest)", "Date (Oldest)", 
             "Execution Date (Newest)", "Execution Date (Oldest)"],
            key=f"workflow_sort_by_{tab_key}"
        )
    
    def _get_available_content_preview(self) -> List[str]:
        """Get sample content previews for filtering dropdown."""
        try:
            metadata_collection = get_mongo_collection("workflow_metadata")
            if metadata_collection:
                # Get unique content previews (limited to prevent overwhelming dropdown)
                pipeline = [
                    {
                        "$match": {
                            "workflow_type": self.workflow_type.lower(),
                            "actual_content": {"$exists": True, "$ne": None, "$ne": ""}
                        }
                    },
                    {
                        "$project": {
                            "content_preview": {
                                "$substr": ["$actual_content", 0, 100]
                            }
                        }
                    },
                    {"$group": {"_id": "$content_preview"}},
                    {"$limit": 50},
                    {"$sort": {"_id": 1}}
                ]
                results = list(metadata_collection.aggregate(pipeline))
                previews = [r["_id"] + "..." if len(r["_id"]) == 100 else r["_id"] 
                           for r in results if r.get("_id")]
                return previews
            return []
        except Exception as e:
            st.error(f"Error loading content previews: {e}")
            return []
    
    def _get_available_accounts(self) -> List[str]:
        """Get list of available accounts from database."""
        try:
            metadata_collection = get_mongo_collection("workflow_metadata")
            if metadata_collection:
                # Get unique account usernames
                accounts = metadata_collection.distinct("username")
                # Filter out None values and return sorted list
                return sorted([acc for acc in accounts if acc])
            return []
        except Exception as e:
            st.error(f"Error loading accounts: {e}")
            return []
    
    def build_filters(self) -> Dict[str, Any]:
        """Build comprehensive filters dictionary with hierarchical options."""
        # Initialize with all expected keys
        filters = {
            "workflow_type": self.workflow_type.lower(),
            "has_link": "All",
            "has_content": "All",
            "user_id": "",
            "content_id": "",
            "execution_status": "All",
            "executed": "All",
            "success": "All",
            "sort_by": "Name (A-Z)",
            "workflow_name": "",
        }
        
        # Metadata filters (for workflow_metadata collection queries)
        metadata_filters = {}
        
        # ============= HAS LINK FILTERS =============
        if self.has_link_filter:
            filters["has_link"] = "Yes"
            metadata_filters["has_link"] = True
            
            # Date filter for links
            if self.link_date_filter:
                if self.link_date_filter["type"] == "specific":
                    date = self.link_date_filter["date"]
                    start_datetime = datetime.combine(date, datetime.min.time())
                    end_datetime = datetime.combine(date, datetime.max.time())
                    metadata_filters["linked_at"] = {
                        "$gte": start_datetime.isoformat(),
                        "$lte": end_datetime.isoformat()
                    }
                elif self.link_date_filter["type"] == "range":
                    start_datetime = datetime.combine(self.link_date_filter["start"], datetime.min.time())
                    end_datetime = datetime.combine(self.link_date_filter["end"], datetime.max.time())
                    metadata_filters["linked_at"] = {
                        "$gte": start_datetime.isoformat(),
                        "$lte": end_datetime.isoformat()
                    }
                elif self.link_date_filter["type"] in ["last_7_days", "last_30_days"]:
                    metadata_filters["linked_at"] = {
                        "$gte": self.link_date_filter["date"].isoformat()
                    }
            
            # Specific link filter
            if self.specific_link_filter:
                metadata_filters["link_url"] = {
                    "$regex": self.specific_link_filter,
                    "$options": "i"
                }
        
        # ============= HAS CONTENT FILTERS =============
        if self.has_content_filter:
            filters["has_content"] = "Yes"
            metadata_filters["has_content"] = True
            
            # Content type filter
            if self.content_type_filter:
                # Convert to lowercase for MongoDB query
                content_types_lower = [ct.lower() for ct in self.content_type_filter]
                metadata_filters["workflow_type"] = {"$in": content_types_lower}
        
        # ============= EXECUTED FILTERS =============
        if self.executed_filter:
            filters["executed"] = "Yes"
            metadata_filters["executed"] = True
            
            # Execution result filter
            if self.execution_result_filter:
                if self.execution_result_filter == "Successful Only":
                    filters["success"] = "Successful"
                    metadata_filters["success"] = True
                elif self.execution_result_filter == "Failed Only":
                    filters["success"] = "Failed"
                    metadata_filters["success"] = False
                # "All Executed" doesn't add additional filters
        
        # ============= ACCOUNT FILTER =============
        if self.account_filter:
            metadata_filters["username"] = {"$in": self.account_filter}
        
        # ============= NEW: CONTENT FILTERS =============
        if self.content_id_filter:
            filters["content_id"] = str(int(self.content_id_filter))
            metadata_filters["postgres_content_id"] = int(self.content_id_filter)
        
        if hasattr(self, 'actual_content_filter') and self.actual_content_filter:
            # Use regex for partial matching
            metadata_filters["actual_content"] = {
                "$regex": self.actual_content_filter.replace("...", ""),
                "$options": "i"
            }
        
        # ============= WORKFLOW NAME FILTER =============
        if self.workflow_name_filter:
            filters["workflow_name"] = self.workflow_name_filter
            metadata_filters["workflow_name"] = {
                "$regex": self.workflow_name_filter,
                "$options": "i"
            }
        
        # ============= SORTING =============
        if self.sort_by_filter:
            filters["sort_by"] = self.sort_by_filter
        
        # Store metadata filters for MongoDB queries
        if metadata_filters:
            filters["_metadata_filters"] = metadata_filters
        
        return filters