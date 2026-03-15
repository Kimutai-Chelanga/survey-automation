# File: utils/workflow_utils.py
import streamlit as st
from src.core.database.postgres import workflow_limits

class WorkflowUtils:
    """Utility functions for workflow operations."""
    
    WORKFLOW_TYPES = [
        'replies_daily', 'replies_hourly', 
        'messages_daily', 'messages_hourly', 
        'retweets_daily', 'retweets_hourly'
    ]
    
    @classmethod
    def get_latest_workflow_status(cls):
        """Fetches the latest status for all workflow types from PostgreSQL."""
        status = {}
        for workflow_type in cls.WORKFLOW_TYPES:
            limit = workflow_limits.get_workflow_limit(workflow_type)
            if limit:
                status[workflow_type] = {
                    'current': limit['current_count'],
                    'max': limit['max_count'],
                    'reset_time': limit['reset_time']
                }
        return status