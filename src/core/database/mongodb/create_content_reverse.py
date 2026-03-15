"""
MongoDB reversal module for Create Content DAG - NO-OP VERSION
Location: src/core/database/mongodb/create_content_reverse.py

The Create Content DAG only creates PostgreSQL records (replies, messages, retweets).
It does NOT create any MongoDB records. This module exists for interface compatibility.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


def get_mongodb_reversal_preview(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    dag_run_id: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Get MongoDB reversal preview for Create Content DAG.
    
    NOTE: Create Content DAG only creates PostgreSQL records.
    This function returns empty results for interface compatibility.
    """
    logger.info("Create Content DAG does not create MongoDB records - returning empty preview")
    
    return {
        'success': True,
        'total_workflows_to_reverse': 0,
        'breakdown_by_type': {},
        'screenshots_to_delete': 0,
        'videos_to_delete': 0,
        'sample_workflows': [],
        'message': 'Create Content DAG only affects PostgreSQL (replies, messages, retweets tables)'
    }


def reverse_workflow_execution_mongodb(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    dag_run_id: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Reverse Create Content workflow executions in MongoDB.
    
    NOTE: Create Content DAG only creates PostgreSQL records.
    This is a no-op that exists for interface compatibility.
    
    MongoDB records for content are created by downstream DAGs:
    - Create Automa DAG (workflow_metadata, automa_workflows)
    - Filter Links DAG (content_workflow_links)
    - Execute Workflows DAG (workflow_executions, screenshots, videos)
    """
    logger.info("Create Content DAG reversal: No MongoDB operations to reverse")
    logger.info("Content is stored in PostgreSQL only (replies, messages, retweets tables)")
    
    return {
        'success': True,
        'total_reversed': 0,
        'workflows_reset': 0,
        'details': {
            'screenshots_deleted': 0,
            'video_recordings_deleted': 0,
            'message': 'Create Content DAG does not create MongoDB records'
        },
        'message': 'No MongoDB operations to reverse for Create Content DAG (PostgreSQL only)'
    }