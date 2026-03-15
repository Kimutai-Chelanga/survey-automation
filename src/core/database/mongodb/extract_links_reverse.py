"""
MongoDB reversal operations for Extract Links DAG.
DUMMY MODULE: Extract Links DAG does not write to MongoDB.
This module exists only to satisfy the interface requirements.

Location: src/core/database/mongodb/extract_links_reverse.py
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
    DUMMY FUNCTION: Extract Links DAG does not write to MongoDB.
    
    The Extract Links DAG:
    - Reads MongoDB: Gets extraction settings and filter words
    - Writes to PostgreSQL: Inserts extracted links into links table
    - No MongoDB reversal needed
    
    This function exists only to satisfy the interface requirements.
    """
    logger.info("Extract Links DAG does not write to MongoDB - no reversal needed")
    
    return {
        'success': True,
        'total_workflows_to_reverse': 0,
        'breakdown_by_type': {},
        'sample_workflows': [],
        'screenshots_to_delete': 0,
        'videos_to_delete': 0,
        'message': 'Extract Links DAG does not write to MongoDB - no operations to reverse'
    }


def reverse_workflow_execution_mongodb(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    dag_run_id: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    DUMMY FUNCTION: Extract Links DAG does not write to MongoDB.
    
    The Extract Links DAG only:
    1. Reads extraction settings from MongoDB (no write)
    2. Reads filter words from MongoDB (no write)
    3. Writes extracted links to PostgreSQL links table
    
    Therefore, there is nothing to reverse in MongoDB.
    
    This function exists only to satisfy the interface requirements.
    """
    logger.info("Extract Links DAG does not write to MongoDB - no reversal performed")
    
    return {
        'success': True,
        'total_reversed': 0,
        'workflows_reset': 0,
        'details': {
            'screenshots_deleted': 0,
            'video_recordings_deleted': 0,
            'workflow_executions_deleted': 0
        },
        'message': 'Extract Links DAG does not write to MongoDB - no operations to reverse'
    }