# src/streamlit/ui/pages/generate_manual_workflows/base/base_workflow_creator.py
"""
Base Workflow Creator Class - All survey site workflow creators must inherit from this
"""

import logging
import json
import uuid
import time
import copy
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from bson import ObjectId

logger = logging.getLogger(__name__)

class BaseWorkflowCreator(ABC):
    """Base class for all survey site workflow creators"""
    
    def __init__(self, db_manager=None):
        self.db_manager = db_manager
        self.site_info = {
            'site_name': 'base',
            'description': 'Base workflow creator',
            'version': '1.0.0',
            'template_name': 'default_template',
            'template_category': 'default'
        }
        
    @abstractmethod
    def get_site_info(self) -> Dict[str, Any]:
        """Return information about this workflow creator"""
        return self.site_info
    
    @abstractmethod
    def create_workflows(self, account_id: int, site_id: int, 
                         questions: List[Dict], prompt: Optional[Dict],
                         **kwargs) -> Dict[str, Any]:
        """
        Create workflows from questions for this survey site
        
        Args:
            account_id: Account ID in database
            site_id: Site ID in database
            questions: List of questions to create workflows from
            prompt: User prompt for personalization
            **kwargs: Additional site-specific parameters
            
        Returns:
            Dict with at least:
                - success: bool
                - workflows: list of created workflows
                - workflows_created: int
                - error: str (if success=False)
        """
        pass
    
    def get_template(self, template_id: Optional[str] = None) -> Optional[Dict]:
        """
        Get workflow template from database
        Override this for site-specific template loading
        """
        try:
            from core.database.mongodb.connection import get_mongo_collection
            
            if template_id is None:
                # Get default template for this site
                template_id = self._get_default_template_id()
                
            if not template_id:
                logger.error(f"No template ID for site {self.site_info['site_name']}")
                return None
                
            templates_collection = get_mongo_collection("workflow_templates")
            if templates_collection is None:
                logger.error("Failed to get workflow_templates collection")
                return None
                
            if isinstance(template_id, str):
                template_id = ObjectId(template_id)
                
            template = templates_collection.find_one({"_id": template_id})
            if not template:
                logger.error(f"Template not found: {template_id}")
                return None
                
            return template
            
        except Exception as e:
            logger.error(f"Error getting template: {e}")
            return None
    
    def _get_default_template_id(self) -> Optional[str]:
        """Get default template ID for this site"""
        try:
            from core.database.mongodb.connection import get_mongo_collection
            
            templates_collection = get_mongo_collection("workflow_templates")
            if templates_collection is None:
                return None
                
            doc = templates_collection.find_one(
                {
                    'template_name': self.site_info.get('template_name'),
                    'category': self.site_info.get('template_category'),
                    'is_active': True
                },
                {'_id': 1}
            )
            return str(doc['_id']) if doc else None
            
        except Exception as e:
            logger.error(f"Error fetching default template: {e}")
            return None
    
    def save_workflows_to_db(self, account_id: int, site_id: int,
                              workflows: List[Dict], batch_id: str) -> int:
        """Save created workflows to database"""
        from src.core.database.postgres.connection import get_postgres_connection
        from psycopg2.extras import RealDictCursor
        
        inserted = 0
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    for wf in workflows:
                        cursor.execute("""
                            INSERT INTO workflows (
                                account_id, site_id, workflow_name, workflow_data,
                                question_id, created_time, updated_time, is_active,
                                uploaded_to_chrome
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING workflow_id
                        """, (
                            account_id,
                            site_id,
                            wf.get('workflow_name'),
                            json.dumps(wf.get('workflow_data', {})),
                            wf.get('question_id'),
                            datetime.now(),
                            datetime.now(),
                            True,
                            False
                        ))
                        result = cursor.fetchone()
                        if result:
                            wf['workflow_id'] = result[0] if isinstance(result, tuple) else result.get('workflow_id')
                            inserted += 1
                            
                    conn.commit()
        except Exception as e:
            logger.error(f"Error saving workflows to DB: {e}")
            
        return inserted
    
    def log_workflow_creation(self, account_id: int, site_id: int, batch_id: str,
                              workflows_created: int, status: str = 'success',
                              error_msg: str = None) -> None:
        """Log workflow creation to workflow_generation_log"""
        from src.core.database.postgres.connection import get_postgres_connection
        
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO workflow_generation_log (
                            workflow_type, workflow_name, account_id, site_id,
                            generated_time, status, workflows_created,
                            error_message, metadata
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        'workflow_creation',
                        f'create_{self.site_info["site_name"]}',
                        account_id,
                        site_id,
                        datetime.now(),
                        status,
                        workflows_created,
                        error_msg,
                        json.dumps({'batch_id': batch_id})
                    ))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error logging workflow creation: {e}")