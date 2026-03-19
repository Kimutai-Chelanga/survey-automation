# src/streamlit/ui/pages/generate_manual_workflows/workflow_creators/topsurveys_workflow.py
"""
Workflow Creator for Top Surveys site
"""

import time
import json
import copy
import random
import uuid
from typing import Dict, List, Any, Optional
from datetime import datetime

from ..base.base_workflow_creator import BaseWorkflowCreator

logger = logging.getLogger(__name__)

class TopSurveysWorkflowCreator(BaseWorkflowCreator):
    """Workflow creator for Top Surveys website"""
    
    def __init__(self, db_manager=None):
        super().__init__(db_manager)
        self.site_info = {
            'site_name': 'Top Surveys',
            'description': 'Workflow creator for Top Surveys',
            'version': '1.0.0',
            'template_name': 'topsurveys_template',
            'template_category': 'surveys'
        }
        
    def get_site_info(self) -> Dict[str, Any]:
        return self.site_info
    
    def create_workflows(self, account_id: int, site_id: int,
                         questions: List[Dict], prompt: Optional[Dict],
                         **kwargs) -> Dict[str, Any]:
        """
        Create workflows from questions for Top Surveys
        
        This implements site-specific workflow creation logic
        """
        logger.info(f"Creating workflows for Top Surveys, account {account_id}")
        
        # Get parameters
        workflow_count = kwargs.get('workflow_count', min(3, len(questions)))
        batch_id = f"topsurveys_wf_{account_id}_{int(time.time())}"
        
        # Get template
        template = self.get_template()
        if not template:
            return {
                'success': False,
                'error': 'Failed to load workflow template'
            }
        
        # Create workflows
        workflows = []
        selected_questions = random.sample(questions, min(workflow_count, len(questions)))
        
        for i, question in enumerate(selected_questions):
            workflow = self._create_single_workflow(
                account_id=account_id,
                site_id=site_id,
                question=question,
                prompt=prompt,
                template=template,
                index=i,
                batch_id=batch_id
            )
            workflows.append(workflow)
            
            # Mark question as used
            self._mark_question_used(question['question_id'])
        
        # Save to database
        inserted = self.save_workflows_to_db(
            account_id=account_id,
            site_id=site_id,
            workflows=workflows,
            batch_id=batch_id
        )
        
        # Log creation
        self.log_workflow_creation(
            account_id=account_id,
            site_id=site_id,
            batch_id=batch_id,
            workflows_created=len(workflows)
        )
        
        return {
            'success': True,
            'workflows': workflows,
            'workflows_created': len(workflows),
            'inserted': inserted,
            'batch_id': batch_id
        }
    
    def _create_single_workflow(self, account_id: int, site_id: int,
                                 question: Dict, prompt: Optional[Dict],
                                 template: Dict, index: int,
                                 batch_id: str) -> Dict:
        """Create a single workflow from a question"""
        
        # Clone template
        workflow_data = copy.deepcopy(template.get('workflow_data', template))
        
        # Remove _id fields
        workflow_data = self._deep_remove_id_fields(workflow_data)
        
        # Set workflow name
        workflow_name = f"topsurveys_q{question['question_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        workflow_data['name'] = workflow_name
        
        # Customize workflow based on question type
        question_type = question.get('question_type', 'text')
        
        if question_type in ['multiple_choice', 'dropdown', 'radio']:
            # Add click elements for options
            self._add_click_elements(workflow_data, question)
        elif question_type == 'text':
            # Add input field
            self._add_input_field(workflow_data, question)
        elif question_type == 'rating':
            # Add rating elements
            self._add_rating_elements(workflow_data, question)
        
        # Add click element for the question itself
        if question.get('click_element'):
            self._add_question_click(workflow_data, question['click_element'])
        
        return {
            'workflow_name': workflow_name,
            'workflow_data': workflow_data,
            'question_id': question['question_id'],
            'question_text': question['question_text'],
            'question_type': question['question_type'],
            'click_element': question.get('click_element', ''),
            'batch_id': batch_id,
            'index': index
        }
    
    def _deep_remove_id_fields(self, obj):
        """Recursively remove all _id fields"""
        if isinstance(obj, dict):
            obj.pop('_id', None)
            for value in obj.values():
                self._deep_remove_id_fields(value)
        elif isinstance(obj, list):
            for item in obj:
                self._deep_remove_id_fields(item)
        return obj
    
    def _add_click_elements(self, workflow_data: Dict, question: Dict):
        """Add click elements for multiple choice options"""
        # Find the appropriate node in workflow_data
        if 'drawflow' in workflow_data and 'nodes' in workflow_data['drawflow']:
            for node in workflow_data['drawflow']['nodes']:
                if node.get('label') == 'click-group':
                    # Add click elements for each option
                    options = question.get('options', ['Option A', 'Option B', 'Option C'])
                    # ... implementation depends on workflow structure
    
    def _add_input_field(self, workflow_data: Dict, question: Dict):
        """Add input field for text questions"""
        # Find forms node and set input element
        if 'drawflow' in workflow_data and 'nodes' in workflow_data['drawflow']:
            for node in workflow_data['drawflow']['nodes']:
                if node.get('label') == 'forms' or node.get('data', {}).get('type') == 'text-field':
                    node['data']['input_element'] = question.get('input_element', '')
    
    def _add_rating_elements(self, workflow_data: Dict, question: Dict):
        """Add rating elements"""
        # Implementation for rating questions
        pass
    
    def _add_question_click(self, workflow_data: Dict, click_element: str):
        """Add click element for the question itself"""
        if 'drawflow' in workflow_data and 'nodes' in workflow_data['drawflow']:
            for node in workflow_data['drawflow']['nodes']:
                if node.get('label') == 'click-question':
                    node['data']['selector'] = click_element
    
    def _mark_question_used(self, question_id: int):
        """Mark question as used in database"""
        try:
            from src.core.database.postgres.connection import get_postgres_connection
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE questions
                        SET used_in_workflow = TRUE, used_at = CURRENT_TIMESTAMP
                        WHERE question_id = %s
                    """, (question_id,))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error marking question used: {e}")