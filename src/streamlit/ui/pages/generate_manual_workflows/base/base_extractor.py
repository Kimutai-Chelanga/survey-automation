# src/streamlit/ui/pages/generate_manual_workflows/extraction/base_extractor.py
"""
Base Extractor Class - All survey site extractors must inherit from this
"""

import logging
import json
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class BaseExtractor(ABC):
    """Base class for all survey site extractors"""
    
    def __init__(self, db_manager=None):
        self.db_manager = db_manager
        self.site_info = {
            'site_name': 'base',
            'description': 'Base extractor',
            'version': '1.0.0',
            'requires_login': True,
            'requires_cookies': True
        }
        
    @abstractmethod
    def get_site_info(self) -> Dict[str, Any]:
        """Return information about this survey site"""
        return self.site_info
    
    @abstractmethod
    def extract_questions(self, account_id: int, site_id: int, 
                          url: str, profile_path: str, **kwargs) -> Dict[str, Any]:
        """
        Extract questions from the survey site
        
        Args:
            account_id: Account ID in database
            site_id: Site ID in database
            url: URL to extract from
            profile_path: Path to Chrome profile for this account
            **kwargs: Additional parameters
            
        Returns:
            Dict with at least:
                - success: bool
                - questions: list of extracted questions
                - questions_found: int
                - error: str (if success=False)
        """
        pass
    
    def save_questions_to_db(self, account_id: int, site_id: int, 
                              questions: List[Dict], batch_id: str) -> int:
        """Save extracted questions to database"""
        from src.core.database.postgres.connection import get_postgres_connection
        from psycopg2.extras import RealDictCursor
        
        inserted = 0
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    for q in questions:
                        cursor.execute("""
                            INSERT INTO questions (
                                survey_site_id, account_id, question_text,
                                question_type, question_category, options,
                                click_element, input_element, submit_element,
                                required, order_index, page_url, element_html,
                                extracted_at, extraction_batch_id, metadata
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (survey_site_id, question_text, account_id) 
                            DO UPDATE SET
                                last_seen_at = EXCLUDED.extracted_at,
                                click_element = EXCLUDED.click_element,
                                input_element = EXCLUDED.input_element,
                                submit_element = EXCLUDED.submit_element
                            RETURNING question_id
                        """, (
                            site_id, account_id, q.get('question_text'),
                            q.get('question_type', 'unknown'),
                            q.get('question_category', 'general'),
                            json.dumps(q.get('options')) if q.get('options') else None,
                            q.get('click_element'),
                            q.get('input_element'),
                            q.get('submit_element'),
                            q.get('required', True),
                            q.get('order_index', 0),
                            q.get('page_url'),
                            q.get('element_html'),
                            datetime.now(),
                            batch_id,
                            json.dumps(q.get('metadata', {})) if q.get('metadata') else None
                        ))
                        inserted += cursor.rowcount
                        
                    conn.commit()
        except Exception as e:
            logger.error(f"Error saving questions to DB: {e}")
            
        return inserted
    
    def log_extraction(self, account_id: int, site_id: int, batch_id: str,
                       questions_found: int, status: str = 'success', 
                       error_msg: str = None) -> None:
        """Log extraction to workflow_generation_log"""
        from src.core.database.postgres.connection import get_postgres_connection
        
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO workflow_generation_log (
                            workflow_type, workflow_name, account_id, site_id,
                            generated_time, status, questions_processed,
                            error_message, metadata
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        'extraction',
                        f'extract_{self.site_info["site_name"]}',
                        account_id,
                        site_id,
                        datetime.now(),
                        status,
                        questions_found,
                        error_msg,
                        json.dumps({'batch_id': batch_id})
                    ))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error logging extraction: {e}")