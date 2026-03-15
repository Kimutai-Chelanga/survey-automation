"""
Filter Links PostgreSQL Reversal Module - Date-based only (NO account filtering)
Location: src/core/database/postgres/filter_links_reverse.py

Resets all filter_links DAG related fields.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from .connection import get_postgres_connection  # ✅ CORRECT: Only PostgreSQL import

logger = logging.getLogger(__name__)


def get_reversal_preview(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Preview what will be reversed for Filter Links operations.
    
    IMPORTANT: Only filters by DATE, ignores account_id
    
    Args:
        workflow_type: IGNORED
        account_id: IGNORED - Filter Links operates on all links
        date_from: Filter by start date (optional)
        date_to: Filter by end date (optional)
    
    Returns:
        Dictionary with preview information
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Build WHERE clause - ONLY DATE FILTERING (optional)
                where_conditions = []
                params = []
                
                # Optional date range filter
                if date_from:
                    where_conditions.append("(filtered_time >= %s OR workflow_processed_time >= %s)")
                    params.extend([date_from, date_from])
                
                if date_to:
                    where_conditions.append("(filtered_time <= %s OR workflow_processed_time <= %s)")
                    params.extend([date_to, date_to])
                
                where_clause = " AND ".join(where_conditions) if where_conditions else "TRUE"
                
                # Get total count of links to reset
                count_query = f"""
                    SELECT COUNT(*) as count
                    FROM links 
                    WHERE {where_clause}
                """
                
                cursor.execute(count_query, params)
                total_to_reverse = cursor.fetchone()['count']
                
                # Get processing state breakdown
                processing_query = f"""
                    SELECT 
                        COUNT(*) as total_links,
                        COUNT(CASE WHEN tweeted_time IS NOT NULL THEN 1 END) as with_timestamp,
                        COUNT(CASE WHEN tweeted_date IS NOT NULL THEN 1 END) as with_date,
                        COUNT(CASE WHEN within_limit = TRUE THEN 1 END) as within_limit,
                        COUNT(CASE WHEN used = TRUE THEN 1 END) as used,
                        COUNT(CASE WHEN filtered = TRUE THEN 1 END) as filtered,
                        COUNT(CASE WHEN processed_by_workflow = TRUE THEN 1 END) as processed,
                        COUNT(CASE WHEN executed = TRUE THEN 1 END) as executed
                    FROM links
                    WHERE {where_clause}
                """
                
                cursor.execute(processing_query, params)
                row = cursor.fetchone()
                processing_breakdown = {
                    'total_links': row['total_links'] or 0,
                    'with_timestamp': row['with_timestamp'] or 0,
                    'with_date': row['with_date'] or 0,
                    'within_limit': row['within_limit'] or 0,
                    'used': row['used'] or 0,
                    'filtered': row['filtered'] or 0,
                    'processed_by_workflow': row['processed'] or 0,
                    'executed': row['executed'] or 0
                }
                
                # Breakdown by workflow_type
                breakdown_query = f"""
                    SELECT 
                        COALESCE(workflow_type, 'unassigned') as type,
                        COUNT(*) as count
                    FROM links
                    WHERE {where_clause}
                    GROUP BY workflow_type
                    ORDER BY count DESC
                """
                
                cursor.execute(breakdown_query, params)
                breakdown_by_type = {row['type']: row['count'] for row in cursor.fetchall()}
                
                # Get sample links
                sample_query = f"""
                    SELECT 
                        links_id,
                        link,
                        tweet_id,
                        tweeted_date,
                        within_limit,
                        filtered,
                        executed,
                        workflow_status
                    FROM links
                    WHERE {where_clause}
                    ORDER BY tweeted_date DESC NULLS LAST
                    LIMIT 10
                """
                
                cursor.execute(sample_query, params)
                sample_links = [dict(row) for row in cursor.fetchall()]
                
                # Format dates in sample
                for link in sample_links:
                    if link.get('tweeted_date'):
                        link['tweeted_date'] = link['tweeted_date'].strftime('%Y-%m-%d')
        
        return {
            'success': True,
            'total_to_reverse': total_to_reverse,
            'breakdown_by_type': breakdown_by_type,
            'processing_breakdown': processing_breakdown,
            'sample_links': sample_links
        }
        
    except Exception as e:
        logger.error(f"Error generating Filter Links preview: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'total_to_reverse': 0
        }


def reverse_workflow_execution_postgres(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    dag_run_id: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Reverse Filter Links workflow execution in PostgreSQL.
    
    IMPORTANT: Only filters by DATE, ignores account_id
    
    Resets all filter_links DAG related fields.
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Build WHERE clause - ONLY DATE FILTERING (optional)
                where_conditions = []
                params = []
                
                if date_from:
                    where_conditions.append("(filtered_time >= %s OR workflow_processed_time >= %s)")
                    params.extend([date_from, date_from])
                
                if date_to:
                    where_conditions.append("(filtered_time <= %s OR workflow_processed_time <= %s)")
                    params.extend([date_to, date_to])
                
                where_clause = " AND ".join(where_conditions) if where_conditions else "TRUE"
                
                # Reset all filter_links DAG related fields
                reset_query = f"""
                    UPDATE links SET 
                        tweeted_time = NULL,
                        tweeted_date = NULL,
                        within_limit = FALSE,
                        used = FALSE,
                        used_time = NULL,
                        filtered = FALSE,
                        filtered_time = NULL,
                        processed_by_workflow = FALSE,
                        workflow_processed_time = NULL,
                        workflow_status = NULL,
                        workflow_type = NULL,
                        workflow_id = NULL,
                        mongo_workflow_id = NULL,
                        executed = FALSE
                    WHERE {where_clause}
                    RETURNING links_id
                """
                
                cursor.execute(reset_query, params)
                links_reset = cursor.rowcount
                
                conn.commit()
        
        logger.info(f"Filter Links reversal completed: {links_reset} links reset")
        
        return {
            'success': True,
            'total_reversed': links_reset,
            'details': {
                'links_reset': links_reset,
                'message': f'Reset {links_reset} links to unprocessed state'
            }
        }
        
    except Exception as e:
        logger.error(f"Error during Filter Links reversal: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'total_reversed': 0
        }