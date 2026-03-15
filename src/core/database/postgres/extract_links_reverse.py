"""
Extract Links PostgreSQL Reversal Module - Date-based only (NO account filtering)
Location: src/core/database/postgres/extract_links_reverse.py

This module handles:
1. Links filtered ONLY by date (no account filtering)
2. Both DELETE and RESET reversal modes
3. Proper database connection handling with context manager
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any
from .connection import get_postgres_connection

logger = logging.getLogger(__name__)


def get_reversal_preview(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    reversal_mode: str = 'delete'
) -> Dict[str, Any]:
    """
    Get preview of what will be reversed for Extract Links DAG.
    
    IMPORTANT: Only filters by DATE, ignores account_id and workflow_type
    
    Args:
        workflow_type: IGNORED - Not used for Extract Links
        account_id: IGNORED - Extract Links operates on all links by date
        date_from: Filter by start date (REQUIRED)
        date_to: Filter by end date (REQUIRED)
        reversal_mode: 'delete' or 'reset'
    
    Returns:
        Dictionary with preview information
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                # Build WHERE clause - ONLY DATE FILTERING
                where_conditions = []
                params = []
                
                # Date range filter - THE ONLY FILTER
                if date_from:
                    where_conditions.append("tweeted_date >= %s")
                    params.append(date_from.date() if hasattr(date_from, 'date') else date_from)
                
                if date_to:
                    where_conditions.append("tweeted_date <= %s")
                    params.append(date_to.date() if hasattr(date_to, 'date') else date_to)
                
                where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
                
                # Get total count
                count_query = f"""
                    SELECT COUNT(*) 
                    FROM links 
                    WHERE {where_clause}
                """
                
                cursor.execute(count_query, params)
                total_to_reverse = cursor.fetchone()[0]
                
                # Get breakdown by date
                breakdown_query = f"""
                    SELECT 
                        tweeted_date::text as date,
                        COUNT(*) as count
                    FROM links
                    WHERE {where_clause}
                    GROUP BY tweeted_date
                    ORDER BY tweeted_date DESC
                    LIMIT 20
                """
                
                cursor.execute(breakdown_query, params)
                breakdown_by_type = {row[0]: row[1] for row in cursor.fetchall()}
                
                # Get processing state breakdown
                processing_query = f"""
                    SELECT
                        COUNT(*) FILTER (WHERE executed = TRUE) as executed,
                        COUNT(*) FILTER (WHERE processed_by_workflow = TRUE) as processed_by_workflow,
                        COUNT(*) FILTER (WHERE used = TRUE) as used,
                        COUNT(*) FILTER (WHERE filtered = TRUE) as filtered,
                        COUNT(*) FILTER (WHERE workflow_processed_time IS NOT NULL) as with_timestamp
                    FROM links
                    WHERE {where_clause}
                """
                
                cursor.execute(processing_query, params)
                processing_row = cursor.fetchone()
                processing_breakdown = {
                    'executed': processing_row[0] or 0,
                    'processed_by_workflow': processing_row[1] or 0,
                    'used': processing_row[2] or 0,
                    'filtered': processing_row[3] or 0,
                    'with_timestamp': processing_row[4] or 0
                }
                
                # Get sample links
                sample_query = f"""
                    SELECT 
                        links_id,
                        link,
                        tweet_id,
                        tweeted_date,
                        executed,
                        used,
                        filtered,
                        workflow_status
                    FROM links
                    WHERE {where_clause}
                    ORDER BY tweeted_date DESC
                    LIMIT 10
                """
                
                cursor.execute(sample_query, params)
                columns = [desc[0] for desc in cursor.description]
                sample_links = [dict(zip(columns, row)) for row in cursor.fetchall()]
                
                # Format dates in sample
                for link in sample_links:
                    if link.get('tweeted_date'):
                        link['tweeted_date'] = link['tweeted_date'].strftime('%Y-%m-%d')
        
        # Determine action description
        if reversal_mode == 'delete':
            action_description = f"Will permanently DELETE {total_to_reverse} links from database (filtered by date)"
        else:
            action_description = f"Will RESET {total_to_reverse} links to unprocessed state (filtered by date)"
        
        return {
            'success': True,
            'total_to_reverse': total_to_reverse,
            'action_description': action_description,
            'breakdown_by_type': breakdown_by_type,
            'processing_breakdown': processing_breakdown,
            'sample_links': sample_links,
            'reversal_mode': reversal_mode
        }
        
    except Exception as e:
        logger.error(f"Error generating reversal preview: {e}", exc_info=True)
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
    date_to: Optional[datetime] = None,
    reversal_mode: str = 'delete'
) -> Dict[str, Any]:
    """
    Reverse Extract Links workflow execution in PostgreSQL.
    
    IMPORTANT: Only filters by DATE, ignores account_id and workflow_type
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                # Build WHERE clause - ONLY DATE FILTERING
                where_conditions = []
                params = []
                
                # Date range filter - THE ONLY FILTER
                if date_from:
                    # Handle NULL dates explicitly
                    where_conditions.append("(tweeted_date >= %s OR tweeted_date IS NULL)")
                    params.append(date_from.date() if hasattr(date_from, 'date') else date_from)
                
                if date_to:
                    # Handle NULL dates explicitly
                    where_conditions.append("(tweeted_date <= %s OR tweeted_date IS NULL)")
                    params.append(date_to.date() if hasattr(date_to, 'date') else date_to)
                
                # If no date filters, select all links
                where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
                
                # Log the query for debugging
                logger.info(f"Reversal query WHERE clause: {where_clause}")
                logger.info(f"Reversal query params: {params}")
                
                # Execute reversal based on mode
                if reversal_mode == 'delete':
                    # DELETE MODE: Permanently remove links
                    delete_query = f"""
                        DELETE FROM links
                        WHERE {where_clause}
                        RETURNING links_id, link, tweeted_date
                    """
                    
                    logger.info(f"Executing DELETE query: {delete_query}")
                    cursor.execute(delete_query, params)
                    deleted_links = cursor.fetchall()
                    links_deleted = len(deleted_links)
                    
                    # Log sample of deleted links
                    if deleted_links:
                        logger.info(f"Sample deleted link: {deleted_links[0]}")
                    
                    conn.commit()
                    
                    result = {
                        'success': True,
                        'total_reversed': links_deleted,
                        'reversal_mode': 'delete',
                        'details': {
                            'links_deleted': links_deleted,
                            'message': f'Permanently deleted {links_deleted} links (filtered by date only)'
                        }
                    }
                    
                else:
                    # RESET MODE: Keep links but reset processing state
                    reset_query = f"""
                        UPDATE links
                        SET 
                            executed = FALSE,
                            used = FALSE,
                            filtered = FALSE,
                            processed_by_workflow = FALSE,
                            workflow_status = 'pending',
                            workflow_id = NULL,
                            mongo_workflow_id = NULL,
                            workflow_processed_time = NULL,
                            used_time = NULL,
                            filtered_time = NULL
                        WHERE {where_clause}
                        RETURNING links_id, link, tweeted_date
                    """
                    
                    logger.info(f"Executing RESET query: {reset_query}")
                    cursor.execute(reset_query, params)
                    reset_links = cursor.fetchall()
                    links_reset = len(reset_links)
                    
                    # Log sample of reset links
                    if reset_links:
                        logger.info(f"Sample reset link: {reset_links[0]}")
                    
                    conn.commit()
                    
                    result = {
                        'success': True,
                        'total_reversed': links_reset,
                        'reversal_mode': 'reset',
                        'details': {
                            'links_reset': links_reset,
                            'message': f'Reset {links_reset} links to unprocessed state (filtered by date only)'
                        }
                    }
        
        logger.info(f"Extract Links reversal completed: {result['total_reversed']} links affected (mode: {reversal_mode})")
        return result
        
    except Exception as e:
        logger.error(f"Error during Extract Links reversal: {e}", exc_info=True)
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return {
            'success': False,
            'error': str(e),
            'total_reversed': 0
        }