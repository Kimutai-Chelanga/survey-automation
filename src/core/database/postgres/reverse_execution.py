import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from .connection import get_postgres_connection

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def reverse_workflow_execution_postgres(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    dag_run_id: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Reverse all PostgreSQL operations performed by local_executor DAG.
    This undoes everything that markAsExecuted() does in the JavaScript orchestrator.
    """
    try:
        logger.info("=" * 80)
        logger.info("STARTING POSTGRESQL WORKFLOW EXECUTION REVERSAL")
        logger.info("=" * 80)
        
        reversed_counts = {
            "links_reset": 0,
            "workflow_status_reset": 0,
            "execution_flags_cleared": 0,
            "timestamps_cleared": 0
        }
        
        # Build filter conditions
        filter_conditions = ["1=1"]
        filter_params = []
        
        if workflow_type:
            filter_conditions.append("l.workflow_type = %s")
            filter_params.append(workflow_type)
            logger.info(f"Filter: workflow_type = {workflow_type}")
        
        if account_id:
            filter_conditions.append("l.account_id = %s")
            filter_params.append(account_id)
            logger.info(f"Filter: account_id = {account_id}")
        
        if date_from:
            filter_conditions.append("l.tweeted_date >= %s")
            filter_params.append(date_from.date())
            logger.info(f"Filter: date_from = {date_from.date()}")
        
        if date_to:
            filter_conditions.append("l.tweeted_date <= %s")
            filter_params.append(date_to.date())
            logger.info(f"Filter: date_to = {date_to.date()}")
        
        # Additional filter: only reverse executed workflows
        filter_conditions.append("l.executed = TRUE")
        
        where_clause = " AND ".join(filter_conditions)
        
        with get_postgres_connection() as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Start transaction
                cursor.execute("BEGIN")
                
                try:
                    # Step 1: Get count of links to be reversed
                    logger.info("\nStep 1: Counting links to reverse...")
                    count_query = f"""
                        SELECT COUNT(*) as count
                        FROM links l
                        WHERE {where_clause}
                    """
                    cursor.execute(count_query, filter_params)
                    count_result = cursor.fetchone()
                    total_to_reverse = count_result['count'] if count_result else 0
                    logger.info(f"Found {total_to_reverse} links to reverse")
                    
                    if total_to_reverse == 0:
                        logger.info("No links found matching criteria - nothing to reverse")
                        cursor.execute("ROLLBACK")
                        return {
                            "success": True,
                            "message": "No links found matching criteria",
                            "total_reversed": 0,
                            "details": reversed_counts,
                            "filters_applied": {
                                "workflow_type": workflow_type,
                                "account_id": account_id,
                                "dag_run_id": dag_run_id,
                                "date_from": date_from.isoformat() if date_from else None,
                                "date_to": date_to.isoformat() if date_to else None
                            }
                        }
                    
                    # Step 2: Reset links table - FIXED: Removed reversed_at and reversal_reason
                    logger.info("\nStep 2: Resetting links table...")
                    update_query = f"""
                        UPDATE links l
                        SET 
                            executed = FALSE,
                            workflow_status = 'completed',
                            workflow_processed_time = NULL
                        WHERE {where_clause}
                    """
                    cursor.execute(update_query, filter_params)
                    links_reset = cursor.rowcount
                    reversed_counts["links_reset"] = links_reset
                    logger.info(f"✓ Reset {links_reset} links (executed → FALSE)")
                    
                    # Step 3: Get detailed statistics
                    logger.info("\nStep 3: Gathering reversal statistics...")
                    
                    # Count by workflow_type
                    type_query = f"""
                        SELECT workflow_type, COUNT(*) as count
                        FROM links l
                        WHERE {where_clause.replace('l.executed = TRUE', '1=1')}
                        GROUP BY workflow_type
                    """
                    cursor.execute(type_query, filter_params)
                    type_counts = cursor.fetchall()
                    
                    type_breakdown = {row['workflow_type']: row['count'] for row in type_counts}
                    logger.info(f"Type breakdown: {type_breakdown}")
                    
                    # Commit transaction
                    cursor.execute("COMMIT")
                    logger.info("\n✓ Transaction committed successfully")
                    
                    reversed_counts["workflow_status_reset"] = links_reset
                    reversed_counts["execution_flags_cleared"] = links_reset
                    reversed_counts["timestamps_cleared"] = links_reset
                    
                    total_reversed = links_reset
                    
                    summary = {
                        "success": True,
                        "total_reversed": total_reversed,
                        "details": reversed_counts,
                        "breakdown_by_type": type_breakdown,
                        "filters_applied": {
                            "workflow_type": workflow_type,
                            "account_id": account_id,
                            "dag_run_id": dag_run_id,
                            "date_from": date_from.isoformat() if date_from else None,
                            "date_to": date_to.isoformat() if date_to else None
                        },
                        "reversed_at": datetime.now().isoformat(),
                        "operations_reversed": [
                            "executed flag reset to FALSE",
                            "workflow_status maintained as 'completed'",
                            "workflow_processed_time cleared",
                            "Workflows can now be executed again"
                        ],
                        "database": "PostgreSQL",
                        "note": "Links are now eligible for re-execution by local_executor DAG"
                    }
                    
                    logger.info("=" * 80)
                    logger.info("POSTGRESQL REVERSAL COMPLETED SUCCESSFULLY")
                    logger.info("=" * 80)
                    logger.info(f"Total reversed: {total_reversed} links")
                    
                    return summary
                    
                except Exception as inner_error:
                    cursor.execute("ROLLBACK")
                    logger.error(f"Transaction rolled back due to error: {inner_error}")
                    raise inner_error
                    
    except Exception as e:
        error_msg = f"Error reversing PostgreSQL workflow executions: {e}"
        logger.error(error_msg)
        
        return {
            "success": False,
            "error": error_msg,
            "total_reversed": 0,
            "details": reversed_counts,
            "filters_applied": {
                "workflow_type": workflow_type,
                "account_id": account_id,
                "dag_run_id": dag_run_id,
                "date_from": date_from.isoformat() if date_from else None,
                "date_to": date_to.isoformat() if date_to else None
            }
        }


def get_reversal_preview(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Preview what would be reversed without actually reversing.
    
    Returns:
        dict: Preview of what would be reversed
    """
    try:
        filter_conditions = ["l.executed = TRUE"]
        filter_params = []
        
        if workflow_type:
            filter_conditions.append("l.workflow_type = %s")
            filter_params.append(workflow_type)
        
        if account_id:
            filter_conditions.append("l.account_id = %s")
            filter_params.append(account_id)
        
        if date_from:
            filter_conditions.append("l.tweeted_date >= %s")
            filter_params.append(date_from.date())
        
        if date_to:
            filter_conditions.append("l.tweeted_date <= %s")
            filter_params.append(date_to.date())
        
        where_clause = " AND ".join(filter_conditions)
        
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Count total
                count_query = f"""
                    SELECT COUNT(*) as count
                    FROM links l
                    WHERE {where_clause}
                """
                cursor.execute(count_query, filter_params)
                count_result = cursor.fetchone()
                total_count = count_result['count'] if count_result else 0
                
                # Breakdown by workflow_type
                type_query = f"""
                    SELECT workflow_type, COUNT(*) as count
                    FROM links l
                    WHERE {where_clause}
                    GROUP BY workflow_type
                """
                cursor.execute(type_query, filter_params)
                type_breakdown = {row['workflow_type']: row['count'] for row in cursor.fetchall()}
                
                # Breakdown by workflow_status
                status_query = f"""
                    SELECT workflow_status, COUNT(*) as count
                    FROM links l
                    WHERE {where_clause}
                    GROUP BY workflow_status
                """
                cursor.execute(status_query, filter_params)
                status_breakdown = {row['workflow_status']: row['count'] for row in cursor.fetchall()}
                
                # Sample links
                sample_query = f"""
                    SELECT 
                        links_id,
                        link,
                        workflow_type,
                        workflow_status,
                        tweeted_date,
                        workflow_processed_time
                    FROM links l
                    WHERE {where_clause}
                    ORDER BY workflow_processed_time DESC
                    LIMIT 10
                """
                cursor.execute(sample_query, filter_params)
                sample_links = cursor.fetchall()
                
                return {
                    "total_to_reverse": total_count,
                    "breakdown_by_type": type_breakdown,
                    "breakdown_by_status": status_breakdown,
                    "sample_links": sample_links,
                    "filters_applied": {
                        "workflow_type": workflow_type,
                        "account_id": account_id,
                        "date_from": date_from.isoformat() if date_from else None,
                        "date_to": date_to.isoformat() if date_to else None
                    }
                }
                
    except Exception as e:
        error_msg = f"Error getting reversal preview: {e}"
        logger.error(error_msg)
        
        return {
            "error": error_msg,
            "total_to_reverse": 0
        }


def reverse_specific_links(link_ids: List[int]) -> Dict[str, Any]:
    """
    Reverse execution for specific link IDs.
    
    Args:
        link_ids: List of link IDs to reverse
    
    Returns:
        dict: Summary of what was reversed
    """
    try:
        if not link_ids:
            return {
                "success": False,
                "error": "No link IDs provided",
                "total_reversed": 0
            }
        
        logger.info(f"Reversing execution for {len(link_ids)} specific links")
        
        with get_postgres_connection() as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("BEGIN")
                
                try:
                    # Convert link_ids to tuple for SQL IN clause
                    link_ids_tuple = tuple(link_ids)
                    
                    # Update links
                    update_query = """
                        UPDATE links
                        SET 
                            executed = FALSE,
                            workflow_processed_time = NULL,
                            reversed_at = CURRENT_TIMESTAMP,
                            reversal_reason = 'reverse_specific_links'
                        WHERE links_id IN %s
                        AND executed = TRUE
                    """
                    cursor.execute(update_query, (link_ids_tuple,))
                    links_reset = cursor.rowcount
                    
                    cursor.execute("COMMIT")
                    
                    logger.info(f"✓ Successfully reversed {links_reset} links")
                    
                    return {
                        "success": True,
                        "total_reversed": links_reset,
                        "link_ids_processed": link_ids,
                        "reversed_at": datetime.now().isoformat()
                    }
                    
                except Exception as inner_error:
                    cursor.execute("ROLLBACK")
                    raise inner_error
                    
    except Exception as e:
        error_msg = f"Error reversing specific links: {e}"
        logger.error(error_msg)
        
        return {
            "success": False,
            "error": error_msg,
            "total_reversed": 0
        }


def get_execution_history(
    workflow_type: Optional[str] = None,
    account_id: Optional[int] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Get execution history for review before reversal.
    
    Args:
        workflow_type: Optional filter by workflow type
        account_id: Optional filter by account ID
        limit: Maximum number of records to return
    
    Returns:
        List of executed workflow records
    """
    try:
        filter_conditions = ["executed = TRUE"]
        filter_params = []
        
        if workflow_type:
            filter_conditions.append("workflow_type = %s")
            filter_params.append(workflow_type)
        
        if account_id:
            filter_conditions.append("account_id = %s")
            filter_params.append(account_id)
        
        where_clause = " AND ".join(filter_conditions)
        
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = f"""
                    SELECT 
                        links_id,
                        link,
                        tweet_id,
                        workflow_type,
                        workflow_status,
                        account_id,
                        tweeted_date,
                        tweeted_time,
                        workflow_processed_time,
                        executed
                    FROM links
                    WHERE {where_clause}
                    ORDER BY workflow_processed_time DESC
                    LIMIT %s
                """
                filter_params.append(limit)
                
                cursor.execute(query, filter_params)
                records = cursor.fetchall()
                
                logger.info(f"Retrieved {len(records)} execution history records")
                return records
                
    except Exception as e:
        error_msg = f"Error getting execution history: {e}"
        logger.error(error_msg)
        
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ {error_msg}")
        
        return []