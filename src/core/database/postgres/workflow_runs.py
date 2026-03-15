import logging
from typing import List, Dict, Any, Optional
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone
from .connection import get_postgres_connection
import streamlit as st

logger = logging.getLogger(__name__)

def save_workflow_run(workflow_name: str, run_id: str, status: str):
    """Saves a workflow run record to the PostgreSQL workflow_runs table."""
    with get_postgres_connection() as conn:
        if not conn:
            logger.error("Failed to connect to PostgreSQL database.")
            st.error("❌ Failed to connect to PostgreSQL database.")
            raise Exception("Failed to connect to PostgreSQL database")
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    '''INSERT INTO workflow_runs (workflow_name, run_id, status, timestamp)
                       VALUES (%s, %s, %s, %s)''',
                    (workflow_name, run_id, status, datetime.now())
                )
                conn.commit()
                logger.info(f"✅ Workflow run saved: {workflow_name} - {run_id}, status: {status}")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error saving workflow run {run_id} for {workflow_name}: {e}")
            st.error(f"❌ Error saving workflow run: {str(e)}")
            raise

def get_workflow_runs(workflow_name: str = None, status: str = None, limit: int = None) -> List[Dict[str, Any]]:
    """Fetches workflow run records from PostgreSQL with optional filters."""
    with get_postgres_connection() as conn:
        if not conn:
            logger.error("Failed to connect to PostgreSQL database.")
            st.error("❌ Failed to connect to PostgreSQL database.")
            return []
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = "SELECT id, workflow_name, run_id, status, timestamp FROM workflow_runs"
                conditions = []
                params = []
                if workflow_name:
                    conditions.append("workflow_name = %s")
                    params.append(workflow_name)
                if status:
                    conditions.append("status = %s")
                    params.append(status.lower())
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                query += " ORDER BY timestamp DESC"
                if limit is not None:
                    query += f" LIMIT {limit}"
                cursor.execute(query, params)
                runs = cursor.fetchall()
                logger.info(f"Retrieved {len(runs)} workflow runs from PostgreSQL.")
                return runs
        except Exception as e:
            logger.error(f"Error fetching workflow runs: {e}")
            st.error(f"❌ Error fetching workflow runs: {str(e)}")
            return []

# NEW: PostgreSQL Reversal Operations

def reverse_postgres_workflow_executions(
    workflow_types: List[str] = None,
    account_ids: List[int] = None,
    time_range_hours: int = None,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Reverse PostgreSQL workflow execution changes by resetting execution status
    and removing execution-related data.
    
    Args:
        workflow_types: List of workflow types to reset (e.g., ['replies', 'messages', 'retweets'])
        account_ids: List of specific account IDs to reset (None for all accounts)
        time_range_hours: Only reset workflows executed within this time range
        dry_run: If True, only count what would be reset without making changes
    
    Returns:
        Dict containing success status and reset details
    """
    with get_postgres_connection() as conn:
        if not conn:
            logger.error("Failed to connect to PostgreSQL database for reversal.")
            return {
                'success': False,
                'error': 'Database connection failed',
                'total_reset': 0
            }
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                reset_details = {
                    'workflow_executions_reset': 0,
                    'content_links_reset': 0,
                    'workflow_runs_removed': 0,
                    'execution_logs_cleared': 0
                }
                
                # Build WHERE conditions
                where_conditions = []
                params = []
                
                # Filter by workflow types if specified
                if workflow_types:
                    workflow_type_placeholders = ','.join(['%s'] * len(workflow_types))
                    where_conditions.append(f"workflow_type IN ({workflow_type_placeholders})")
                    params.extend(workflow_types)
                
                # Filter by account IDs if specified
                if account_ids:
                    account_placeholders = ','.join(['%s'] * len(account_ids))
                    where_conditions.append(f"account_id IN ({account_placeholders})")
                    params.extend(account_ids)
                
                # Filter by time range if specified
                if time_range_hours:
                    time_cutoff = datetime.now(timezone.utc) - datetime.timedelta(hours=time_range_hours)
                    where_conditions.append("executed_at >= %s")
                    params.append(time_cutoff)
                
                # Only target executed workflows
                where_conditions.append("executed = TRUE")
                
                where_clause = " AND ".join(where_conditions) if where_conditions else "executed = TRUE"
                
                # 1. Reset workflow_executions table
                if dry_run:
                    # Count what would be reset
                    cursor.execute(f"""
                        SELECT COUNT(*) as count
                        FROM workflow_executions 
                        WHERE {where_clause}
                    """, params)
                    reset_details['workflow_executions_reset'] = cursor.fetchone()['count']
                else:
                    # Reset execution status and clear execution data
                    cursor.execute(f"""
                        UPDATE workflow_executions 
                        SET 
                            executed = FALSE,
                            executed_at = NULL,
                            execution_success = NULL,
                            execution_error = NULL,
                            execution_time = NULL,
                            steps_taken = NULL,
                            final_result = NULL,
                            session_id = NULL,
                            dag_run_id = NULL,
                            updated_at = %s,
                            -- Reversal metadata
                            postgres_reset_at = %s,
                            postgres_reset_reason = %s,
                            postgres_reset_method = 'reverse_postgres_workflow_executions'
                        WHERE {where_clause}
                    """, params + [
                        datetime.now(timezone.utc),
                        datetime.now(timezone.utc),
                        f"PostgreSQL execution reversal - types: {workflow_types or 'all'}"
                    ])
                    reset_details['workflow_executions_reset'] = cursor.rowcount
                
                # 2. Reset content_links table if it exists
                try:
                    if dry_run:
                        cursor.execute(f"""
                            SELECT COUNT(*) as count
                            FROM content_links cl
                            INNER JOIN workflow_executions we ON cl.workflow_id = we.id
                            WHERE {where_clause.replace('workflow_executions', 'we')}
                        """, params)
                        result = cursor.fetchone()
                        reset_details['content_links_reset'] = result['count'] if result else 0
                    else:
                        cursor.execute(f"""
                            UPDATE content_links 
                            SET 
                                processed = FALSE,
                                processed_at = NULL,
                                processing_error = NULL,
                                link_extracted = FALSE,
                                link_url = NULL,
                                updated_at = %s,
                                -- Reversal metadata
                                postgres_link_reset_at = %s,
                                postgres_link_reset_reason = %s
                            FROM workflow_executions we
                            WHERE content_links.workflow_id = we.id
                            AND {where_clause.replace('workflow_executions', 'we')}
                        """, [
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc),
                            "PostgreSQL link reversal via execution reset"
                        ] + params)
                        reset_details['content_links_reset'] = cursor.rowcount
                except Exception as e:
                    logger.warning(f"Content links table may not exist or error occurred: {e}")
                    reset_details['content_links_reset'] = 0
                
                # 3. Remove workflow_runs entries for reversed executions
                if dry_run:
                    cursor.execute(f"""
                        SELECT COUNT(*) as count
                        FROM workflow_runs wr
                        WHERE wr.status IN ('completed', 'success', 'failed')
                        AND wr.workflow_name = ANY(%s)
                    """, [workflow_types or ['replies', 'messages', 'retweets']])
                    result = cursor.fetchone()
                    reset_details['workflow_runs_removed'] = result['count'] if result else 0
                else:
                    # Mark workflow runs as reversed instead of deleting
                    cursor.execute(f"""
                        UPDATE workflow_runs 
                        SET 
                            status = 'reversed',
                            timestamp = %s,
                            notes = %s
                        WHERE status IN ('completed', 'success', 'failed')
                        AND workflow_name = ANY(%s)
                    """, [
                        datetime.now(timezone.utc),
                        f"Reversed via PostgreSQL reset - original status preserved",
                        workflow_types or ['replies', 'messages', 'retweets']
                    ])
                    reset_details['workflow_runs_removed'] = cursor.rowcount
                
                # 4. Clear execution logs if they exist
                try:
                    if dry_run:
                        cursor.execute(f"""
                            SELECT COUNT(*) as count
                            FROM execution_logs el
                            WHERE el.workflow_type = ANY(%s)
                            AND el.log_level IN ('SUCCESS', 'EXECUTION')
                        """, [workflow_types or ['replies', 'messages', 'retweets']])
                        result = cursor.fetchone()
                        reset_details['execution_logs_cleared'] = result['count'] if result else 0
                    else:
                        cursor.execute(f"""
                            DELETE FROM execution_logs
                            WHERE workflow_type = ANY(%s)
                            AND log_level IN ('SUCCESS', 'EXECUTION')
                            AND created_at >= %s
                        """, [
                            workflow_types or ['replies', 'messages', 'retweets'],
                            datetime.now(timezone.utc) - datetime.timedelta(days=7)  # Only recent logs
                        ])
                        reset_details['execution_logs_cleared'] = cursor.rowcount
                except Exception as e:
                    logger.warning(f"Execution logs table may not exist: {e}")
                    reset_details['execution_logs_cleared'] = 0
                
                # Calculate total reset count
                total_reset = sum([
                    reset_details['workflow_executions_reset'],
                    reset_details['content_links_reset'],
                    reset_details['workflow_runs_removed'],
                    reset_details['execution_logs_cleared']
                ])
                
                if not dry_run:
                    # Record the reversal operation
                    save_workflow_run(
                        workflow_name="system_reversal",
                        run_id=f"postgres_reverse_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        status="completed"
                    )
                    
                    conn.commit()
                    logger.info(f"PostgreSQL workflow reversal completed: {total_reset} records affected")
                else:
                    logger.info(f"PostgreSQL workflow reversal dry run: {total_reset} records would be affected")
                
                return {
                    'success': True,
                    'total_reset': total_reset,
                    'reset_at': datetime.now(timezone.utc).isoformat(),
                    'method': 'reverse_postgres_workflow_executions',
                    'dry_run': dry_run,
                    'details': reset_details,
                    'workflow_types_processed': workflow_types or ['all'],
                    'account_ids_processed': account_ids or ['all'],
                    'time_range_hours': time_range_hours
                }
                
        except Exception as e:
            if not dry_run:
                conn.rollback()
            error_msg = f"PostgreSQL workflow reversal failed: {e}"
            logger.error(error_msg)
            return {
                'success': False,
                'total_reset': 0,
                'error': str(e),
                'method': 'reverse_postgres_workflow_executions'
            }

def reverse_postgres_account_data(
    account_ids: List[int] = None,
    preserve_profiles: bool = True,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Reverse account-specific PostgreSQL changes.
    
    Args:
        account_ids: List of account IDs to reset (None for all)
        preserve_profiles: Whether to keep profile assignments
        dry_run: If True, only count what would be reset
    
    Returns:
        Dict containing success status and reset details
    """
    with get_postgres_connection() as conn:
        if not conn:
            return {'success': False, 'error': 'Database connection failed', 'total_reset': 0}
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                reset_details = {
                    'accounts_reset': 0,
                    'profile_assignments_cleared': 0,
                    'session_data_cleared': 0,
                    'account_stats_reset': 0
                }
                
                # Build account filter
                account_filter = ""
                params = []
                if account_ids:
                    account_placeholders = ','.join(['%s'] * len(account_ids))
                    account_filter = f"WHERE id IN ({account_placeholders})"
                    params = account_ids
                
                # 1. Reset account execution statistics
                if dry_run:
                    cursor.execute(f"""
                        SELECT COUNT(*) as count FROM accounts 
                        {account_filter}
                    """, params)
                    reset_details['accounts_reset'] = cursor.fetchone()['count']
                else:
                    cursor.execute(f"""
                        UPDATE accounts 
                        SET 
                            total_workflows_executed = 0,
                            successful_workflows = 0,
                            failed_workflows = 0,
                            last_execution_date = NULL,
                            execution_success_rate = NULL,
                            total_execution_time = NULL,
                            updated_at = %s,
                            -- Reversal metadata
                            postgres_account_reset_at = %s,
                            postgres_account_reset_reason = %s
                        {account_filter}
                    """, [
                        datetime.now(timezone.utc),
                        datetime.now(timezone.utc),
                        "PostgreSQL account data reversal"
                    ] + params)
                    reset_details['accounts_reset'] = cursor.rowcount
                
                # 2. Clear profile assignments if not preserving
                if not preserve_profiles:
                    try:
                        if dry_run:
                            cursor.execute(f"""
                                SELECT COUNT(*) as count FROM account_profiles 
                                {account_filter.replace('id', 'account_id') if account_filter else ''}
                            """, params)
                            result = cursor.fetchone()
                            reset_details['profile_assignments_cleared'] = result['count'] if result else 0
                        else:
                            cursor.execute(f"""
                                DELETE FROM account_profiles 
                                {account_filter.replace('id', 'account_id') if account_filter else ''}
                            """, params)
                            reset_details['profile_assignments_cleared'] = cursor.rowcount
                    except Exception as e:
                        logger.warning(f"Account profiles table may not exist: {e}")
                
                # 3. Clear session data
                try:
                    if dry_run:
                        cursor.execute(f"""
                            SELECT COUNT(*) as count FROM account_sessions 
                            {account_filter.replace('id', 'account_id') if account_filter else ''}
                        """, params)
                        result = cursor.fetchone()
                        reset_details['session_data_cleared'] = result['count'] if result else 0
                    else:
                        cursor.execute(f"""
                            UPDATE account_sessions 
                            SET 
                                is_active = FALSE,
                                ended_at = %s,
                                session_status = 'reset_via_reversal'
                            {account_filter.replace('id', 'account_id') if account_filter else ''}
                            AND is_active = TRUE
                        """, [datetime.now(timezone.utc)] + params)
                        reset_details['session_data_cleared'] = cursor.rowcount
                except Exception as e:
                    logger.warning(f"Account sessions table may not exist: {e}")
                
                total_reset = sum(reset_details.values())
                
                if not dry_run:
                    conn.commit()
                    logger.info(f"PostgreSQL account reversal completed: {total_reset} records affected")
                
                return {
                    'success': True,
                    'total_reset': total_reset,
                    'reset_at': datetime.now(timezone.utc).isoformat(),
                    'method': 'reverse_postgres_account_data',
                    'dry_run': dry_run,
                    'details': reset_details,
                    'account_ids_processed': account_ids or ['all'],
                    'preserve_profiles': preserve_profiles
                }
                
        except Exception as e:
            if not dry_run:
                conn.rollback()
            logger.error(f"PostgreSQL account reversal failed: {e}")
            return {
                'success': False,
                'total_reset': 0,
                'error': str(e),
                'method': 'reverse_postgres_account_data'
            }

def get_postgres_execution_statistics(
    workflow_types: List[str] = None,
    account_ids: List[int] = None
) -> Dict[str, Any]:
    """
    Get execution statistics from PostgreSQL for reversal planning.
    
    Args:
        workflow_types: Filter by workflow types
        account_ids: Filter by account IDs
    
    Returns:
        Dict containing execution statistics
    """
    with get_postgres_connection() as conn:
        if not conn:
            return {'error': 'Database connection failed'}
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Build WHERE conditions
                where_conditions = []
                params = []
                
                if workflow_types:
                    workflow_type_placeholders = ','.join(['%s'] * len(workflow_types))
                    where_conditions.append(f"workflow_type IN ({workflow_type_placeholders})")
                    params.extend(workflow_types)
                
                if account_ids:
                    account_placeholders = ','.join(['%s'] * len(account_ids))
                    where_conditions.append(f"account_id IN ({account_placeholders})")
                    params.extend(account_ids)
                
                where_clause = ""
                if where_conditions:
                    where_clause = "WHERE " + " AND ".join(where_conditions)
                
                # Get overall statistics
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as total_workflows,
                        COUNT(CASE WHEN executed = TRUE THEN 1 END) as executed_workflows,
                        COUNT(CASE WHEN executed = TRUE AND execution_success = TRUE THEN 1 END) as successful_workflows,
                        COUNT(CASE WHEN executed = TRUE AND execution_success = FALSE THEN 1 END) as failed_workflows,
                        COUNT(CASE WHEN executed = FALSE THEN 1 END) as pending_workflows
                    FROM workflow_executions
                    {where_clause}
                """, params)
                
                overall_stats = cursor.fetchone()
                
                # Get statistics by workflow type
                cursor.execute(f"""
                    SELECT 
                        workflow_type,
                        COUNT(*) as total,
                        COUNT(CASE WHEN executed = TRUE THEN 1 END) as executed,
                        COUNT(CASE WHEN executed = TRUE AND execution_success = TRUE THEN 1 END) as successful,
                        COUNT(CASE WHEN executed = TRUE AND execution_success = FALSE THEN 1 END) as failed
                    FROM workflow_executions
                    {where_clause}
                    GROUP BY workflow_type
                """, params)
                
                type_stats = {row['workflow_type']: dict(row) for row in cursor.fetchall()}
                
                # Get recent execution activity
                cursor.execute(f"""
                    SELECT 
                        DATE(executed_at) as execution_date,
                        COUNT(*) as executions_count
                    FROM workflow_executions
                    WHERE executed = TRUE 
                    AND executed_at >= NOW() - INTERVAL '7 days'
                    {('AND ' + ' AND '.join(where_conditions)) if where_conditions else ''}
                    GROUP BY DATE(executed_at)
                    ORDER BY execution_date DESC
                """, params)
                
                recent_activity = [dict(row) for row in cursor.fetchall()]
                
                return {
                    'success': True,
                    'overall_statistics': dict(overall_stats),
                    'type_statistics': type_stats,
                    'recent_activity': recent_activity,
                    'query_filters': {
                        'workflow_types': workflow_types,
                        'account_ids': account_ids
                    }
                }
                
        except Exception as e:
            logger.error(f"Error getting PostgreSQL execution statistics: {e}")
            return {
                'success': False,
                'error': str(e)
            }

# Integration function for the reset execution method
def reset_execution_with_postgres_reversal(
    workflow_types: List[str] = None,
    include_postgres: bool = True,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Comprehensive reset that includes both MongoDB and PostgreSQL reversal.
    This should be called from your reset execution method.
    
    Args:
        workflow_types: List of workflow types to reset
        include_postgres: Whether to include PostgreSQL reversal
        dry_run: If True, only show what would be reset
    
    Returns:
        Dict containing comprehensive reset results
    """
    results = {
        'success': True,
        'total_reset': 0,
        'mongodb_results': None,
        'postgres_results': None,
        'errors': []
    }
    
    try:
        # 1. PostgreSQL reversal (if enabled)
        if include_postgres:
            logger.info("Starting PostgreSQL workflow reversal...")
            postgres_result = reverse_postgres_workflow_executions(
                workflow_types=workflow_types,
                dry_run=dry_run
            )
            
            results['postgres_results'] = postgres_result
            if postgres_result.get('success'):
                results['total_reset'] += postgres_result.get('total_reset', 0)
                logger.info(f"PostgreSQL reversal completed: {postgres_result.get('total_reset', 0)} records")
            else:
                results['errors'].append(f"PostgreSQL reversal failed: {postgres_result.get('error')}")
        
        # 2. MongoDB reversal (your existing reverse_js_workflow_operations)
        try:
            from src.core.database.mongodb.workflow_executions import reverse_js_workflow_operations
            
            logger.info("Starting MongoDB workflow reversal...")
            mongodb_total = 0
            
            for wf_type in (workflow_types or ['replies', 'messages', 'retweets']):
                mongo_result = reverse_js_workflow_operations(
                    workflow_type=wf_type,
                    account_id=None,
                    dag_run_id=None
                )
                if mongo_result.get('success'):
                    mongodb_total += mongo_result.get('total_reversed', 0)
                else:
                    results['errors'].append(f"MongoDB reversal failed for {wf_type}: {mongo_result.get('error')}")
            
            results['mongodb_results'] = {
                'success': True,
                'total_reset': mongodb_total,
                'method': 'reverse_js_workflow_operations'
            }
            results['total_reset'] += mongodb_total
            
        except ImportError as e:
            results['errors'].append(f"MongoDB reversal not available: {e}")
        except Exception as e:
            results['errors'].append(f"MongoDB reversal error: {e}")
        
        # 3. Record the comprehensive reversal
        if not dry_run and results['total_reset'] > 0:
            save_workflow_run(
                workflow_name="comprehensive_reversal",
                run_id=f"full_reset_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                status="completed"
            )
        
        # Final success determination
        results['success'] = len(results['errors']) == 0 and results['total_reset'] > 0
        
        return results
        
    except Exception as e:
        logger.error(f"Comprehensive reset failed: {e}")
        return {
            'success': False,
            'total_reset': 0,
            'error': str(e),
            'mongodb_results': None,
            'postgres_results': None
        }