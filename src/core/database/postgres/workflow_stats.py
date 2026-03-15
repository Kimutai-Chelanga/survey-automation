"""Enhanced database utilities for workflow linkage and filtering"""
from .connection import get_postgres_connection
import logging

logger = logging.getLogger(__name__)


def get_comprehensive_data_with_workflow_filter(table_name: str, account_id: int = None, 
                                                workflow_linkage: str = "All", 
                                                workflow_id: str = None,
                                                used_only: bool = False,
                                                limit: int = 100) -> list:
    """
    Get comprehensive data with workflow linkage filtering
    
    Args:
        table_name: Table name (replies, messages, retweets)
        account_id: Filter by account ID
        workflow_linkage: "All", "Linked", or "Unlinked"
        workflow_id: Filter by specific generated_workflow_id
        used_only: Show only used content
        limit: Maximum number of records
    
    Returns:
        List of dictionaries with comprehensive content data
    """
    try:
        # Determine ID field
        id_field = f"{table_name.rstrip('s')}_id"
        
        # Build query
        query = f"""
            SELECT 
                c.{id_field},
                c.content,
                c.account_id,
                c.prompt_id,
                c.workflow_id as workflow_template_id,
                c.generated_workflow_id,
                c.mongo_workflow_id,
                c.workflow_name,
                c.workflow_status,
                c.used,
                c.workflow_linked,
                c.processed_by_workflow,
                c.created_time,
                c.used_time,
                c.workflow_processed_time,
                a.username,
                a.profile_id,
                p.name as prompt_name,
                p.prompt_type,
                w.name as workflow_template_name,
                w.workflow_type
            FROM {table_name} c
            LEFT JOIN accounts a ON c.account_id = a.account_id
            LEFT JOIN prompts p ON c.prompt_id = p.prompt_id
            LEFT JOIN workflows w ON c.workflow_id = w.workflow_id
            WHERE 1=1
        """
        
        params = []
        
        # Account filter
        if account_id is not None:
            query += " AND c.account_id = %s"
            params.append(account_id)
        
        # Workflow linkage filter
        if workflow_linkage == "Linked":
            query += " AND c.workflow_linked = TRUE"
        elif workflow_linkage == "Unlinked":
            query += " AND c.workflow_linked = FALSE"
        
        # Specific workflow ID filter
        if workflow_id:
            query += " AND c.generated_workflow_id::text = %s"
            params.append(workflow_id)
        
        # Used filter
        if used_only:
            query += " AND c.used = TRUE"
        else:
            query += " AND c.used = FALSE"
        
        query += f" ORDER BY c.created_time DESC LIMIT {limit}"
        
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                results = []
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    # Convert UUID to string if present
                    if row_dict.get('generated_workflow_id'):
                        row_dict['generated_workflow_id'] = str(row_dict['generated_workflow_id'])
                    results.append(row_dict)
                
                logger.info(f"Retrieved {len(results)} {table_name} records with workflow filter: {workflow_linkage}")
                return results
    
    except Exception as e:
        logger.error(f"Error fetching comprehensive data with workflow filter: {e}")
        return []


def get_workflow_linkage_statistics(table_name: str, account_id: int = None) -> dict:
    """
    Get workflow linkage statistics for a content type
    
    Args:
        table_name: Table name (replies, messages, retweets)
        account_id: Optional account ID filter
    
    Returns:
        Dictionary with linkage statistics
    """
    try:
        query = f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN used THEN 1 ELSE 0 END) as used,
                SUM(CASE WHEN NOT used THEN 1 ELSE 0 END) as unused,
                SUM(CASE WHEN workflow_linked THEN 1 ELSE 0 END) as workflow_linked,
                SUM(CASE WHEN NOT workflow_linked THEN 1 ELSE 0 END) as workflow_unlinked,
                SUM(CASE WHEN processed_by_workflow THEN 1 ELSE 0 END) as processed_by_workflow,
                COUNT(DISTINCT generated_workflow_id) as unique_workflows,
                COUNT(DISTINCT CASE WHEN workflow_linked THEN generated_workflow_id END) as unique_linked_workflows
            FROM {table_name}
        """
        
        params = []
        if account_id is not None:
            query += " WHERE account_id = %s"
            params.append(account_id)
        
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                
                if result:
                    stats = {
                        f'total_{table_name}': result[0] or 0,
                        f'used_{table_name}': result[1] or 0,
                        f'unused_{table_name}': result[2] or 0,
                        f'workflow_linked_{table_name}': result[3] or 0,
                        f'workflow_unlinked_{table_name}': result[4] or 0,
                        f'processed_by_workflow_{table_name}': result[5] or 0,
                        'unique_workflows': result[6] or 0,
                        'unique_linked_workflows': result[7] or 0
                    }
                    
                    # Calculate rates
                    total = stats[f'total_{table_name}']
                    if total > 0:
                        stats['usage_rate'] = round(stats[f'used_{table_name}'] / total * 100, 2)
                        stats['linkage_rate'] = round(stats[f'workflow_linked_{table_name}'] / total * 100, 2)
                    else:
                        stats['usage_rate'] = 0.0
                        stats['linkage_rate'] = 0.0
                    
                    return stats
                
                return {}
    
    except Exception as e:
        logger.error(f"Error getting workflow linkage statistics: {e}")
        return {}


def get_all_workflow_ids(table_name: str, account_id: int = None) -> list:
    """
    Get all unique generated_workflow_ids from a table
    
    Args:
        table_name: Table name (replies, messages, retweets)
        account_id: Optional account ID filter
    
    Returns:
        List of workflow ID strings
    """
    try:
        query = f"""
            SELECT DISTINCT generated_workflow_id
            FROM {table_name}
            WHERE generated_workflow_id IS NOT NULL
        """
        
        params = []
        if account_id is not None:
            query += " AND account_id = %s"
            params.append(account_id)
        
        query += " ORDER BY generated_workflow_id"
        
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                workflow_ids = [str(row[0]) for row in results if row[0]]
                logger.info(f"Found {len(workflow_ids)} unique workflow IDs in {table_name}")
                return workflow_ids
    
    except Exception as e:
        logger.error(f"Error getting workflow IDs: {e}")
        return []


def get_content_by_workflow_id(workflow_id: str) -> dict:
    """
    Get content from all tables by generated_workflow_id
    
    Args:
        workflow_id: The generated_workflow_id to search for
    
    Returns:
        Dictionary with content from each table
    """
    try:
        results = {
            'replies': [],
            'messages': [],
            'retweets': []
        }
        
        for table_name in results.keys():
            id_field = f"{table_name.rstrip('s')}_id"
            
            query = f"""
                SELECT 
                    c.{id_field},
                    c.content,
                    c.account_id,
                    c.prompt_id,
                    c.workflow_id as workflow_template_id,
                    c.generated_workflow_id,
                    c.mongo_workflow_id,
                    c.workflow_name,
                    c.workflow_status,
                    c.used,
                    c.workflow_linked,
                    c.created_time,
                    a.username,
                    p.name as prompt_name
                FROM {table_name} c
                LEFT JOIN accounts a ON c.account_id = a.account_id
                LEFT JOIN prompts p ON c.prompt_id = p.prompt_id
                WHERE c.generated_workflow_id::text = %s
            """
            
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, [workflow_id])
                    
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        if row_dict.get('generated_workflow_id'):
                            row_dict['generated_workflow_id'] = str(row_dict['generated_workflow_id'])
                        results[table_name].append(row_dict)
        
        total_found = sum(len(v) for v in results.values())
        logger.info(f"Found {total_found} content items for workflow_id {workflow_id}")
        return results
    
    except Exception as e:
        logger.error(f"Error getting content by workflow ID: {e}")
        return {'replies': [], 'messages': [], 'retweets': []}


def update_workflow_linkage_status(table_name: str, content_id: int, 
                                   workflow_linked: bool = True) -> bool:
    """
    Update the workflow_linked status for a content item
    
    Args:
        table_name: Table name (replies, messages, retweets)
        content_id: Content ID
        workflow_linked: New workflow_linked status
    
    Returns:
        True if successful, False otherwise
    """
    try:
        id_field = f"{table_name.rstrip('s')}_id"
        
        query = f"""
            UPDATE {table_name}
            SET workflow_linked = %s,
                workflow_processed_time = CURRENT_TIMESTAMP
            WHERE {id_field} = %s
        """
        
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, [workflow_linked, content_id])
                rows_affected = cursor.rowcount
                conn.commit()
                
                if rows_affected > 0:
                    logger.info(f"Updated workflow_linked={workflow_linked} for {table_name} {content_id}")
                    return True
                else:
                    logger.warning(f"No rows updated for {table_name} {content_id}")
                    return False
    
    except Exception as e:
        logger.error(f"Error updating workflow linkage status: {e}")
        return False


def bulk_update_workflow_linkage(table_name: str, content_ids: list, 
                                 workflow_linked: bool = True) -> int:
    """
    Bulk update workflow_linked status for multiple content items
    
    Args:
        table_name: Table name (replies, messages, retweets)
        content_ids: List of content IDs
        workflow_linked: New workflow_linked status
    
    Returns:
        Number of rows updated
    """
    try:
        if not content_ids:
            return 0
        
        id_field = f"{table_name.rstrip('s')}_id"
        
        query = f"""
            UPDATE {table_name}
            SET workflow_linked = %s,
                workflow_processed_time = CURRENT_TIMESTAMP
            WHERE {id_field} = ANY(%s)
        """
        
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, [workflow_linked, content_ids])
                rows_affected = cursor.rowcount
                conn.commit()
                
                logger.info(f"Bulk updated {rows_affected} {table_name} records with workflow_linked={workflow_linked}")
                return rows_affected
    
    except Exception as e:
        logger.error(f"Error in bulk update workflow linkage: {e}")
        return 0


def get_unlinked_content_summary(account_id: int = None) -> dict:
    """
    Get summary of unlinked content across all tables
    
    Args:
        account_id: Optional account ID filter
    
    Returns:
        Dictionary with summary statistics
    """
    try:
        summary = {}
        
        for table_name in ['replies', 'messages', 'retweets']:
            query = f"""
                SELECT 
                    COUNT(*) as total_unlinked,
                    COUNT(CASE WHEN used = FALSE THEN 1 END) as unused_unlinked,
                    MIN(created_time) as oldest_unlinked,
                    MAX(created_time) as newest_unlinked
                FROM {table_name}
                WHERE workflow_linked = FALSE
            """
            
            params = []
            if account_id is not None:
                query += " AND account_id = %s"
                params.append(account_id)
            
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    
                    if result:
                        summary[table_name] = {
                            'total_unlinked': result[0] or 0,
                            'unused_unlinked': result[1] or 0,
                            'oldest_unlinked': result[2],
                            'newest_unlinked': result[3]
                        }
        
        return summary
    
    except Exception as e:
        logger.error(f"Error getting unlinked content summary: {e}")
        return {}
