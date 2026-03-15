import pandas as pd

def format_workflows_dataframe(workflow_links_data):
    """Format workflow links data for display (from content_workflow_links collection)."""
    df_workflows = pd.DataFrame(workflow_links_data)
    df_workflows['_id'] = df_workflows['_id'].astype(str)
    
    # Format date columns
    date_columns = ['linked_at']
    for col in date_columns:
        if col in df_workflows.columns:
            df_workflows[col] = pd.to_datetime(df_workflows[col], errors='coerce')
            df_workflows[col] = df_workflows[col].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None
            )
    
    # Convert ObjectId fields to strings if present
    if 'automa_workflow_id' in df_workflows.columns:
        df_workflows['automa_workflow_id'] = df_workflows['automa_workflow_id'].astype(str)
    
    # Truncate content preview for display
    if 'content_preview' in df_workflows.columns:
        df_workflows['content_preview'] = df_workflows['content_preview'].apply(
            lambda x: (x[:50] + '...') if isinstance(x, str) and len(x) > 50 else x
        )
    
    # Rename columns for better display
    df_workflows = df_workflows.rename(columns={
        '_id': 'Link ID',
        'postgres_content_id': 'Content ID',
        'postgres_user_id': 'User ID',
        'workflow_name': 'Workflow Name',
        'automa_workflow_id': 'Automa Workflow ID',
        'content_type': 'Content Type',
        'linked_at': 'Linked At',
        'content_preview': 'Content Preview',
        'content_length': 'Content Length',
        'content_hash': 'Content Hash',
        'has_link': 'Has Link'
    })
    
    return df_workflows

def format_executions_dataframe(executions_data):
    """Format executions data for display (from workflow_executions collection)."""
    df_executions = pd.DataFrame(executions_data)
    df_executions['_id'] = df_executions['_id'].astype(str)
    
    # Format date columns
    date_columns = ['generated_at', 'executed_at', 'started_at', 'completed_at']
    for col in date_columns:
        if col in df_executions.columns:
            df_executions[col] = pd.to_datetime(df_executions[col], errors='coerce')
            df_executions[col] = df_executions[col].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None
            )
    
    # Convert ObjectId fields to strings if present
    for col in ['automa_workflow_id', 'content_link_id']:
        if col in df_executions.columns:
            df_executions[col] = df_executions[col].astype(str)
    
    # Format timing columns
    if 'generation_time' in df_executions.columns:
        df_executions['generation_time'] = df_executions['generation_time'].apply(
            lambda x: f"{x:.0f} ms" if pd.notnull(x) and x > 0 else "N/A"
        )
    
    if 'actual_execution_time' in df_executions.columns:
        df_executions['actual_execution_time'] = df_executions['actual_execution_time'].apply(
            lambda x: f"{x:.0f} ms" if pd.notnull(x) and x > 0 else "N/A"
        )
    
    if 'execution_time' in df_executions.columns:
        df_executions['execution_time'] = df_executions['execution_time'].apply(
            lambda x: f"{x:.0f} ms" if pd.notnull(x) and x > 0 else "N/A"
        )
    
    # Truncate error messages for display
    if 'error_message' in df_executions.columns:
        df_executions['error_message'] = df_executions['error_message'].apply(
            lambda x: (x[:100] + '...') if isinstance(x, str) and len(x) > 100 else x
        )
    
    # Rename columns for better display
    df_executions = df_executions.rename(columns={
        '_id': 'Execution ID',
        'automa_workflow_id': 'Automa Workflow ID',
        'content_link_id': 'Content Link ID',
        'postgres_content_id': 'Content ID',
        'postgres_user_id': 'User ID',
        'workflow_type': 'Workflow Type',
        'status': 'Status',
        'executed': 'Executed',
        'success': 'Success',
        'generated_at': 'Generated At',
        'executed_at': 'Executed At',
        'started_at': 'Started At',
        'completed_at': 'Completed At',
        'generation_time': 'Generation Time',
        'execution_time': 'Execution Time (Legacy)',
        'actual_execution_time': 'Actual Execution Time',
        'blocks_generated': 'Blocks Generated',
        'template_used': 'Template Used',
        'error_message': 'Error Message',
        'execution_attempts': 'Execution Attempts'
    })
    
    return df_executions