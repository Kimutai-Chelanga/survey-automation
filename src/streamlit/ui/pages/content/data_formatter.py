import pandas as pd

def format_content_dataframe(content_data, content_type: str):
    """Format content data for display, handling missing columns including new relationship fields."""
    df_content = pd.DataFrame(content_data)
    
    if df_content.empty:
        return df_content
    
    # Format datetime columns
    datetime_columns = ['created_time', 'used_time', 'workflow_processed_time']
    for col in datetime_columns:
        if col in df_content.columns:
            df_content[col] = pd.to_datetime(df_content[col], errors='coerce')
            df_content[col] = df_content[col].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None
            )
    
    # Define column rename mapping for all possible columns
    content_type_singular = content_type.rstrip('s')
    if content_type_singular == 'message':
        id_column = 'messages_id'
    elif content_type_singular == 'reply' or content_type_singular == 'replie':
        id_column = 'replies_id'  
    elif content_type_singular == 'retweet':
        id_column = 'retweets_id'
    else:
        id_column = f'{content_type_singular}s_id'
    
    # Enhanced rename mapping including relationship fields
    rename_map = {
        # Basic content fields
        id_column: 'ID',
        'content': 'Content',
        'used': 'Used',
        'used_time': 'Used Time',
        'created_time': 'Created Time',
        
        # Legacy fields (maintain backwards compatibility)
        'mongo_object_id': 'MongoDB Object ID',
        'user_id': 'User ID',  # Legacy field
        
        # New relationship fields
        'account_id': 'Account ID',
        'prompt_id': 'Prompt ID',
        'workflow_id': 'Workflow ID',
        
        # Workflow tracking fields
        'workflow_status': 'Workflow Status',
        'processed_by_workflow': 'Processed by Workflow',
        'workflow_processed_time': 'Workflow Processed Time',
        'mongo_workflow_id': 'MongoDB Workflow ID',
        
        # Relationship data (from JOINs)
        'username': 'Account Username',
        'profile_id': 'Profile ID',
        'prompt_name': 'Prompt Name',
        'prompt_type': 'Prompt Type',
        'prompt_content': 'Prompt Content',
        'workflow_name': 'Workflow Name',
        'workflow_type': 'Workflow Type',
    }
    
    # Only rename columns that exist in the dataframe
    rename_map = {k: v for k, v in rename_map.items() if k in df_content.columns}
    df_content = df_content.rename(columns=rename_map)
    
    # Reorder columns for better display (prioritize most important columns first)
    preferred_order = [
        'ID', 'Content', 'Used', 'Account Username', 'Prompt Name', 'Prompt Type',
        'Workflow Name', 'Workflow Status', 'Created Time', 'Used Time',
        'Workflow Processed Time', 'Account ID', 'Prompt ID', 'Workflow ID'
    ]
    
    # Get columns that exist in the dataframe in preferred order, then add remaining columns
    existing_preferred = [col for col in preferred_order if col in df_content.columns]
    remaining_cols = [col for col in df_content.columns if col not in existing_preferred]
    final_order = existing_preferred + remaining_cols
    
    df_content = df_content[final_order]
    
    return df_content

def format_analytics_dataframe(analytics_data, data_type: str):
    """Format analytics data for display with proper column names and formatting."""
    df_analytics = pd.DataFrame(analytics_data)
    
    if df_analytics.empty:
        return df_analytics
    
    # Format datetime columns
    datetime_columns = ['date', 'created_time', 'last_sync']
    for col in datetime_columns:
        if col in df_analytics.columns:
            df_analytics[col] = pd.to_datetime(df_analytics[col], errors='coerce')
            if col == 'date':
                df_analytics[col] = df_analytics[col].dt.strftime('%Y-%m-%d')
            else:
                df_analytics[col] = df_analytics[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Format percentage columns
    percentage_columns = ['usage_rate', 'effectiveness_rate', 'success_rate', 'overall_effectiveness']
    for col in percentage_columns:
        if col in df_analytics.columns:
            df_analytics[col] = df_analytics[col].apply(
                lambda x: f"{x:.1f}%" if pd.notnull(x) else "0.0%"
            )
    
    # Format numeric columns with appropriate precision
    numeric_columns = ['avg_processing_minutes', 'avg_hours_to_use', 'avg_length']
    for col in numeric_columns:
        if col in df_analytics.columns:
            df_analytics[col] = df_analytics[col].apply(
                lambda x: f"{x:.2f}" if pd.notnull(x) else "0.00"
            )
    
    return df_analytics

def format_account_summary_dataframe(account_data):
    """Format account summary data for dashboard display."""
    df_accounts = pd.DataFrame(account_data)
    
    if df_accounts.empty:
        return df_accounts
    
    # Rename columns for better display
    rename_map = {
        'account_id': 'Account ID',
        'username': 'Username',
        'profile_id': 'Profile ID',
        'total_content': 'Total Content',
        'total_used_content': 'Used Content',
        'prompt_count': 'Prompts',
        'workflow_count': 'Workflows',
        'reply_usage_rate': 'Reply Usage %',
        'message_usage_rate': 'Message Usage %',
        'retweet_usage_rate': 'Retweet Usage %',
        'total_replies': 'Total Replies',
        'total_messages': 'Total Messages',
        'total_retweets': 'Total Retweets',
    }
    
    # Apply renames only for existing columns
    rename_map = {k: v for k, v in rename_map.items() if k in df_accounts.columns}
    df_accounts = df_accounts.rename(columns=rename_map)
    
    # Format percentage columns
    percentage_cols = [col for col in df_accounts.columns if 'Usage %' in col]
    for col in percentage_cols:
        if col in df_accounts.columns:
            df_accounts[col] = df_accounts[col].apply(
                lambda x: f"{x:.1f}%" if pd.notnull(x) else "0.0%"
            )
    
    return df_accounts