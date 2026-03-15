import streamlit as st
import json
import re
from datetime import datetime
from ...settings.settings_manager import get_system_setting

def mask_sensitive_uri(uri):
    """Mask sensitive information in database URIs."""
    if not uri or uri == 'Not configured':
        return uri
    try:
        def mask_password(match):
            password = match.group(1)
            if len(password) <= 2:
                return f":{password}@"
            return f":{password[0]}{'*' * (len(password) - 2)}{password[-1]}@"
        masked = re.sub(r':([^:@]+)@', mask_password, uri)
        return masked
    except:
        return "URI (masked for security)"

def export_settings(settings_manager):
    """Export all current settings to JSON."""
    try:
        settings_export = {
            "export_timestamp": datetime.now().isoformat(),
            "create_content_settings": get_system_setting('create_content_settings', {}),
            "extraction_processing_settings": get_system_setting('extraction_processing_settings', {}),
            "extraction_time_settings": get_system_setting('extraction_time_settings', {}),
            "workflow_strategy_settings": get_system_setting('workflow_strategy_settings', {}),
            "create_content_config": get_system_setting('create_content_config', {}),
            "automa_config": get_system_setting('automa_config', {}),
            "mongodb_uri": "MASKED_FOR_SECURITY",
            "database_url": "MASKED_FOR_SECURITY"
        }
        json_str = json.dumps(settings_export, indent=2)
        st.download_button(
            label="📁 Download Settings JSON",
            data=json_str,
            file_name=f"workflow_settings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            key="download_settings"
        )
        st.success("✅ Settings export ready for download!")
    except Exception as e:
        st.error(f"❌ Error exporting settings: {str(e)}")

def get_link_grouped_workflows_example():
    """Generate example MongoDB query for link-based workflow grouping."""
    example_query = """
    // MongoDB Aggregation Pipeline for Link-Based Workflow Grouping
    
    // 1. Get all workflows with links
    db.getCollection('messages_workflows').aggregate([
    {
        $lookup: {
        from: 'links',
        localField: 'postgres_content_id',
        foreignField: 'content_id', 
        as: 'link_data'
        }
    },
    {
        $unwind: '$link_data'
    },
    {
        $group: {
        _id: '$link_data.link',  // Group by actual link
        workflows: {
            $push: {
            workflow_id: '$_id',
            name: '$name',
            type: 'messages',
            postgres_id: '$postgres_content_id'
            }
        },
        total_workflows: { $sum: 1 }
        }
    },
    {
        $sort: { 'total_workflows': -1 }  // Most workflows first
    }
    ])
    
    // 2. Execute workflows in link groups
    // For each link group, execute workflows in specified order:
    // - messages_first: messages → replies → retweets
    // - replies_first: replies → messages → retweets  
    // - etc.
    """
    return example_query