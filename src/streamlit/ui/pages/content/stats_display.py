# stats_display.py
import streamlit as st

def display_stats(section_name: str, db_module, account_id=None):
    """Display statistics for the given section, with account context if provided."""
    st.subheader(f"{section_name} Statistics")
    
    try:
        # Try to get account-specific stats if account is selected
        if account_id and hasattr(db_module, f'get_account_{section_name.lower()}_statistics'):
            stats_method = getattr(db_module, f'get_account_{section_name.lower()}_statistics')
            stats = stats_method(account_id)
            
            # Display account-specific stats
            col1, col2, col3, col4 = st.columns(4)
            col1.metric(f"Total {section_name}", stats.get(f'total_{section_name.lower()}', 0))
            col2.metric(f"Used {section_name}", stats.get(f'used_{section_name.lower()}', 0))
            col3.metric(f"Unused {section_name}", stats.get(f'unused_{section_name.lower()}', 0))
            
            # Show usage rate if available
            if 'usage_rate' in stats:
                col4.metric("Usage Rate", f"{stats['usage_rate']:.1f}%")
            else:
                col4.metric("Workflow Linked", stats.get(f'workflow_linked_{section_name.lower()}', 0))
        
        else:
            # Fall back to general stats
            stats = db_module.get_detailed_stats()
            col1, col2, col3, col4 = st.columns(4)
            col1.metric(f"Total {section_name}", stats.get(f'total_{section_name.lower()}', 0))
            col2.metric(f"Used {section_name}", stats.get(f'used_{section_name.lower()}', 0))
            col3.metric(f"Unused {section_name}", stats.get(f'unused_{section_name.lower()}', 0))
            col4.metric("Workflow Linked", stats.get(f'workflow_linked_{section_name.lower()}', 0))
            
    except Exception as e:
        st.error(f"Error fetching {section_name.lower()} stats: {str(e)}")