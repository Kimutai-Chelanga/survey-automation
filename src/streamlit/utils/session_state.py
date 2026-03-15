import streamlit as st

class SessionStateManager:
    """Manages Streamlit session state initialization."""
    
    SESSION_KEYS = {
        'replies_input': "",
        'messages_input': "",
        'retweets_input': "",
        'links_to_scrape_input': "",
        'refresh_stats': True,
        'workflow_status': {},
        'trigger_daily_workflow': False,
        'trigger_hourly_workflow': False,
        'refresh_links_stats': True,
        'refresh_mongo_workflows': True
    }
    
    @classmethod
    def initialize(cls):
        """Initialize all session state variables."""
        for key, default_value in cls.SESSION_KEYS.items():
            if key not in st.session_state:
                st.session_state[key] = default_value