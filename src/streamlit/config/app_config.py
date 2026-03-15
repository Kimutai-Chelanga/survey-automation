
# File: config/app_config.py
import streamlit as st

class AppConfig:
    """Application configuration settings."""
    
    PAGE_TITLE = "Social Media Automation Dashboard"
    LAYOUT = "wide"
    
    # Database settings
    MONGODB_AVAILABLE = True
    POSTGRESQL_AVAILABLE = True
    
    # UI settings
    DEFAULT_LIMIT = 100
    LOGS_LIMIT = 50
    WORKFLOWS_LIMIT = 20
    
    @classmethod
    def configure_streamlit(cls):
        """Configure Streamlit page settings."""
        st.set_page_config(
            layout=cls.LAYOUT,
            page_title=cls.PAGE_TITLE
        )





