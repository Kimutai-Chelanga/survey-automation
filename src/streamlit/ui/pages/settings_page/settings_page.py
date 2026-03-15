import streamlit as st
from ..base_page import BasePage
from .view_settings_overview import render_view_settings_overview
from .create_content_config import render_create_content_config
from .create_automa_workflow_config import render_create_automa_workflow_config
from .extraction_processing_config import render_extraction_processing_config
from .filtering_config import render_filtering_config
from .execution_config import render_execution_config


class SettingsPage(BasePage):
    """Settings page for managing DAG configurations and viewing settings overview."""

    def __init__(self, db_manager):
        super().__init__(db_manager)
        self.mongo_client = getattr(db_manager, 'mongo_client', None) if db_manager else None
        self.settings_manager = None

    def render(self):
        """Render the settings page with tabs for different configuration sections."""
        if self.mongo_client is None:
            st.error("❌ MongoDB connection is not available.")
            st.info("Please check your database configuration and connection settings.")
            return

        st.title("🔧 Workflow Settings Management")

        # Create main tabs - removed "Update Weekly Settings"
        tab1, tab2 = st.tabs([
            "📊 View Settings Overview",
            "⚙️ System Settings"
        ])

        with tab1:
            render_view_settings_overview()

        with tab2:
            # Create nested tabs within System Settings
            subtab1, subtab2, subtab3, subtab4, subtab5 = st.tabs([
                "📝 Create Content Config",
                "🤖 Automa Workflow Config",
                "🔍 Extraction & Processing Config",
                "🎯 Filtering Configuration",
                "⚡ Execution Configuration"
            ])

            with subtab1:
                render_create_content_config()

            with subtab2:
                render_create_automa_workflow_config()

            with subtab3:
                render_extraction_processing_config()

            with subtab4:
                render_filtering_config()

            with subtab5:
                render_execution_config()
