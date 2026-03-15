# File: main.py
import streamlit as st
from config.app_config import AppConfig
from core.database_manager import DatabaseManager
from ui.sidebar import Sidebar
from ui.pages import PageManager
from utils.session_state import SessionStateManager
from utils.logger import AppLogger

def main():
    """Main application entry point."""
    # Initialize logger
    logger = AppLogger.get_logger(__name__)
    logger.info("Starting Social Media Automation Dashboard...")

    # Configure Streamlit page
    AppConfig.configure_streamlit()

    # Initialize databases
    db_manager = DatabaseManager()
    db_manager.initialize()

    # Initialize session state
    SessionStateManager.initialize()

    # Initialize sidebar and get selected page
    sidebar = Sidebar()
    selected_page = sidebar.render()

    # Initialize page manager and render selected page
    page_manager = PageManager(db_manager)
    page_manager.render_page(selected_page)

if __name__ == "__main__":
    main()
