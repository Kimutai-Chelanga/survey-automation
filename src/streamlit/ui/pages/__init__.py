import streamlit as st

from .dashboard import DashboardPage
from .accounts import AccountsPage
from .links import LinksPage
from .content import ContentPage
from .workflows import WorkflowsPage
from .prompts import PromptsPage
from .manual_executor import ManualExecutor
from .reverse_dags import ReverseDagsPage   # ✅ NEW
from .settings_page import SettingsPage


class PageManager:
    """Manages page routing and rendering."""

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.pages = {
            "Dashboard":      DashboardPage(db_manager),
            "Accounts":       AccountsPage(db_manager),
            "Links":          LinksPage(db_manager),
            "Content":        ContentPage(db_manager),
            "Workflows":      WorkflowsPage(db_manager),
            "Prompts":        PromptsPage(db_manager),
            "Manual Executor": ManualExecutor(db_manager),
            "Reverse DAGs":   ReverseDagsPage(db_manager),   # ✅ NEW
            "Settings":       SettingsPage(db_manager),
        }

    def render_page(self, page_name: str):
        """Render the selected page."""
        if page_name in self.pages:
            self.pages[page_name].render()
        else:
            st.error(f"Page '{page_name}' not found!")
