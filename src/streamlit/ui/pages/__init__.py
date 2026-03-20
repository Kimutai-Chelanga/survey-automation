import streamlit as st

from .dashboard_page import DashboardPage
from .accounts import AccountsPage
from .questions_page import QuestionsPage
from .answers_page import AnswersPage
from .prompts_page import PromptsPage
from .generate_manual_workflows import GenerateManualWorkflowsPage


class PageManager:
    """Manages page routing and rendering."""

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.pages = {
            "Dashboard": DashboardPage(db_manager),
            "Accounts": AccountsPage(db_manager),
            "Questions": QuestionsPage(db_manager),
            "Answers": AnswersPage(db_manager),
            "Prompts": PromptsPage(db_manager),
            "Generate Manual Workflows": GenerateManualWorkflowsPage(db_manager),
        }

    def render_page(self, page_name: str):
        """Render the selected page."""
        if page_name in self.pages:
            self.pages[page_name].render()
        else:
            st.error(f"Page '{page_name}' not found!")


class Sidebar:
    """Manages the sidebar navigation - Clean and minimal."""

    PAGES = [
        "Dashboard",
        "Accounts",
        "Questions",
        "Answers",
        "Prompts",
        "Generate Manual Workflows",
    ]

    def render(self) -> str:
        """Render sidebar and return selected page."""
        st.sidebar.title("Navigation")
        selected_page = st.sidebar.radio("Go to", self.PAGES)
        return selected_page