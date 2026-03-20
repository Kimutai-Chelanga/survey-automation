import streamlit as st


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