# File: ui/pages/base_page.py
from abc import ABC, abstractmethod
import streamlit as st

class BasePage(ABC):
    """Base class for all pages."""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    @abstractmethod
    def render(self):
        """Render the page content."""
        pass
    
    def show_error(self, message: str):
        """Display error message."""
        st.error(f"❌ {message}")
    
    def show_success(self, message: str):
        """Display success message."""
        st.success(f"✅ {message}")
    
    def show_info(self, message: str):
        """Display info message."""
        st.info(message)