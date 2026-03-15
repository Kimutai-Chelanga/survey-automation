# File: config/theme_manager.py
import streamlit as st

class ThemeManager:
    """Manages dark/light theme across the application."""
    
    LIGHT_THEME = {
        'name': 'light',
        'background': '#ffffff',
        'secondary_background': '#f8f9fa',
        'text_color': '#262730',
        'secondary_text': '#6c757d',
        'accent_color': '#ff6b6b',
        'success_color': '#28a745',
        'warning_color': '#ffc107',
        'error_color': '#dc3545',
        'border_color': '#dee2e6',
        'card_background': '#ffffff',
        'sidebar_background': '#f8f9fa'
    }
    
    DARK_THEME = {
        'name': 'dark',
        'background': '#0e1117',
        'secondary_background': '#262730',
        'text_color': '#fafafa',
        'secondary_text': '#a9a9a9',
        'accent_color': '#ff6b6b',
        'success_color': '#4ade80',
        'warning_color': '#facc15',
        'error_color': '#ef4444',
        'border_color': '#404040',
        'card_background': '#1e1e1e',
        'sidebar_background': '#262730'
    }
    
    @classmethod
    def initialize_theme(cls):
        """Initialize theme in session state if not present."""
        if 'theme_mode' not in st.session_state:
            st.session_state.theme_mode = 'light'
    
    @classmethod
    def get_current_theme(cls):
        """Get current theme configuration."""
        cls.initialize_theme()
        return cls.DARK_THEME if st.session_state.theme_mode == 'dark' else cls.LIGHT_THEME
    
    @classmethod
    def toggle_theme(cls):
        """Toggle between light and dark theme."""
        cls.initialize_theme()
        st.session_state.theme_mode = 'light' if st.session_state.theme_mode == 'dark' else 'dark'
    
    @classmethod
    def apply_theme_css(cls):
        """Apply CSS for current theme."""
        theme = cls.get_current_theme()
        
        css = f"""
        <style>
        /* Main app styling */
        .stApp {{
            background-color: {theme['background']};
            color: {theme['text_color']};
        }}
        
        /* Sidebar styling */
        .css-1d391kg {{
            background-color: {theme['sidebar_background']};
        }}
        
        /* Metric containers */
        .metric-container {{
            background-color: {theme['card_background']};
            padding: 1rem;
            border-radius: 10px;
            border: 1px solid {theme['border_color']};
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 1rem;
            transition: all 0.3s ease;
        }}
        
        .metric-container:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.15);
        }}
        
        /* Custom metric styling */
        div[data-testid="metric-container"] {{
            background-color: {theme['card_background']};
            border: 1px solid {theme['border_color']};
            padding: 1rem;
            border-radius: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        div[data-testid="metric-container"] > div {{
            color: {theme['text_color']};
        }}
        
        div[data-testid="metric-container"] label {{
            color: {theme['secondary_text']} !important;
            font-weight: 600;
        }}
        
        div[data-testid="metric-container"] div[data-testid="metric-value"] {{
            color: {theme['accent_color']} !important;
            font-size: 2rem !important;
            font-weight: 700 !important;
        }}
        
        /* Headers */
        h1, h2, h3, h4, h5, h6 {{
            color: {theme['text_color']} !important;
        }}
        
        /* Subheader styling */
        .stSubheader {{
            color: {theme['text_color']};
            border-bottom: 2px solid {theme['accent_color']};
            padding-bottom: 0.5rem;
            margin-bottom: 1.5rem;
        }}
        
        /* Sidebar elements */
        .stSidebar {{
            background-color: {theme['sidebar_background']};
        }}
        
        .stSidebar .stRadio > div {{
            background-color: {theme['sidebar_background']};
            color: {theme['text_color']};
        }}
        
        .stSidebar .stCheckbox > div {{
            color: {theme['text_color']};
        }}
        
        /* Button styling */
        .stButton > button {{
            background-color: {theme['accent_color']};
            color: white;
            border: none;
            border-radius: 20px;
            padding: 0.5rem 1rem;
            font-weight: 600;
            transition: all 0.3s ease;
        }}
        
        .stButton > button:hover {{
            background-color: {theme['accent_color']};
            opacity: 0.8;
            transform: translateY(-1px);
        }}
        
        /* Toggle button styling */
        .theme-toggle {{
            position: fixed;
            top: 20px;
            right: 20px;
            z-index: 999999;
            background-color: {theme['accent_color']};
            color: white;
            border: none;
            border-radius: 50%;
            width: 50px;
            height: 50px;
            cursor: pointer;
            box-shadow: 0 4px 8px rgba(0,0,0,0.2);
            transition: all 0.3s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.2rem;
        }}
        
        .theme-toggle:hover {{
            transform: scale(1.1);
            box-shadow: 0 6px 12px rgba(0,0,0,0.3);
        }}
        
        /* Data display styling */
        .stDataFrame {{
            background-color: {theme['card_background']};
            border: 1px solid {theme['border_color']};
            border-radius: 10px;
        }}
        
        /* Markdown styling */
        .stMarkdown {{
            color: {theme['text_color']};
        }}
        
        /* Horizontal rule styling */
        hr {{
            border-color: {theme['border_color']};
            margin: 2rem 0;
        }}
        
        /* Success/Warning/Error messages */
        .stSuccess {{
            background-color: {theme['success_color']}20;
            border-left: 4px solid {theme['success_color']};
        }}
        
        .stWarning {{
            background-color: {theme['warning_color']}20;
            border-left: 4px solid {theme['warning_color']};
        }}
        
        .stError {{
            background-color: {theme['error_color']}20;
            border-left: 4px solid {theme['error_color']};
        }}
        
        /* Input fields */
        .stTextInput > div > div > input {{
            background-color: {theme['card_background']};
            color: {theme['text_color']};
            border: 1px solid {theme['border_color']};
        }}
        
        .stSelectbox > div > div > select {{
            background-color: {theme['card_background']};
            color: {theme['text_color']};
            border: 1px solid {theme['border_color']};
        }}
        
        /* Section dividers */
        .section-divider {{
            background: linear-gradient(90deg, transparent, {theme['accent_color']}, transparent);
            height: 2px;
            margin: 2rem 0;
            border-radius: 1px;
        }}
        
        /* Card styling for better organization */
        .dashboard-card {{
            background-color: {theme['card_background']};
            padding: 1.5rem;
            border-radius: 15px;
            border: 1px solid {theme['border_color']};
            margin-bottom: 2rem;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        
        /* Responsive design */
        @media (max-width: 768px) {{
            .theme-toggle {{
                top: 10px;
                right: 10px;
                width: 40px;
                height: 40px;
                font-size: 1rem;
            }}
        }}
        </style>
        """
        
        st.markdown(css, unsafe_allow_html=True)
    
    @classmethod
    def render_theme_toggle(cls):
        """Render theme toggle button."""
        theme = cls.get_current_theme()
        icon = "🌙" if theme['name'] == 'light' else "☀️"
        
        # Create toggle button with JavaScript
        toggle_js = f"""
        <div class="theme-toggle" onclick="toggleTheme()" title="Toggle Theme">
            {icon}
        </div>
        
        <script>
        function toggleTheme() {{
            // This will be handled by Streamlit's button interaction
            const event = new CustomEvent('themeToggle');
            window.dispatchEvent(event);
        }}
        </script>
        """
        
        st.markdown(toggle_js, unsafe_allow_html=True)


# File: config/app_config.py (Updated)
import streamlit as st
from .theme_manager import ThemeManager

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
            page_title=cls.PAGE_TITLE,
            page_icon="🚀",
            initial_sidebar_state="expanded"
        )
        
        # Initialize and apply theme
        ThemeManager.initialize_theme()
        ThemeManager.apply_theme_css()


# File: ui/sidebar.py (Updated)
import streamlit as st
from config.theme_manager import ThemeManager

class Sidebar:
    """Manages the sidebar navigation."""
    
    PAGES = [
        "Dashboard",
        "Links", 
        "Actions",
        "Logs & Screenshots",
        "Workflows",
        "Prompts",
        "Settings",
        "Clean Data"
    ]
    
    def render(self) -> str:
        """Render sidebar and return selected page."""
        with st.sidebar:
            # Theme toggle in sidebar
            st.markdown("### 🎨 Theme")
            col1, col2 = st.columns([1, 1])
            
            with col1:
                if st.button("🌙 Dark" if st.session_state.get('theme_mode', 'light') == 'light' else "☀️ Light"):
                    ThemeManager.toggle_theme()
                    st.rerun()
            
            with col2:
                current_theme = st.session_state.get('theme_mode', 'light').title()
                st.write(f"**{current_theme}**")
            
            st.markdown("---")
            
            # Navigation
            st.markdown("### 🧭 Navigation")
            selected_page = st.radio("Go to", self.PAGES, label_visibility="collapsed")
            
            return selected_page


# File: ui/pages/dashboard.py (Updated)
import streamlit as st
from ..base_page import BasePage
from config.theme_manager import ThemeManager
from src.core.database.postgres import (
    users as pg_users,
    replies as pg_replies,
    messages as pg_messages,
    retweets as pg_retweets,
    links as pg_links,
)

class DashboardPage(BasePage):
    """Enhanced Dashboard page with modern dark mode support."""

    def render(self):
        # Apply theme CSS
        ThemeManager.apply_theme_css()
        theme = ThemeManager.get_current_theme()
        
        # Main header with modern styling
        st.markdown(f"""
        <div style="background: linear-gradient(90deg, {theme['accent_color']}, {theme['accent_color']}88); 
                    padding: 2rem; border-radius: 15px; margin-bottom: 2rem; text-align: center;">
            <h1 style="color: white; margin: 0; font-size: 2.5rem; font-weight: 700;">
                📊 Dashboard
            </h1>
            <p style="color: white; margin: 0.5rem 0 0 0; opacity: 0.9;">
                Social Media Automation Analytics
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Sidebar controls
        with st.sidebar:
            st.markdown("### 🔍 Dashboard Sections")
            show_users = st.checkbox("👥 Users", value=True)
            show_content = st.checkbox("📝 Content", value=True)
            show_links = st.checkbox("🔗 Links", value=True)

        # Render sections with modern cards
        if show_users:
            self._render_users_section(theme)

        if show_content:
            self._render_content_section(theme)

        if show_links:
            self._render_links_section(theme)

    def _render_users_section(self, theme):
        """Render users section with modern card styling."""
        st.markdown(f"""
        <div class="dashboard-card">
            <h2 style="color: {theme['text_color']}; margin-bottom: 1.5rem; display: flex; align-items: center;">
                👥 Users Overview
            </h2>
        """, unsafe_allow_html=True)
        
        stats = pg_users.get_user_stats() or {}
        
        # Create metrics with custom styling
        cols = st.columns(4, gap="medium")
        metrics = [
            ("Total Users", "total_users", "👤"),
            ("Active Replies", "active_replies_workflows", "💬"),
            ("Active Messages", "active_messages_workflows", "📧"),
            ("Active Retweets", "active_retweets_workflows", "🔄")
        ]
        
        for col, (label, key, icon) in zip(cols, metrics):
            with col:
                value = stats.get(key, 0)
                st.markdown(f"""
                <div class="metric-container">
                    <div style="font-size: 1.5rem; text-align: center; margin-bottom: 0.5rem;">{icon}</div>
                    <div style="font-size: 2rem; font-weight: 700; color: {theme['accent_color']}; text-align: center;">{value}</div>
                    <div style="color: {theme['secondary_text']}; text-align: center; font-size: 0.9rem;">{label}</div>
                </div>
                """, unsafe_allow_html=True)
        
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    def _render_content_section(self, theme):
        """Render content section with modern card styling."""
        st.markdown(f"""
        <div class="dashboard-card">
            <h2 style="color: {theme['text_color']}; margin-bottom: 1.5rem; display: flex; align-items: center;">
                📝 Content Statistics
            </h2>
        """, unsafe_allow_html=True)
        
        # Replies section
        st.markdown(f"<h3 style='color: {theme['accent_color']}; margin-bottom: 1rem;'>💬 Replies</h3>", unsafe_allow_html=True)
        replies = pg_replies.get_detailed_replies_stats() or {}
        self._render_content_metrics(
            replies,
            ["Total", "Used", "Unused", "Workflow-Linked"],
            ["total_replies", "used_replies", "unused_replies", "workflow_linked_replies"],
            theme
        )
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Messages section
        st.markdown(f"<h3 style='color: {theme['accent_color']}; margin-bottom: 1rem;'>📧 Messages</h3>", unsafe_allow_html=True)
        messages = pg_messages.get_detailed_messages_stats() or {}
        self._render_content_metrics(
            messages,
            ["Total", "Used", "Unused", "Workflow-Linked"],
            ["total_messages", "used_messages", "unused_messages", "workflow_linked_messages"],
            theme
        )
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Retweets section
        st.markdown(f"<h3 style='color: {theme['accent_color']}; margin-bottom: 1rem;'>🔄 Retweets</h3>", unsafe_allow_html=True)
        retweets = pg_retweets.get_detailed_retweets_stats() or {}
        self._render_content_metrics(
            retweets,
            ["Total", "Used", "Unused", "Workflow-Linked"],
            ["total_retweets", "used_retweets", "unused_retweets", "workflow_linked_retweets"],
            theme
        )
        
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    def _render_links_section(self, theme):
        """Render links section with modern card styling."""
        st.markdown(f"""
        <div class="dashboard-card">
            <h2 style="color: {theme['text_color']}; margin-bottom: 1.5rem; display: flex; align-items: center;">
                🔗 Link Statistics
            </h2>
        """, unsafe_allow_html=True)
        
        links = pg_links.get_detailed_links_stats() or {}
        self._render_content_metrics(
            links,
            ["Total Links", "Used Links", "Unused Links", "Filtered Links"],
            ["total_links", "used_links", "unused_links", "filtered_links"],
            theme
        )
        
        st.markdown("</div>", unsafe_allow_html=True)

    def _render_content_metrics(self, stats: dict, labels: list, keys: list, theme: dict):
        """Render metrics with modern styling."""
        cols = st.columns(len(labels), gap="small")
        
        for col, label, key in zip(cols, labels, keys):
            with col:
                value = stats.get(key, 0)
                st.markdown(f"""
                <div style="background-color: {theme['card_background']}; 
                           border: 1px solid {theme['border_color']}; 
                           padding: 1rem; border-radius: 8px; text-align: center;
                           box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                    <div style="font-size: 1.8rem; font-weight: 600; color: {theme['accent_color']}; margin-bottom: 0.5rem;">{value}</div>
                    <div style="color: {theme['secondary_text']}; font-size: 0.85rem;">{label}</div>
                </div>
                """, unsafe_allow_html=True)


# File: main.py (Updated)
import streamlit as st
from config.app_config import AppConfig
from config.theme_manager import ThemeManager
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
    
    # Configure Streamlit page (includes theme initialization)
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


# File: ui/components/theme_toggle.py (Optional standalone component)
import streamlit as st
from config.theme_manager import ThemeManager

class ThemeToggle:
    """Standalone theme toggle component."""
    
    @staticmethod
    def render_floating_toggle():
        """Render a floating theme toggle button."""
        theme = ThemeManager.get_current_theme()
        
        # Create a container for the toggle
        toggle_container = st.container()
        
        with toggle_container:
            if st.button(
                "🌙" if theme['name'] == 'light' else "☀️",
                key="theme_toggle",
                help="Toggle dark/light mode"
            ):
                ThemeManager.toggle_theme()
                st.rerun()
    
    @staticmethod
    def render_sidebar_toggle():
        """Render theme toggle in sidebar."""
        theme = ThemeManager.get_current_theme()
        
        st.markdown("### 🎨 Appearance")
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            if st.button(
                "🌙" if theme['name'] == 'light' else "☀️",
                key="sidebar_theme_toggle"
            ):
                ThemeManager.toggle_theme()
                st.rerun()
        
        with col2:
            st.write(f"**{theme['name'].title()} Mode**")