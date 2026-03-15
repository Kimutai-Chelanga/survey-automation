# File: utils/auth.py
import streamlit as st
import hashlib
import hmac
from typing import Optional, Dict
import os
from datetime import datetime, timedelta

class AuthManager:
    """Manages user authentication for the Streamlit app."""
    
    def __init__(self):
        """Initialize authentication manager."""
        # Store credentials in environment variables or a secure config
        # For production, use environment variables
        self.credentials = {
            "username": os.getenv("APP_USERNAME", "admin"),
            "password_hash": os.getenv("APP_PASSWORD_HASH", self._hash_password("changeme123"))
        }
        
        # Session timeout in minutes
        self.session_timeout = int(os.getenv("SESSION_TIMEOUT", "480"))  # 8 hours default
    
    @staticmethod
    def _hash_password(password: str) -> str:
        """Hash a password using SHA-256."""
        return hashlib.sha256(password.encode()).hexdigest()
    
    def verify_password(self, username: str, password: str) -> bool:
        """Verify username and password."""
        if username != self.credentials["username"]:
            return False
        
        password_hash = self._hash_password(password)
        return hmac.compare_digest(password_hash, self.credentials["password_hash"])
    
    def check_session_timeout(self) -> bool:
        """Check if the current session has timed out."""
        if "last_activity" not in st.session_state:
            return True
        
        last_activity = st.session_state.last_activity
        current_time = datetime.now()
        time_diff = current_time - last_activity
        
        return time_diff > timedelta(minutes=self.session_timeout)
    
    def update_activity(self):
        """Update the last activity timestamp."""
        st.session_state.last_activity = datetime.now()
    
    def login(self, username: str, password: str) -> bool:
        """Authenticate user and create session."""
        if self.verify_password(username, password):
            st.session_state.authenticated = True
            st.session_state.username = username
            st.session_state.last_activity = datetime.now()
            return True
        return False
    
    def logout(self):
        """Clear authentication session."""
        st.session_state.authenticated = False
        st.session_state.username = None
        st.session_state.last_activity = None
    
    def is_authenticated(self) -> bool:
        """Check if user is authenticated and session is valid."""
        if not st.session_state.get("authenticated", False):
            return False
        
        if self.check_session_timeout():
            self.logout()
            return False
        
        # Update activity on each check
        self.update_activity()
        return True
    
    def render_login_page(self):
        """Render the login page."""
        st.markdown("""
            <style>
            .login-container {
                max-width: 400px;
                margin: 100px auto;
                padding: 40px;
                background: white;
                border-radius: 10px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }
            .login-title {
                text-align: center;
                color: #1f77b4;
                margin-bottom: 30px;
            }
            </style>
        """, unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            st.markdown('<h1 class="login-title">🔒 Login</h1>', unsafe_allow_html=True)
            st.markdown("### Social Media Automation Dashboard")
            st.markdown("---")
            
            # Login form
            with st.form("login_form"):
                username = st.text_input("Username", placeholder="Enter your username")
                password = st.text_input("Password", type="password", placeholder="Enter your password")
                submit = st.form_submit_button("Login", use_container_width=True)
                
                if submit:
                    if username and password:
                        if self.login(username, password):
                            st.success("✅ Login successful!")
                            st.rerun()
                        else:
                            st.error("❌ Invalid username or password")
                    else:
                        st.warning("⚠️ Please enter both username and password")
            
            st.markdown("---")
            st.caption("🔐 Secure access required")


def require_authentication(func):
    """Decorator to require authentication for a function."""
    def wrapper(*args, **kwargs):
        auth_manager = AuthManager()
        
        if not auth_manager.is_authenticated():
            auth_manager.render_login_page()
            st.stop()
        
        return func(*args, **kwargs)
    
    return wrapper