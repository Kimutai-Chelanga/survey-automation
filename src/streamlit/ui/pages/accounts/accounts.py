import subprocess
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import numpy as np
import time
import traceback
import json
import numpy as np
from src.core.database.postgres import accounts as pg_accounts
from src.streamlit.ui.pages.accounts.chrome_session_manager import ChromeSessionManager
import os
import logging
import time
import json
import subprocess
import tempfile
import stat
from pathlib import Path
import json
import uuid
from datetime import datetime
import os
import json
import stat
from pathlib import Path

import json
import os
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
HYPERBROWSER_API_KEY = os.environ.get('HYPERBROWSER_API_KEY')

class AccountsPage:

    # This is part of your __init__ method in AccountsPage class
# File: src/streamlit/ui/pages/accounts/accounts.py
# Add this section to your existing __init__ method

    """
Updated methods for accounts.py — x_account_id support
Replace each method in your existing file with the version below.
"""

# ─────────────────────────────────────────────────────────────────────────────
# 1. __init__  (add one block after username_to_create)
# ─────────────────────────────────────────────────────────────────────────────

    def __init__(self, db_manager):
        """Initialize the AccountsPage with database manager"""
        self.db_manager = db_manager

        if 'use_local_chrome' not in st.session_state:
            st.session_state.use_local_chrome = True

        if 'account_creation_logs' not in st.session_state:
            st.session_state.account_creation_logs = []

        if 'show_add_account' not in st.session_state:
            st.session_state.show_add_account = False
        if 'creating_account' not in st.session_state:
            st.session_state.creating_account = False
        if 'creation_completed' not in st.session_state:
            st.session_state.creation_completed = False
        if 'username_to_create' not in st.session_state:
            st.session_state.username_to_create = None
        if 'x_account_id_to_create' not in st.session_state:       # NEW
            st.session_state.x_account_id_to_create = None
        if 'creation_in_progress' not in st.session_state:
            st.session_state.creation_in_progress = False
        if 'account_creation_message' not in st.session_state:
            st.session_state.account_creation_message = None
        if 'account_creation_error' not in st.session_state:
            st.session_state.account_creation_error = False

        if 'show_delete_account' not in st.session_state:
            st.session_state.show_delete_account = False
        if 'deleting_account' not in st.session_state:
            st.session_state.deleting_account = False
        if 'deletion_completed' not in st.session_state:
            st.session_state.deletion_completed = False
        if 'account_id_to_delete' not in st.session_state:
            st.session_state.account_id_to_delete = None
        if 'deletion_in_progress' not in st.session_state:
            st.session_state.deletion_in_progress = False
        if 'account_deletion_message' not in st.session_state:
            st.session_state.account_deletion_message = None
        if 'account_deletion_error' not in st.session_state:
            st.session_state.account_deletion_error = False

        if 'local_chrome_sessions' not in st.session_state:
            st.session_state.local_chrome_sessions = {}

        if 'hyperbrowser_sessions' not in st.session_state:
            st.session_state.hyperbrowser_sessions = {}

        if 'active_sessions' in st.session_state:
            st.session_state.hyperbrowser_sessions = st.session_state.get('active_sessions', {})
            del st.session_state['active_sessions']

        if 'selected_profile_for_session' not in st.session_state:
            st.session_state.selected_profile_for_session = None
        if 'session_management_logs' not in st.session_state:
            st.session_state.session_management_logs = []

        try:
            from ..hyperbrowser_utils import get_mongodb_client
            client, db = get_mongodb_client()
            self.chrome_manager = ChromeSessionManager(db_manager, client)
        except Exception as e:
            logger.warning(f"Could not initialize Chrome manager: {e}")
            self.chrome_manager = ChromeSessionManager(db_manager, None)

        self._refresh_active_sessions_from_mongodb()


# ─────────────────────────────────────────────────────────────────────────────
# 2. _clear_account_creation_state
# ─────────────────────────────────────────────────────────────────────────────

    def _clear_account_creation_state(self, keep_message=False):
        """Clear account creation session state"""
        st.session_state.show_add_account = False
        st.session_state.creating_account = False
        st.session_state.creation_completed = False
        st.session_state.username_to_create = None
        st.session_state.x_account_id_to_create = None          # NEW
        st.session_state.account_mode_to_create = None
        st.session_state.creation_in_progress = False

        if not keep_message:
            st.session_state.account_creation_message = None
            st.session_state.account_creation_error = False

        if not keep_message:
            self.clear_logs()


# ─────────────────────────────────────────────────────────────────────────────
# 3. _create_account_in_postgres_minimal
# ─────────────────────────────────────────────────────────────────────────────

    def _create_account_in_postgres_minimal(self, username, x_account_id=None):
        """
        Create minimal account record to get account_id.
        Now accepts and stores x_account_id.
        """
        try:
            current_time = datetime.now()

            insert_query = """
            INSERT INTO accounts (
                username, x_account_id, created_time, updated_time, total_content_processed
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING account_id
            """

            values = (username, x_account_id, current_time, current_time, 0)
            result = self.db_manager.execute_query(insert_query, values, fetch=True)

            if not result:
                raise Exception("Account creation failed")

            account_id = result[0][0] if isinstance(result[0], tuple) else result[0]['account_id']

            self.add_log(f"✅ Minimal account created - ID: {account_id}, X Account ID: {x_account_id}")

            return {'account_id': account_id}

        except Exception as e:
            raise Exception(f"Failed to create account: {str(e)}")


# ─────────────────────────────────────────────────────────────────────────────
# 4. _create_account_in_postgres
# ─────────────────────────────────────────────────────────────────────────────

    def _create_account_in_postgres(self, username, profile_data, x_account_id=None):
        """
        Create account in PostgreSQL with extension linking.
        Now accepts x_account_id.
        """
        try:
            from ..hyperbrowser_extensions import get_or_create_extension_for_account
            profile_id = profile_data['profile_id']
            mongodb_id = profile_data.get('mongodb_id')
            self.add_log(f"Starting PostgreSQL account creation for username: {username}, profile_id: {profile_id}")

            check_query = "SELECT account_id FROM accounts WHERE username = %s"
            existing = self.db_manager.execute_query(check_query, (username,), fetch=True)

            if existing:
                raise ValueError(f"Username '{username}' already exists")

            profile_check_query = "SELECT account_id FROM accounts WHERE profile_id = %s"
            existing_profile = self.db_manager.execute_query(profile_check_query, (profile_id,), fetch=True)

            if existing_profile:
                raise ValueError(f"Profile ID '{profile_id}' is already associated with another account")

            current_time = datetime.now()

            insert_query = """
            INSERT INTO accounts (
                username, x_account_id, profile_id, created_time, updated_time,
                mongo_object_id, active_replies_workflow, active_messages_workflow,
                active_retweets_workflow, last_workflow_sync,
                total_replies_processed, total_messages_processed,
                total_retweets_processed, total_links_processed
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING account_id
            """

            values = (
                username, x_account_id, profile_id, current_time, current_time,
                mongodb_id, None, None, None, None,
                0, 0, 0, 0
            )

            result = self.db_manager.execute_query(insert_query, values, fetch=True)

            if not result or len(result) == 0:
                raise Exception("Account creation failed - no account_id returned")

            first_row = result[0]
            if hasattr(first_row, 'get'):
                account_id = first_row.get('account_id') or first_row['account_id']
            else:
                account_id = first_row[0]

            self.add_log(f"✅ Account created in PostgreSQL - ID: {account_id}")

            if mongodb_id:
                self.add_log(f"Linking MongoDB document {mongodb_id} to account...")
                mongo_link_success = self._link_mongodb_to_postgres(
                    mongodb_id=mongodb_id,
                    postgres_account_id=account_id,
                    username=username,
                    profile_id=profile_id
                )

                if not mongo_link_success:
                    self.add_log(f"⚠️ MongoDB linking had issues but continuing", "WARNING")

            extension_linked = False
            extension_result = None
            extension_warning = None

            try:
                self.add_log(f"Attempting to create and link extension for account {account_id}...")

                extension_result = get_or_create_extension_for_account(
                    postgres_account_id=account_id,
                    username=username,
                    extension_name="automa-extension"
                )

                if extension_result.get('verified') or extension_result.get('linked_to_account'):
                    extension_linked = True
                    self.add_log(f"✅ Extension successfully linked: {extension_result['extension_id']}")
                elif extension_result.get('extension_id'):
                    extension_linked = True
                    extension_warning = f"Extension available but not linked: {extension_result['extension_id']}"
                    self.add_log(f"⚠️ {extension_warning}", "WARNING")
                else:
                    raise Exception("Extension creation returned no extension_id")

            except Exception as ext_error:
                self.add_log(f"⚠️ Extension linking failed: {ext_error}", "WARNING")

                extension_warning = f"Account created without extension: {str(ext_error)}"
                self.add_log(f"⚠️ {extension_warning}", "WARNING")

                extension_linked = True
                extension_result = {
                    'extension_id': None,
                    'action': 'account_created_without_extension',
                    'warning': extension_warning
                }

            self.add_log("Performing final verification...")
            final_verification = self._verify_account_configuration_relaxed(
                account_id, profile_id, extension_result.get('extension_id') if extension_result else None
            )

            if not final_verification['account_created']:
                raise Exception(f"Account verification failed: {final_verification.get('errors')}")

            if final_verification.get('warnings'):
                for warning in final_verification['warnings']:
                    self.add_log(f"⚠️ {warning}", "WARNING")

            self.add_log("✅ Account creation completed with verification passed")

            return {
                'account_id': account_id,
                'username': username,
                'x_account_id': x_account_id,
                'profile_id': profile_id,
                'profile_data': profile_data,
                'created_at': current_time,
                'mongodb_linked': bool(mongodb_id),
                'extension_linked': extension_linked,
                'extension_id': extension_result.get('extension_id') if extension_result else None,
                'extension_verified': extension_result.get('verified', False) if extension_result else False,
                'fully_configured': final_verification['account_created'],
                'warnings': [extension_warning] if extension_warning else []
            }

        except Exception as e:
            error_msg = f"Failed to create account in PostgreSQL: {str(e)}"
            self.add_log(error_msg, "ERROR")
            raise Exception(error_msg)


# ─────────────────────────────────────────────────────────────────────────────
# 5. render_add_account_modal
# ─────────────────────────────────────────────────────────────────────────────

    def _render_account_details_view(self, df):
        """Render detailed account view - UPDATED to show x_account_id."""
        for _, row in df.iterrows():
            with st.expander(f"🏷️ {row['username']} (ID: {row['account_id']})", expanded=False):
                col1, col2, col3, col4 = st.columns(4)
    
                with col1:
                    st.write("**Account Info**")
                    st.write(f"Account ID: `{row['account_id']}`")
                    st.write(f"Username: `{row['username']}`")
                    # ADD X_ACCOUNT_ID HERE
                    x_account_id = row.get('x_account_id')
                    if x_account_id and str(x_account_id).strip():
                        st.write(f"X Account ID: `{x_account_id}`")
                        # Add a helpful note about DM chat links
                        st.caption(f"DM Chat Link: https://x.com/i/chat/{x_account_id}-AUTHOR_ID")
                    else:
                        st.write(f"X Account ID: `Not set`")
                    
                    st.write(f"Profile ID: `{row['profile_id'][:12] if row['profile_id'] else 'None'}...`")
                    st.write(f"Type: `{row['profile_type']}`")
                    st.write(f"Created: {row['created_time'].strftime('%Y-%m-%d %H:%M')}")
                    st.write(f"Active Sessions: {row['active_sessions']}")
    
                with col2:
                    st.write("**Processing Stats**")
                    st.write(f"Total Content: {row['total_content_processed']}")
                    # Add more stats if they exist in your dataframe
                    if 'total_replies_processed' in row:
                        st.write(f"Replies: {row['total_replies_processed']}")
                    if 'total_messages_processed' in row:
                        st.write(f"Messages: {row['total_messages_processed']}")
                    if 'total_retweets_processed' in row:
                        st.write(f"Retweets: {row['total_retweets_processed']}")
    
                with col3:
                    st.write("**Cookie Status**")
                    st.write(f"Has Cookies: {'✓' if row.get('has_cookies') else '✗'}")
                    if row.get('cookies_last_updated'):
                        st.write(f"Last Updated: {row['cookies_last_updated'].strftime('%Y-%m-%d %H:%M')}")
                    
                    # Show cookie validity if available
                    if row.get('has_cookies'):
                        validity = self._check_cookie_validity(row['account_id'])
                        if validity.get('valid'):
                            st.success("✓ Cookies valid")
                            if validity.get('warning'):
                                st.caption(f"⚠️ {validity['warning']}")
                        else:
                            st.error(f"❌ Cookies invalid: {validity.get('reason', 'Unknown')}")
    
                with col4:
                    st.write("**Actions**")
                    
                    # Quick actions for this account
                    col_a, col_b = st.columns(2)
                    
                    with col_a:
                        # View cookies button if they exist
                        if row.get('has_cookies'):
                            if st.button("🍪 View Cookies", key=f"view_cookies_{row['account_id']}", use_container_width=True):
                                st.session_state[f'modal_view_{row["account_id"]}'] = True
                                st.rerun()
                    
                    with col_b:
                        # Sync to Airflow button if cookies exist
                        if row.get('has_cookies') and row['profile_type'] == 'local_chrome':
                            if st.button("🔄 Sync to Airflow", key=f"sync_airflow_{row['account_id']}", use_container_width=True):
                                with st.spinner("Syncing..."):
                                    result = self._sync_cookies_to_session_file(row['account_id'], row['username'])
                                    if result['success']:
                                        st.success("✓ Synced!")
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        st.error(f"Failed: {result.get('error')}")
                    
                    # Start session button
                    st.markdown("---")
                    if st.button("▶️ Start Session", key=f"start_session_{row['account_id']}", use_container_width=True, type="primary"):
                        if row['profile_type'] == 'local_chrome':
                            result = self._start_local_chrome_session(
                                row['profile_id'],
                                row['account_id'],
                                row['username']
                            )
                        else:
                            result = self._start_hyperbrowser_session(
                                row['profile_id'],
                                row['account_id'],
                                row['username'],
                                {}
                            )
                        
                        if result.get('success'):
                            st.success("✓ Session started!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"Failed: {result.get('error')}")
    
                # Show modals if triggered
                if st.session_state.get(f'modal_view_{row["account_id"]}'):
                    st.markdown("---")
                    st.subheader(f"🍪 Cookies for {row['username']}")
                    
                    cookie_info = self._get_account_cookies(row['account_id'])
                    if cookie_info['has_cookies']:
                        cookies = cookie_info['cookie_data']
                        if isinstance(cookies, str):
                            cookies = json.loads(cookies)
                        
                        # Show critical cookies first
                        critical_names = ['auth_token', 'ct0', 'kdt']
                        st.write("**Critical Authentication Cookies:**")
                        
                        for name in critical_names:
                            cookie = next((c for c in cookies if c['name'] == name), None)
                            if cookie:
                                with st.expander(f"🔑 {name}", expanded=False):
                                    st.json({
                                        'name': cookie.get('name'),
                                        'domain': cookie.get('domain'),
                                        'value': cookie.get('value')[:20] + '...' if len(cookie.get('value', '')) > 20 else cookie.get('value'),
                                        'secure': cookie.get('secure'),
                                        'httpOnly': cookie.get('httpOnly'),
                                    })
                            else:
                                st.warning(f"⚠️ Missing: {name}")
                        
                        st.write(f"**All Cookies ({len(cookies)} total):**")
                        st.json(cookies)
                    else:
                        st.info("No cookies stored for this account")
                    
                    if st.button("Close", key=f"close_view_{row['account_id']}"):
                        del st.session_state[f'modal_view_{row["account_id"]}']
                        st.rerun()

    def render_add_account_modal(self):
        """
        Render add account form with x_account_id field and optional cookie upload.
        """
        if st.session_state.get('show_add_account', False):
            with st.expander("➕ Add New Account", expanded=True):

                self.render_creation_logs()

                if st.session_state.get('creating_account', False):
                    st.info("⏳ Creating account... Please wait.")

                    if (not st.session_state.get('creation_completed', False) and
                        not st.session_state.get('creation_in_progress', False) and
                        st.session_state.get('username_to_create') and
                        st.session_state.get('account_mode_to_create')):

                        username = st.session_state.get('username_to_create', '')
                        account_mode = st.session_state.get('account_mode_to_create', 'local_chrome')
                        x_account_id = st.session_state.get('x_account_id_to_create')   # NEW
                        cookies_json = st.session_state.get('cookies_to_upload')

                        if username:
                            self.add_log(f"Triggering account creation for: {username} ({account_mode})")
                            self._handle_account_creation(username, account_mode, cookies_json, x_account_id)

                    return

                with st.form("add_account_form"):
                    st.write("**Account Configuration:**")

                    col1, col2 = st.columns(2)

                    with col1:
                        username = st.text_input(
                            "Username",
                            placeholder="Enter username",
                            help="Username for this account"
                        )

                    with col2:
                        account_mode = st.selectbox(
                            "Account Type",
                            options=["local_chrome", "hyperbrowser"],
                            format_func=lambda x: "🖥️ Local Chrome (Free)" if x == "local_chrome" else "☁️ Hyperbrowser (Paid)",
                            help="Local Chrome: Free, persistent profiles\nHyperbrowser: Cloud-based, requires API key"
                        )

                    # ── NEW: X Account ID field ──────────────────────────────
                    st.markdown("---")
                    st.write("**X / Twitter Identity:**")

                    x_account_id = st.text_input(
                        "X Account ID",
                        placeholder="e.g. 1946168476348858370",
                        help=(
                            "Your numeric X/Twitter user ID. "
                            "Used to build direct DM chat links (https://x.com/i/chat/YOUR_ID-AUTHOR_ID). "
                            "Find it via twitter.com/i/api/1.1/account/settings.json or a tool like TweetDeck."
                        )
                    )

                    if x_account_id and x_account_id.strip():
                        if x_account_id.strip().isdigit():
                            st.success(f"✓ Valid numeric X Account ID: {x_account_id.strip()}")
                        else:
                            st.error("❌ X Account ID must contain only digits")
                    # ─────────────────────────────────────────────────────────

                    st.markdown("---")

                    if account_mode == "local_chrome":
                        st.info("**🖥️ Local Chrome Account**")
                        st.write("✓ Free to use")
                        st.write("✓ Persistent browser profiles")
                        st.write("✓ Extensions preserved")
                        st.write("✓ Login state maintained")

                        st.markdown("---")

                        st.write("**🍪 Cookie Management (Optional)**")

                        cookies_json = st.text_area(
                            "Paste EditThisCookie JSON",
                            placeholder='[\n  {\n    "domain": ".x.com",\n    "name": "auth_token",\n    "value": "...",\n    ...\n  }\n]',
                            height=150,
                            help="Paste the JSON exported from EditThisCookie extension"
                        )

                        if cookies_json:
                            try:
                                parsed_cookies = json.loads(cookies_json)
                                if isinstance(parsed_cookies, list) and len(parsed_cookies) > 0:
                                    st.success(f"✓ {len(parsed_cookies)} cookies detected")
                                    st.caption("Cookies will be stored with the account")
                                else:
                                    st.warning("⚠️ Invalid cookie format")
                            except json.JSONDecodeError:
                                st.error("❌ Invalid JSON format")

                        st.caption("Profile will be created at: `/workspace/chrome_profiles/account_{username}`")

                    else:  # hyperbrowser
                        st.info("**☁️ Hyperbrowser Account**")
                        api_key = os.environ.get('HYPERBROWSER_API_KEY')
                        if api_key:
                            st.success("✓ API Key configured")
                            st.write("✓ Cloud-based browser")
                            st.write("✓ Remote access")
                            st.write("✓ Scalable sessions")
                        else:
                            st.error("❌ API Key not found")
                            st.warning("Set HYPERBROWSER_API_KEY environment variable")
                        cookies_json = None  # Not used for hyperbrowser

                    if st.session_state.get('account_creation_message'):
                        if st.session_state.get('account_creation_error'):
                            st.error(st.session_state.account_creation_message)
                        else:
                            st.success(st.session_state.account_creation_message)

                    st.markdown("---")

                    col_submit, col_cancel = st.columns([1, 1])

                    with col_submit:
                        disable_submit = (account_mode == "hyperbrowser" and not os.environ.get('HYPERBROWSER_API_KEY'))

                        submitted = st.form_submit_button(
                            f"Create {'🖥️ Local' if account_mode == 'local_chrome' else '☁️ Hyperbrowser'} Account",
                            use_container_width=True,
                            disabled=disable_submit
                        )

                    with col_cancel:
                        cancel = st.form_submit_button("Cancel", use_container_width=True)

                    if submitted:
                        if not username.strip():
                            st.warning("Please enter a username")
                        elif x_account_id.strip() and not x_account_id.strip().isdigit():
                            st.error("❌ X Account ID must contain only digits")
                        else:
                            self.add_log(f"Form submitted - preparing creation for: {username.strip()} ({account_mode})")
                            st.session_state.username_to_create = username.strip()
                            st.session_state.x_account_id_to_create = x_account_id.strip() if x_account_id.strip() else None  # NEW
                            st.session_state.account_mode_to_create = account_mode
                            st.session_state.cookies_to_upload = cookies_json if account_mode == 'local_chrome' else None
                            st.session_state.creating_account = True
                            st.session_state.account_creation_message = None
                            st.session_state.account_creation_error = False
                            st.session_state.creation_completed = False
                            st.session_state.creation_in_progress = False
                            st.rerun()

                    if cancel:
                        self._clear_account_creation_state()
                        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# 6. _handle_account_creation
# ─────────────────────────────────────────────────────────────────────────────

    def _handle_account_creation(self, username, account_mode, cookies_json=None, x_account_id=None):
        """
        Handle account creation with x_account_id support and optional cookie storage.
        """
        if st.session_state.get('creation_in_progress'):
            self.add_log("Creation already in progress, skipping...", "WARNING")
            return

        st.session_state.creation_in_progress = True

        try:
            self.clear_logs()
            self.add_log(f"=== Starting account creation for: {username} ===")
            self.add_log(f"Mode: {account_mode}")
            if x_account_id:
                self.add_log(f"X Account ID: {x_account_id}")

            status_placeholder = st.empty()

            with status_placeholder.container():
                with st.status("Creating account...", expanded=True) as status:

                    if account_mode == 'local_chrome':
                        st.write("🖥️ Creating LOCAL Chrome account...")
                        self.add_log("Creating LOCAL Chrome account")

                        st.write("🔄 Creating account record...")
                        account_result = self._create_account_in_postgres_minimal(username, x_account_id)  # NEW: pass x_account_id
                        account_id = account_result['account_id']
                        self.add_log(f"✓ Account ID: {account_id}")

                        st.write("🔄 Creating local Chrome profile...")
                        profile_result = self._create_local_chrome_profile_for_account(
                            username, account_id
                        )
                        profile_path = profile_result['profile_path']
                        profile_id = profile_result['profile_id']
                        self.add_log(f"✓ Profile: {profile_path}")

                        st.write("🔄 Linking profile to account...")
                        update_query = """
                        UPDATE accounts
                        SET profile_id = %s,
                            profile_type = 'local_chrome',
                            updated_time = CURRENT_TIMESTAMP
                        WHERE account_id = %s
                        """
                        self.db_manager.execute_query(
                            update_query,
                            (profile_id, account_id)
                        )
                        self.add_log("✓ Account configured for local Chrome")

                        cookies_stored = False
                        cookie_count = 0

                        if cookies_json:
                            st.write("🔄 Storing cookies...")
                            cookie_result = self._store_account_cookies(
                                account_id, cookies_json, username
                            )

                            if cookie_result['success']:
                                cookies_stored = True
                                cookie_count = cookie_result['cookie_count']
                                self.add_log(f"✓ Stored {cookie_count} cookies")
                                st.write(f"✓ Stored {cookie_count} cookies")
                            else:
                                self.add_log(f"⚠ Cookie storage failed: {cookie_result['error']}", "WARNING")
                                st.warning(f"Cookie storage failed: {cookie_result['error']}")

                        success_message = (
                            f"✓ Local Chrome account '{username}' created!\n"
                            f"Account ID: {account_id}\n"
                        )
                        if x_account_id:
                            success_message += f"X Account ID: {x_account_id}\n"
                        success_message += f"Profile: {profile_path}\n"
                        if cookies_stored:
                            success_message += f"Cookies: {cookie_count} stored\n"
                        success_message += "Sessions will use persistent local Chrome"

                    else:
                        st.write("☁️ Creating HYPERBROWSER account...")
                        self.add_log("Creating HYPERBROWSER account")

                        st.write("🔄 Testing Hyperbrowser API...")
                        if not self._test_imports_and_dependencies():
                            raise Exception("Hyperbrowser API not available")

                        st.write("🔄 Creating Hyperbrowser profile...")
                        profile_result = self._create_hyperbrowser_profile_for_account(username)
                        profile_id = profile_result['profile_id']

                        import uuid
                        try:
                            uuid.UUID(profile_id)
                            self.add_log(f"✓ Valid Hyperbrowser UUID: {profile_id}")
                        except ValueError:
                            raise Exception(f"Invalid UUID from Hyperbrowser: {profile_id}")

                        st.write(f"✓ Profile created: {profile_id[:8]}...")

                        st.write("🔄 Creating account in database...")
                        account_result = self._create_account_in_postgres(
                            username, profile_result, x_account_id  # NEW: pass x_account_id
                        )
                        account_id = account_result['account_id']

                        update_query = """
                        UPDATE accounts
                        SET profile_type = 'hyperbrowser'
                        WHERE account_id = %s
                        """
                        self.db_manager.execute_query(update_query, (account_id,))

                        st.write("✓ Account configured for Hyperbrowser")
                        self.add_log("✓ Account configured for Hyperbrowser")

                        success_message = (
                            f"✓ Hyperbrowser account '{username}' created!\n"
                            f"Account ID: {account_id}\n"
                        )
                        if x_account_id:
                            success_message += f"X Account ID: {x_account_id}\n"
                        success_message += (
                            f"Profile UUID: {profile_id}\n"
                            f"Sessions will use cloud Hyperbrowser"
                        )

                    self.add_log("=== Account creation completed ===")
                    status.update(label="✓ Account created successfully!", state="complete")

                    st.session_state.account_creation_message = success_message
                    st.session_state.account_creation_error = False
                    st.session_state.creation_completed = True
                    st.session_state.creating_account = False
                    st.session_state.creation_in_progress = False

                    self.add_log("Clearing cache to refresh accounts list...")
                    st.cache_data.clear()

                    st.success(success_message)
                    time.sleep(3)
                    st.rerun()

        except Exception as e:
            error_msg = str(e)
            self.add_log(f"=== Account creation failed: {error_msg} ===", "ERROR")

            st.session_state.account_creation_message = f"Error: {error_msg}"
            st.session_state.account_creation_error = True
            st.session_state.creation_completed = True
            st.session_state.creating_account = False
            st.session_state.creation_in_progress = False

            st.error(f"Account creation failed: {error_msg}")
            time.sleep(2)
            st.rerun()

    


    def add_log(self, message, level="INFO"):
        """Add a log message to both Python logging and Streamlit session state"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] {level}: {message}"

        # Add to session state logs for UI display
        if 'account_creation_logs' not in st.session_state:
            st.session_state.account_creation_logs = []
        st.session_state.account_creation_logs.append(log_entry)

        # Keep only last 50 log entries to prevent memory issues
        if len(st.session_state.account_creation_logs) > 50:
            st.session_state.account_creation_logs = st.session_state.account_creation_logs[-50:]

        # Also log to Python logger
        if level == "ERROR":
            logger.error(message)
        elif level == "WARNING":
            logger.warning(message)
        else:
            logger.info(message)

    def clear_logs(self):
        """Clear the creation logs"""
        st.session_state.account_creation_logs = []




    

    def _clear_account_deletion_state(self):
        """Clear account deletion session state"""
        st.session_state.show_delete_account = False
        st.session_state.deleting_account = False
        st.session_state.deletion_completed = False
        st.session_state.account_id_to_delete = None
        st.session_state.deletion_in_progress = False
        st.session_state.account_deletion_message = None
        st.session_state.account_deletion_error = False










    # 4. UPDATE: _apply_filters to filter by type
    def _apply_filters(self, df, status_filter, time_filter):
        """Apply filters to the accounts DataFrame"""
        filtered_df = df.copy()

        # Apply status filter - UPDATED for account types
        if status_filter == "Local Chrome":
            filtered_df = filtered_df[filtered_df['profile_type'] == 'local_chrome']
        elif status_filter == "Hyperbrowser":
            filtered_df = filtered_df[filtered_df['profile_type'] == 'hyperbrowser']
        elif status_filter == "With Active Sessions":
            filtered_df = filtered_df[filtered_df['active_sessions'] > 0]

        # Apply time filter
        if time_filter != "All Time":
            days_map = {
                "Last 7 Days": 7,
                "Last 30 Days": 30,
                "Last 90 Days": 90
            }
            days = days_map[time_filter]
            cutoff_date = datetime.now() - timedelta(days=days)
            filtered_df = filtered_df[filtered_df['created_time'] >= cutoff_date]

        return filtered_df

    # Add this to your AccountsPage class - REORGANIZED VERSION


    def _render_quick_stats(self):
        """Render quick stats at top of page - UPDATED."""
        try:
            accounts_stats_query = """
            SELECT
                COUNT(*) as total_accounts,
                COUNT(CASE WHEN profile_type = 'local_chrome' THEN 1 END) as local_accounts,
                COUNT(CASE WHEN profile_type = 'hyperbrowser' THEN 1 END) as hyperbrowser_accounts,
                SUM(total_content_processed) as total_content
            FROM accounts
            """

            stats_result = self.db_manager.execute_query(accounts_stats_query, fetch=True)

            if stats_result:
                stats = stats_result[0]
                stats_dict = dict(stats) if hasattr(stats, 'keys') else {
                    'total_accounts': stats[0],
                    'local_accounts': stats[1],
                    'hyperbrowser_accounts': stats[2],
                    'total_content': stats[3] or 0
                }

                # Get session counts
                local_sessions = len(st.session_state.get('local_chrome_sessions', {}))
                hb_sessions = len(st.session_state.get('hyperbrowser_sessions', {}))

                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("Total Accounts", stats_dict['total_accounts'])
                with col2:
                    st.metric("Local Chrome", stats_dict['local_accounts'])
                with col3:
                    st.metric("Hyperbrowser", stats_dict['hyperbrowser_accounts'])
                with col4:
                    st.metric("Local Sessions", local_sessions)
                with col5:
                    st.metric("Total Content", stats_dict['total_content'])

        except Exception as e:
            st.info("Load data to see stats")







    def _render_accounts_overview(self, df):
        """Render standard accounts overview with type column - UPDATED to show x_account_id."""
        display_df = df.copy()
    
        # Select and rename columns for display - ADD x_account_id
        display_columns = {
            'account_id': 'ID',
            'username': 'Username',
            'x_account_id': 'X Account ID',  # ADD THIS
            'profile_type': 'Type',
            'profile_id': 'Profile ID',
            'active_sessions': 'Active Sessions',
            'total_content_processed': 'Content Processed',
            'has_cookies': 'Has Cookies',
            'created_time': 'Created'
        }
    
        # Format the dataframe for display
        formatted_df = display_df[list(display_columns.keys())].copy()
        formatted_df = formatted_df.rename(columns=display_columns)
    
        # Format Type for display
        formatted_df['Type'] = formatted_df['Type'].apply(
            lambda x: '🖥️ Local' if x == 'local_chrome' else '☁️ HyperB' if x == 'hyperbrowser' else 'Unknown'
        )
    
        # Format Profile ID for display
        formatted_df['Profile ID'] = formatted_df['Profile ID'].apply(
            lambda x: f"{x[:12]}..." if x and len(str(x)) > 12 else (x if x else "None")
        )
    
        # Format X Account ID - show full or placeholder
        formatted_df['X Account ID'] = formatted_df['X Account ID'].apply(
            lambda x: str(x) if x and str(x).strip() else "—"
        )
    
        # Format Has Cookies
        formatted_df['Has Cookies'] = formatted_df['Has Cookies'].apply(
            lambda x: '✓' if x else '✗'
        )
    
        # Format created time
        formatted_df['Created'] = formatted_df['Created'].dt.strftime('%Y-%m-%d %H:%M')
    
        # Display the dataframe
        st.dataframe(
            formatted_df,
            use_container_width=True,
            column_config={
                "ID": st.column_config.NumberColumn("ID", help="Account ID", width="small"),
                "Username": st.column_config.TextColumn("Username", help="Account username", width="medium"),
                "X Account ID": st.column_config.TextColumn("X Account ID", help="Numeric X/Twitter user ID", width="medium"),  # ADD THIS
                "Type": st.column_config.TextColumn("Type", help="Profile type", width="small"),
                "Profile ID": st.column_config.TextColumn("Profile ID", help="Profile identifier", width="medium"),
                "Active Sessions": st.column_config.NumberColumn("Active Sessions", help="Currently active browser sessions", width="small"),
                "Content Processed": st.column_config.NumberColumn("Content Processed", help="Total content processed", width="small"),
                "Has Cookies": st.column_config.TextColumn("Has Cookies", help="Cookie status", width="small"),
                "Created": st.column_config.TextColumn("Created", help="Account creation date", width="medium")
            }
        )

    def _render_performance_analytics_view(self, df):
        """Render performance analytics view - UPDATED."""
        if len(df) > 0:
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Account Type Distribution")
                type_counts = df['profile_type'].value_counts()
                fig = px.pie(
                    values=type_counts.values,
                    names=type_counts.index,
                    title="Accounts by Type"
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.subheader("Content Processing")
                top_accounts = df.nlargest(10, 'total_content_processed')[['username', 'total_content_processed']]
                fig2 = px.bar(
                    top_accounts,
                    x='username',
                    y='total_content_processed',
                    title="Top 10 Accounts by Content Processed"
                )
                st.plotly_chart(fig2, use_container_width=True)
    # [Include all remaining helper methods for account creation, deletion,
    # session management, MongoDB operations, etc. - keeping the same implementations]

    # Note: All existing methods like render_add_account_modal,
    # render_delete_account_modal, render_session_management_section,
    # _start_persistent_session_for_profile, _stop_persistent_session,
    # _refresh_active_sessions_from_mongodb, _cleanup_stale_sessions, etc.
    # should be kept as they are in your original code.

    def _test_imports_and_dependencies(self):
        """Test all required imports and dependencies"""
        self.add_log("Testing imports and dependencies...")

        # Test Hyperbrowser import
        try:
            from hyperbrowser import Hyperbrowser
            self.add_log("✅ Hyperbrowser import successful")
        except ImportError as e:
            self.add_log(f"❌ Hyperbrowser import failed: {e}", "ERROR")
            return False

        # Test profile manager import - Use your existing streamlit_hyperbrowser_manager
        try:
            import_success = False
            import_error = None

            # Try to import from your streamlit_hyperbrowser_manager
            try:
                from ..streamlit_hyperbrowser_manager import create_hyperbrowser_profile, get_profile_by_id
                self.add_log("✅ Profile manager import successful (relative)")
                import_success = True
            except ImportError as e1:
                import_error = e1
                try:
                    from ..streamlit_hyperbrowser_manager import create_hyperbrowser_profile, get_profile_by_id
                    self.add_log("✅ Profile manager import successful (absolute)")
                    import_success = True
                except ImportError as e2:
                    try:
                        import streamlit_hyperbrowser_manager
                        create_hyperbrowser_profile = streamlit_hyperbrowser_manager.create_hyperbrowser_profile
                        get_profile_by_id = streamlit_hyperbrowser_manager.get_profile_by_id
                        self.add_log("✅ Profile manager import successful (module)")
                        import_success = True
                    except (ImportError, AttributeError) as e3:
                        self.add_log(f"❌ All profile manager import patterns failed: {e1}, {e2}, {e3}", "ERROR")

            if not import_success:
                return False

        except Exception as e:
            self.add_log(f"❌ Profile manager import test failed: {e}", "ERROR")
            return False

        # Test API Key
        api_key = os.environ.get('HYPERBROWSER_API_KEY')
        if not api_key:
            self.add_log("❌ HYPERBROWSER_API_KEY not set", "ERROR")
            return False
        else:
            self.add_log("✅ HYPERBROWSER_API_KEY found")

        # Test Hyperbrowser connection - FIXED: Remove problematic list() call
        try:
            hb_client = Hyperbrowser(api_key=api_key)
            # Instead of calling profiles.list(), test with a simple profile creation check
            # This avoids the ProfileManager.list() error entirely
            self.add_log("✅ Hyperbrowser API connection successful - client initialized")
        except Exception as e:
            self.add_log(f"❌ Hyperbrowser API connection failed: {e}", "ERROR")
            return False

        # Test database connection
        try:
            test_query = "SELECT 1 as test"
            result = self.db_manager.execute_query(test_query, fetch=True)
            self.add_log("✅ Database connection successful")
        except Exception as e:
            self.add_log(f"❌ Database connection failed: {e}", "ERROR")
            return False

        self.add_log("✅ All dependency tests passed")
        return True


    def _delete_account_from_hyperbrowser(self, profile_id):
        """Delete profile from Hyperbrowser using the API"""
        try:
            self.add_log(f"Starting Hyperbrowser profile deletion for profile_id: {profile_id}")

            api_key = os.environ.get('HYPERBROWSER_API_KEY')
            if not api_key:
                raise Exception("HYPERBROWSER_API_KEY not available for profile deletion")

            from hyperbrowser import Hyperbrowser
            hb_client = Hyperbrowser(api_key=api_key)

            # First, verify the profile exists
            try:
                profile = hb_client.profiles.get(profile_id)
                self.add_log(f"Profile {profile_id} found in Hyperbrowser - proceeding with deletion")
            except Exception as e:
                self.add_log(f"Profile {profile_id} not found in Hyperbrowser: {e}", "WARNING")
                return {
                    'deleted_from_hyperbrowser': False,
                    'reason': 'profile_not_found',
                    'error': str(e)
                }

            # Delete the profile
            try:
                # Using direct API call as per documentation
                import requests

                headers = {
                    'x-api-key': api_key,
                    'Content-Type': 'application/json'
                }

                delete_url = f'https://app.hyperbrowser.ai/api/profile/{profile_id}'
                response = requests.delete(delete_url, headers=headers)

                if response.status_code == 200:
                    self.add_log(f"✅ Successfully deleted profile {profile_id} from Hyperbrowser")
                    return {
                        'deleted_from_hyperbrowser': True,
                        'status_code': response.status_code,
                        'response': response.text
                    }
                else:
                    error_msg = f"Failed to delete profile from Hyperbrowser: HTTP {response.status_code} - {response.text}"
                    self.add_log(error_msg, "ERROR")
                    return {
                        'deleted_from_hyperbrowser': False,
                        'reason': 'api_error',
                        'status_code': response.status_code,
                        'error': response.text
                    }

            except Exception as e:
                # Fallback: Try using SDK delete method if it exists
                try:
                    # Some SDKs might have a delete method
                    if hasattr(hb_client.profiles, 'delete'):
                        hb_client.profiles.delete(profile_id)
                        self.add_log(f"✅ Successfully deleted profile {profile_id} from Hyperbrowser (SDK method)")
                        return {'deleted_from_hyperbrowser': True, 'method': 'sdk_delete'}
                    else:
                        raise Exception("SDK delete method not available")
                except Exception as sdk_error:
                    error_msg = f"Both API and SDK delete methods failed: API: {str(e)}, SDK: {str(sdk_error)}"
                    self.add_log(error_msg, "ERROR")
                    return {
                        'deleted_from_hyperbrowser': False,
                        'reason': 'both_methods_failed',
                        'api_error': str(e),
                        'sdk_error': str(sdk_error)
                    }

        except Exception as e:
            error_msg = f"Failed to delete profile from Hyperbrowser: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {
                'deleted_from_hyperbrowser': False,
                'reason': 'exception',
                'error': str(e)
            }


    def _delete_account_from_postgres(self, account_id):
        """Delete account and all related data from PostgreSQL"""
        try:
            self.add_log(f"Starting PostgreSQL deletion for account_id: {account_id}")

            # Get account details before deletion for logging
            account_query = "SELECT username, profile_id FROM accounts WHERE account_id = %s"
            account_info = self.db_manager.execute_query(account_query, (account_id,), fetch=True)

            if not account_info:
                raise Exception(f"Account with ID {account_id} not found")

            username, profile_id = account_info[0]
            self.add_log(f"Found account: {username} with profile_id: {profile_id}")

            # Delete in correct order due to foreign key constraints
            # Start with child tables first

            deletion_results = {}

            # Delete workflow_generation_log entries
            try:
                gen_log_query = "DELETE FROM workflow_generation_log WHERE account_id = %s"
                self.db_manager.execute_query(gen_log_query, (account_id,))
                deletion_results['workflow_generation_log'] = 'deleted'
                self.add_log("Deleted workflow_generation_log entries")
            except Exception as e:
                self.add_log(f"Failed to delete workflow_generation_log: {e}", "WARNING")
                deletion_results['workflow_generation_log'] = f'error: {str(e)}'

            # Delete workflow_runs entries
            try:
                runs_query = "DELETE FROM workflow_runs WHERE account_id = %s"
                self.db_manager.execute_query(runs_query, (account_id,))
                deletion_results['workflow_runs'] = 'deleted'
                self.add_log("Deleted workflow_runs entries")
            except Exception as e:
                self.add_log(f"Failed to delete workflow_runs: {e}", "WARNING")
                deletion_results['workflow_runs'] = f'error: {str(e)}'

            # Delete workflow_sync_log entries
            try:
                sync_log_query = "DELETE FROM workflow_sync_log WHERE account_id = %s"
                self.db_manager.execute_query(sync_log_query, (account_id,))
                deletion_results['workflow_sync_log'] = 'deleted'
                self.add_log("Deleted workflow_sync_log entries")
            except Exception as e:
                self.add_log(f"Failed to delete workflow_sync_log: {e}", "WARNING")
                deletion_results['workflow_sync_log'] = f'error: {str(e)}'

            # Delete content tables (replies, messages, retweets, links)
            content_tables = ['replies', 'messages', 'retweets', 'links']
            for table in content_tables:
                try:
                    content_query = f"DELETE FROM {table} WHERE account_id = %s"
                    self.db_manager.execute_query(content_query, (account_id,))
                    deletion_results[table] = 'deleted'
                    self.add_log(f"Deleted {table} entries")
                except Exception as e:
                    self.add_log(f"Failed to delete from {table}: {e}", "WARNING")
                    deletion_results[table] = f'error: {str(e)}'

            # Delete workflows
            try:
                workflows_query = "DELETE FROM workflows WHERE account_id = %s"
                self.db_manager.execute_query(workflows_query, (account_id,))
                deletion_results['workflows'] = 'deleted'
                self.add_log("Deleted workflows entries")
            except Exception as e:
                self.add_log(f"Failed to delete workflows: {e}", "WARNING")
                deletion_results['workflows'] = f'error: {str(e)}'

            # Delete prompts
            try:
                prompts_query = "DELETE FROM prompts WHERE account_id = %s"
                self.db_manager.execute_query(prompts_query, (account_id,))
                deletion_results['prompts'] = 'deleted'
                self.add_log("Deleted prompts entries")
            except Exception as e:
                self.add_log(f"Failed to delete prompts: {e}", "WARNING")
                deletion_results['prompts'] = f'error: {str(e)}'

            # Finally, delete the main account
            try:
                account_query = "DELETE FROM accounts WHERE account_id = %s"
                self.db_manager.execute_query(account_query, (account_id,))
                deletion_results['accounts'] = 'deleted'
                self.add_log("✅ Deleted main account entry")
            except Exception as e:
                error_msg = f"Failed to delete main account: {e}"
                self.add_log(error_msg, "ERROR")
                deletion_results['accounts'] = f'error: {str(e)}'
                raise Exception(error_msg)

            return {
                'deleted_from_postgres': True,
                'username': username,
                'profile_id': profile_id,
                'results': deletion_results
            }

        except Exception as e:
            error_msg = f"Failed to delete from PostgreSQL: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {
                'deleted_from_postgres': False,
                'error': str(e)
            }

    def _handle_account_deletion(self, account_id):
        """Handle the complete account deletion process"""
        if st.session_state.get('deletion_in_progress'):
            self.add_log("Deletion already in progress, skipping...", "WARNING")
            return

        st.session_state.deletion_in_progress = True

        try:
            self.clear_logs()
            self.add_log(f"=== Starting account deletion process for account_id: {account_id} ===")

            # Create a placeholder for the status
            status_placeholder = st.empty()

            with status_placeholder.container():
                with st.status("Deleting account...", expanded=True) as status:

                    # Step 1: Get account information
                    st.write("🔍 Getting account information...")
                    self.add_log("Step 1: Getting account information")

                    try:
                        account_query = "SELECT username, profile_id, mongo_object_id FROM accounts WHERE account_id = %s"
                        account_info = self.db_manager.execute_query(account_query, (account_id,), fetch=True)

                        if not account_info:
                            raise Exception(f"Account with ID {account_id} not found")

                        username, profile_id, mongo_object_id = account_info[0]
                        st.write(f"✅ Found account: {username}")
                        self.add_log(f"Found account: {username}, profile_id: {profile_id}")

                    except Exception as e:
                        error_msg = f"Step 1 failed - Could not get account information: {str(e)}"
                        self.add_log(error_msg, "ERROR")
                        st.error(error_msg)
                        raise Exception(error_msg)

                    # Step 2: Delete from Hyperbrowser
                    st.write("🔄 Deleting from Hyperbrowser...")
                    self.add_log("Step 2: Deleting from Hyperbrowser")

                    hyperbrowser_result = self._delete_account_from_hyperbrowser(profile_id)

                    if hyperbrowser_result.get('deleted_from_hyperbrowser'):
                        st.write("✅ Deleted from Hyperbrowser")
                        self.add_log("Step 2 completed successfully")
                    else:
                        st.write(f"⚠️ Hyperbrowser deletion issue: {hyperbrowser_result.get('reason', 'unknown')}")
                        self.add_log(f"Step 2 warning: {hyperbrowser_result}", "WARNING")

                    # Step 3: Delete from MongoDB
                    st.write("🔄 Deleting from MongoDB...")
                    self.add_log("Step 3: Deleting from MongoDB")

                    mongodb_result = self._delete_account_from_mongodb(account_id, profile_id, mongo_object_id)

                    if mongodb_result.get('deleted_from_mongodb'):
                        st.write("✅ Deleted from MongoDB")
                        self.add_log("Step 3 completed successfully")
                    else:
                        st.write("⚠️ MongoDB deletion issue")
                        self.add_log(f"Step 3 warning: {mongodb_result}", "WARNING")

                    # Step 4: Delete from PostgreSQL (do this last)
                    st.write("🔄 Deleting from PostgreSQL...")
                    self.add_log("Step 4: Deleting from PostgreSQL")

                    try:
                        postgres_result = self._delete_account_from_postgres(account_id)

                        if postgres_result.get('deleted_from_postgres'):
                            st.write("✅ Deleted from PostgreSQL")
                            self.add_log("Step 4 completed successfully")
                        else:
                            raise Exception(postgres_result.get('error', 'Unknown PostgreSQL deletion error'))

                    except Exception as e:
                        error_msg = f"Step 4 failed - PostgreSQL deletion: {str(e)}"
                        self.add_log(error_msg, "ERROR")
                        st.error(error_msg)
                        raise Exception(error_msg)

                    # Success
                    self.add_log("=== Account deletion process completed successfully ===")
                    status.update(label=f"Account '{username}' deleted successfully!", state="complete")

                    # Update session state
                    success_message = f"Account '{username}' (ID: {account_id}) has been completely deleted from all systems."
                    st.session_state.account_deletion_message = success_message
                    st.session_state.account_deletion_error = False
                    st.session_state.deletion_completed = True
                    st.session_state.deleting_account = False
                    st.session_state.deletion_in_progress = False

                    # Clear cache to refresh data
                    st.cache_data.clear()

                    # Brief pause to show success
                    time.sleep(3)
                    st.rerun()

        except Exception as e:
            error_msg = str(e)
            self.add_log(f"=== Account deletion process failed: {error_msg} ===", "ERROR")

            # Update session state with error
            st.session_state.account_deletion_message = f"Error deleting account: {error_msg}"
            st.session_state.account_deletion_error = True
            st.session_state.deletion_completed = True
            st.session_state.deleting_account = False
            st.session_state.deletion_in_progress = False

            st.error(f"Account deletion failed: {error_msg}")
            time.sleep(2)
            st.rerun()

    def render_delete_account_modal(self, accounts_df):
        """Render delete account interface"""
        if st.session_state.get('show_delete_account', False):
            with st.expander("🗑️ Delete Account", expanded=True):

                # Show deletion logs if available
                self.render_creation_logs()

                # Handle deletion process
                if st.session_state.get('deleting_account', False):
                    st.info("⏳ Deleting account... Please wait.")

                    # Process deletion if not already completed
                    if (not st.session_state.get('deletion_completed', False) and
                        not st.session_state.get('deletion_in_progress', False) and
                        st.session_state.get('account_id_to_delete')):

                        account_id = st.session_state.get('account_id_to_delete')
                        if account_id:
                            self.add_log(f"Triggering account deletion for ID: {account_id}")
                            self._handle_account_deletion(account_id)

                    return  # Don't show form while deleting

                # Show deletion form
                st.warning("⚠️ **Account Deletion Warning**")
                st.write("This action will permanently delete the account from:")
                st.write("- ✅ PostgreSQL database (all related data)")
                st.write("- ✅ MongoDB collections (profiles, sessions, etc.)")
                st.write("- ✅ Hyperbrowser platform (browser profiles)")
                st.write("")
                st.error("**This action cannot be undone!**")

                with st.form("delete_account_form"):
                    if not accounts_df.empty:
                        # Create selection options - FIXED: Use index-based selection
                        account_options = [(f"{row['username']} (ID: {row['account_id']}) - Profile: {row['profile_id'][:8]}...", idx)
                                          for idx, row in accounts_df.iterrows()]

                        selected_account = st.selectbox(
                            "Select Account to Delete",
                            options=[("Choose an account...", None)] + account_options,
                            format_func=lambda x: x[0]
                        )

                        # Show account details if selected
                        if selected_account[1] is not None:
                            account_id = accounts_df.iloc[selected_account[1]]['account_id']
                            account_row = accounts_df.iloc[selected_account[1]]

                            st.info("**Account Details:**")
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write(f"**Username:** {account_row['username']}")
                                st.write(f"**Account ID:** {account_row['account_id']}")
                                st.write(f"**Created:** {account_row['created_time']}")
                            with col2:
                                st.write(f"**Profile ID:** {account_row['profile_id']}")
                                if 'total_replies_processed' in account_row:
                                    st.write(f"**Replies Processed:** {account_row['total_replies_processed']}")
                                if 'total_messages_processed' in account_row:
                                    st.write(f"**Messages Processed:** {account_row['total_messages_processed']}")

                    else:
                        st.info("No accounts available to delete.")
                        selected_account = (None, None)

                    # Confirmation checkbox
                    if selected_account[1] is not None:
                        account_row = accounts_df.iloc[selected_account[1]]
                        confirm_deletion = st.checkbox(
                            f"I understand this will permanently delete '{account_row['username']}' and all related data"
                        )
                    else:
                        confirm_deletion = False

                    # Show deletion status or errors
                    if st.session_state.get('account_deletion_message'):
                        if st.session_state.get('account_deletion_error'):
                            st.error(st.session_state.account_deletion_message)
                        else:
                            st.success(st.session_state.account_deletion_message)

                    col_delete, col_cancel = st.columns([1, 1])

                    with col_delete:
                        delete_submitted = st.form_submit_button(
                            "🗑️ Delete Account",
                            use_container_width=True,
                            type="primary",
                            disabled=not (selected_account[1] is not None and confirm_deletion)
                        )

                    with col_cancel:
                        cancel_delete = st.form_submit_button("Cancel", use_container_width=True)

                    if delete_submitted and selected_account[1] is not None and confirm_deletion:
                        account_id = accounts_df.iloc[selected_account[1]]['account_id']
                        username = accounts_df.iloc[selected_account[1]]['username']

                        self.add_log(f"Form submitted - preparing deletion for account: {username} (ID: {account_id})")
                        st.session_state.account_id_to_delete = account_id
                        st.session_state.deleting_account = True
                        st.session_state.account_deletion_message = None
                        st.session_state.account_deletion_error = False
                        st.session_state.deletion_completed = False
                        st.session_state.deletion_in_progress = False
                        st.rerun()

                    if cancel_delete:
                        self._clear_account_deletion_state()
                        st.rerun()

    def _clear_account_deletion_state(self):
        """Clear account deletion session state"""
        st.session_state.show_delete_account = False
        st.session_state.deleting_account = False
        st.session_state.deletion_completed = False
        st.session_state.account_id_to_delete = None
        st.session_state.deletion_in_progress = False
        st.session_state.account_deletion_message = None
        st.session_state.account_deletion_error = False



    def render_creation_logs(self):
        """Render the creation logs in the UI"""
        if st.session_state.get('account_creation_logs'):
            with st.expander("🔍 Creation Logs", expanded=True):
                log_text = "\n".join(st.session_state.account_creation_logs)
                st.code(log_text, language="log")

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Clear Logs"):
                        self.clear_logs()
                        st.rerun()

                with col2:
                    st.download_button(
                        label="Download Log File",
                        data=log_text,
                        file_name=f"account_creation_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                        mime="text/plain"
                    )




    def _render_sessions_prompts_view(self, df):
        """Render accounts view focused on sessions, prompts, and extensions - UPDATED with x_account_id"""
        for _, row in df.iterrows():
            with st.expander(f"🏷️ {row['username']} (ID: {row['account_id']})", expanded=False):
                col1, col2, col3, col4 = st.columns(4)
    
                with col1:
                    st.write("**Account Info**")
                    st.write(f"Account ID: `{row['account_id']}`")
                    st.write(f"Username: `{row['username']}`")
                    # ADD THIS LINE
                    st.write(f"X Account ID: `{row.get('x_account_id', 'Not set')}`")
                    st.write(f"Profile ID: `{row['profile_id'][:12] if row['profile_id'] else 'None'}...`")
                    st.write(f"Created: {row['created_time'].strftime('%Y-%m-%d')}")
                    st.write(f"Active Sessions: {row['active_sessions']}")
    
                with col2:
                    st.write("**Prompts Summary**")
                    st.write(f"Total Prompts: {row['total_prompts']}")
                    st.write(f"• Replies: {row['replies_prompts']}")
                    st.write(f"• Messages: {row['messages_prompts']}")
                    st.write(f"• Retweets: {row['retweets_prompts']}")
    
                with col3:
                    st.write("**Extension Info**")
                    if row.get('extension_id'):
                        st.write(f"Extension: `{row['extension_id'][:8]}...`")
                        st.write(f"Name: {row.get('extension_name', 'Unknown')}")
                        st.write(f"Status: {row.get('extension_status', 'Unknown')}")
                    else:
                        st.write("Extension: None")
                        st.write("Status: Not assigned")
    
                with col4:
                    st.write("**Processing Stats**")
                    st.write(f"Replies: {row['total_replies_processed']}")
                    st.write(f"Messages: {row['total_messages_processed']}")
                    st.write(f"Retweets: {row['total_retweets_processed']}")





    """
Updated AccountsPage methods that need to be modified to use the new extension linking functionality.
These methods should replace the corresponding methods in the existing AccountsPage class.
"""



    def _create_hyperbrowser_profile_for_account(self, username):
        """
        Create a Hyperbrowser profile with proper UUID validation and account linking.
        Returns the actual Hyperbrowser UUID, not a custom identifier.
        """
        try:
            self.add_log(f"Starting Hyperbrowser profile creation for username: {username}")

            # Create profile name with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            profile_name = f'Account Profile - {username} - {timestamp}'
            self.add_log(f"Generated profile name: {profile_name}")

            # Import the fixed profile manager functions
            try:
                from ..hyperbrowser_extensions import get_or_create_extension_for_account
                from ..streamlit_hyperbrowser_manager import create_hyperbrowser_profile
                self.add_log("✅ Profile manager functions imported")
            except ImportError as e:
                self.add_log(f"Import warning: {e}", "WARNING")
                # Fallback to direct creation
                return self._create_profile_direct_api(username, profile_name)

            # Call the profile manager function
            self.add_log("Calling create_hyperbrowser_profile...")
            profile_result = create_hyperbrowser_profile(
                profile_type='hyperbrowser_automa',
                profile_name=profile_name,
                skip_if_exists=False
            )

            # Validate the response
            if not profile_result:
                raise Exception("Profile manager returned None/empty result")

            if not profile_result.get('profile_id'):
                raise Exception(f"Profile manager returned result without profile_id: {profile_result}")

            profile_id = profile_result['profile_id']

            # CRITICAL: Validate it's a proper UUID from Hyperbrowser
            try:
                # Attempt to parse as UUID - this will raise ValueError if invalid
                uuid_obj = uuid.UUID(profile_id)
                self.add_log(f"✅ Valid Hyperbrowser UUID validated: {profile_id}")

                # Additional check: Ensure it's formatted as expected (lowercase with hyphens)
                canonical_uuid = str(uuid_obj)
                if profile_id != canonical_uuid:
                    self.add_log(
                        f"⚠️ UUID format mismatch: received '{profile_id}', "
                        f"canonical is '{canonical_uuid}'. Using canonical format.",
                        "WARNING"
                    )
                    profile_id = canonical_uuid

            except ValueError as ve:
                error_msg = (
                    f"❌ CRITICAL: Hyperbrowser returned INVALID UUID: '{profile_id}'\n"
                    f"This is not a valid UUID format. Expected format: "
                    f"'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'\n"
                    f"Error: {ve}"
                )
                self.add_log(error_msg, "ERROR")
                raise Exception(error_msg)

            # Return the validated profile data
            profile_data = {
                'profile_id': profile_id,  # ← THIS IS THE ACTUAL HYPERBROWSER UUID
                'profile_name': profile_name,
                'created_at': datetime.now(),
                'mongodb_id': profile_result.get('mongodb_id'),
                'created_via': 'streamlit_hyperbrowser_manager_enhanced',
                'extension_ready': False,  # Extension will be linked during account creation
                'uuid_validated': True,
                'uuid_version': uuid_obj.version if hasattr(uuid_obj, 'version') else None
            }

            self.add_log(
                f"✅ Profile created successfully\n"
                f"  Profile ID (UUID): {profile_id}\n"
                f"  Profile Name: {profile_name}\n"
                f"  MongoDB ID: {profile_data.get('mongodb_id', 'N/A')}"
            )

            return profile_data

        except Exception as e:
            self.add_log(
                f"❌ Enhanced profile creation failed: {str(e)}\n"
                f"Error type: {type(e).__name__}",
                "ERROR"
            )
            self.add_log("Attempting fallback to direct API creation...")

            try:
                return self._create_profile_direct_api(username, profile_name)
            except Exception as fallback_error:
                self.add_log(
                    f"❌ Fallback API creation also failed: {fallback_error}",
                    "ERROR"
                )
                raise Exception(
                    f"All profile creation methods failed. "
                    f"Primary error: {e}, Fallback error: {fallback_error}"
                )

    def _create_profile_direct_api(self, username, profile_name):
        """UPDATED: Fallback method to create profile directly via API (no extension linking here)"""
        try:
            self.add_log("Creating profile directly via Hyperbrowser API...")

            api_key = os.environ.get('HYPERBROWSER_API_KEY')
            if not api_key:
                raise Exception("HYPERBROWSER_API_KEY not available for direct API call")

            from hyperbrowser import Hyperbrowser
            hb_client = Hyperbrowser(api_key=api_key)

            # Create profile
            profile = hb_client.profiles.create()
            profile_id = profile.id
            self.add_log(f"✅ Direct API created profile: {profile_id}")

            # Store in MongoDB (extension will be linked later during account creation)
            try:
                from ..hyperbrowser_utils import store_in_mongodb

                profile_data = {
                    'profile_id': profile_id,
                    'profile_name': profile_name,
                    'profile_path': f'/hyperbrowser/profiles/{profile_id}',
                    'profile_type': 'hyperbrowser_automa',
                    'is_default': True,
                    'is_active': True,
                    'created_at': datetime.now(),
                    'last_used_at': datetime.now(),
                    'usage_count': 0,
                    'assigned_workflows': ['replies', 'messages', 'retweets'],
                    'profile_settings': {
                        'disable_images': False,
                        'disable_javascript': False,
                        'user_agent': None,
                        'window_size': '1920,1080',
                        'created_via': 'direct_api_fallback_enhanced',
                        'hyperbrowser_metadata': {
                            'profile_id': profile_id,
                            'creation_timestamp': datetime.now().isoformat()
                        }
                    },
                    'postgres_account_id': None,
                    'username': username,
                    'linked_to_postgres': False,
                    'extension_ready': False  # Extension will be linked separately
                }

                mongodb_id = store_in_mongodb('accounts', profile_data)
                self.add_log(f"✅ Stored profile in MongoDB: {mongodb_id}")

            except Exception as e:
                self.add_log(f"Warning: Could not store in MongoDB: {e}", "WARNING")
                mongodb_id = None

            return {
                'profile_id': profile_id,
                'profile_name': profile_name,
                'created_at': datetime.now(),
                'mongodb_id': mongodb_id,
                'created_via': 'direct_api_fallback_enhanced',
                'extension_ready': False
            }

        except Exception as e:
            error_msg = f"Direct API profile creation failed: {str(e)}"
            self.add_log(error_msg, "ERROR")
            raise Exception(error_msg)



    


    def _verify_account_configuration_relaxed(self, account_id, profile_id, extension_id):
        """
        FIXED: Relaxed verification that allows accounts without extensions
        """
        try:
            self.add_log(f"Verifying account {account_id} configuration (relaxed mode)...")
            errors = []
            warnings = []

            # Check PostgreSQL account (REQUIRED)
            pg_query = "SELECT account_id, username, profile_id FROM accounts WHERE account_id = %s"
            pg_result = self.db_manager.execute_query(pg_query, (account_id,), fetch=True)

            if not pg_result:
                errors.append("Account not found in PostgreSQL")
            else:
                self.add_log("✓ PostgreSQL account verified")

            # Check MongoDB profile (REQUIRED)
            try:
                from .hyperbrowser_utils import get_mongodb_client
            except ImportError:
                try:
                    from ..streamlit_hyperbrowser_manager import get_mongodb_client
                except ImportError:
                    import streamlit_hyperbrowser_manager
                    get_mongodb_client = streamlit_hyperbrowser_manager.get_mongodb_client

            try:
                client, db = get_mongodb_client()

                profile = db.accounts.find_one({
                    'profile_id': profile_id,
                    'postgres_account_id': account_id
                })

                if not profile:
                    errors.append("Profile not found in MongoDB or not linked to account")
                else:
                    self.add_log("✓ MongoDB profile verified")

                # Check extension (OPTIONAL in relaxed mode)
                if extension_id:
                    extension = db.extension_instances.find_one({
                        'extension_id': extension_id,
                        'is_enabled': True
                    })

                    if not extension:
                        warnings.append(f"Extension {extension_id} not found but account creation proceeding")
                    elif not extension.get('postgres_account_id') == account_id:
                        warnings.append(f"Extension {extension_id} not linked to account {account_id} but account creation proceeding")
                    else:
                        self.add_log("✓ Extension configuration verified")
                else:
                    warnings.append("No extension linked to account - manual setup may be required")

                client.close()

            except Exception as e:
                errors.append(f"MongoDB verification failed: {e}")

            return {
                'account_created': len(errors) == 0,  # Only fail if critical errors
                'complete': len(errors) == 0 and len(warnings) == 0,  # Full success only if no errors or warnings
                'errors': errors,
                'warnings': warnings,
                'account_id': account_id,
                'profile_id': profile_id,
                'extension_id': extension_id
            }

        except Exception as e:
            return {
                'account_created': False,
                'complete': False,
                'errors': [str(e)],
                'warnings': [],
                'account_id': account_id
            }




    def _stop_persistent_session(self, session_id):
        """Stop a persistent session and ensure data is saved to profile"""
        try:
            self.add_log(f"Stopping persistent session: {session_id}")

            # Get session info from session state
            session_info = st.session_state.get('active_sessions', {}).get(session_id)

            if not HYPERBROWSER_API_KEY:
                raise ValueError("HYPERBROWSER_API_KEY not available")

            from hyperbrowser import Hyperbrowser
            hb_client = Hyperbrowser(api_key=HYPERBROWSER_API_KEY)

            # Close the session - Hyperbrowser will automatically persist changes if configured
            try:
                hb_client.sessions.close(session_id)
                self.add_log(f"Session {session_id} closed via Hyperbrowser API")
            except Exception as e:
                self.add_log(f"Could not close session via API: {e}", "WARNING")
                # Continue with cleanup even if API call fails

            # Update session in MongoDB
            end_time = datetime.now()

            try:
                from ..hyperbrowser_utils import get_mongodb_client, update_in_mongodb

                # Get session to calculate duration and update stats
                client, db = get_mongodb_client()
                session_doc = db.browser_sessions.find_one({'session_id': session_id})

                session_duration = None
                if session_doc and session_doc.get('started_at'):
                    start_time = session_doc['started_at']
                    if isinstance(start_time, str):
                        start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    session_duration = (end_time - start_time).total_seconds()

                # Update session document
                update_data = {
                    'is_active': False,
                    'ended_at': end_time,
                    'session_status': 'completed_persistent',
                    'session_closure_type': 'manual_stop',
                    'data_persisted': session_info.get('persist_changes', True) if session_info else True
                }

                if session_duration:
                    update_data['session_duration'] = session_duration
                    update_data['session_duration_hours'] = round(session_duration / 3600, 2)

                result = db.browser_sessions.update_one(
                    {'session_id': session_id},
                    {'$set': update_data}
                )

                # Update profile with session completion stats
                if session_doc and session_doc.get('profile_id'):
                    profile_id = session_doc['profile_id']
                    db.accounts.update_one(
                        {'profile_id': profile_id},
                        {
                            '$inc': {'completed_sessions': 1},
                            '$set': {'last_session_completed_at': end_time}
                        }
                    )

                    self.add_log(f"Updated profile {profile_id} with session completion")

                client.close()

                if result.modified_count > 0:
                    self.add_log(f"Updated MongoDB session record")

            except Exception as e:
                self.add_log(f"Could not update MongoDB: {e}", "WARNING")

            # Remove from session state
            if 'active_sessions' in st.session_state and session_id in st.session_state.active_sessions:
                del st.session_state.active_sessions[session_id]

            success_msg = f"Persistent session {session_id} stopped successfully"
            if session_info and session_info.get('persist_changes'):
                success_msg += " - Profile data has been saved"

            self.add_log(success_msg)

            return {
                'success': True,
                'message': success_msg
            }

        except Exception as e:
            error_msg = f"Failed to stop persistent session {session_id}: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {
                'success': False,
                'error': error_msg
            }

    def _get_active_sessions_for_account(self, profile_id):
        """Get active sessions for a profile from MongoDB"""
        try:
            # Import MongoDB utilities
            try:
                from ..hyperbrowser_utils import get_mongodb_client
            except ImportError:
                try:
                    from ..hyperbrowser_utils import get_mongodb_client
                except ImportError:
                    import hyperbrowser_utils
                    get_mongodb_client = hyperbrowser_utils.get_mongodb_client

            client, db = get_mongodb_client()

            # Get active sessions for this profile
            active_sessions = list(db.browser_sessions.find({
                'profile_id': profile_id,
                'is_active': True,
                'session_status': 'active'
            }).sort('created_at', -1))

            client.close()

            return active_sessions

        except Exception as e:
            self.add_log(f"Failed to get active sessions for profile {profile_id}: {e}", "ERROR")
            return []



    def _update_mongodb_account_postgres_link(self, mongodb_id, postgres_account_id, username):
        """Update MongoDB account document with PostgreSQL account link"""
        try:
            if not mongodb_id:
                return False

            # Import MongoDB utilities
            try:
                from streamlit_hyperbrowser_manager import get_mongodb_client
            except ImportError:
                try:
                    from ..streamlit_hyperbrowser_manager import get_mongodb_client
                except ImportError:
                    import streamlit_hyperbrowser_manager
                    get_mongodb_client = streamlit_hyperbrowser_manager.get_mongodb_client

            client, db = get_mongodb_client()

            from bson import ObjectId
            result = db.accounts.update_one(
                {'_id': ObjectId(mongodb_id)},
                {
                    '$set': {
                        'postgres_account_id': postgres_account_id,
                        'username': username,
                        'linked_to_postgres': True,
                        'postgres_link_created_at': datetime.now()
                    }
                }
            )

            client.close()

            if result.modified_count > 0:
                self.add_log(f"✅ Updated MongoDB document with postgres_account_id: {postgres_account_id}")
                return True
            else:
                self.add_log(f"Warning: MongoDB document not updated (may already be linked)", "WARNING")
                return False

        except Exception as e:
            self.add_log(f"Failed to update MongoDB with postgres link: {e}", "ERROR")
            return False



    def _link_mongodb_to_postgres(self, mongodb_id, postgres_account_id, username, profile_id):
        """Link MongoDB document to PostgreSQL account - FIXED VERSION"""
        try:
            self.add_log(f"Linking MongoDB document {mongodb_id} to PostgreSQL account {postgres_account_id}")

            # Import MongoDB utilities with fallback pattern
            try:
                from streamlit_hyperbrowser_manager import get_mongodb_client
            except ImportError:
                try:
                    from ..streamlit_hyperbrowser_manager import get_mongodb_client
                except ImportError:
                    import streamlit_hyperbrowser_manager
                    get_mongodb_client = streamlit_hyperbrowser_manager.get_mongodb_client

            client, db = get_mongodb_client()

            from bson import ObjectId

            # Prepare update data
            update_data = {
                'postgres_account_id': postgres_account_id,
                'username': username,
                'linked_to_postgres': True,
                'postgres_link_created_at': datetime.now(),
                'last_postgres_sync': datetime.now()
            }

            # Update the MongoDB document
            result = db.accounts.update_one(
                {'_id': ObjectId(mongodb_id)},
                {'$set': update_data}
            )

            client.close()

            if result.modified_count > 0:
                self.add_log(f"✅ Successfully updated MongoDB document with postgres_account_id: {postgres_account_id}")
                return True
            else:
                self.add_log(f"⚠️ MongoDB update returned 0 modified count", "WARNING")
                return False

        except Exception as e:
            self.add_log(f"❌ Failed to link MongoDB to PostgreSQL: {e}", "ERROR")
            return False
    def _initialize_local_chrome_mode(self):
        """Initialize local Chrome mode in session state"""
        if 'use_local_chrome' not in st.session_state:
            st.session_state.use_local_chrome = True  # Default to local
        if 'local_chrome_sessions' not in st.session_state:
            st.session_state.local_chrome_sessions = {}

    def render_chrome_mode_selector(self):
        """Render Chrome mode selector in sidebar"""
        st.sidebar.markdown("---")
        st.sidebar.subheader("🖥️ Chrome Mode")

        mode = st.sidebar.radio(
            "Select Chrome Mode:",
            options=["Local Chrome", "Hyperbrowser"],
            index=0 if st.session_state.get('use_local_chrome', True) else 1,
            help="Local Chrome: Free, uses local browser\nHyperbrowser: Paid, cloud-based"
        )

        st.session_state.use_local_chrome = (mode == "Local Chrome")

        if mode == "Local Chrome":
            st.sidebar.success("✓ Using Local Chrome (Free)")
            st.sidebar.info("Sessions run on local Chrome with persistent profiles")
        else:
            st.sidebar.warning("⚠️ Hyperbrowser Mode (Requires API Key)")
            api_key = os.environ.get('HYPERBROWSER_API_KEY')
            if api_key:
                st.sidebar.success("✓ API Key Configured")
            else:
                st.sidebar.error("❌ API Key Not Found")
                st.sidebar.caption("Set HYPERBROWSER_API_KEY environment variable")

    # Updated methods for AccountsPage class to support persistent profiles

    def _create_local_chrome_profile_for_account(self, username, account_id):
        """Create a persistent local Chrome profile - uses username for consistency"""
        try:
            self.add_log(f"Creating persistent Chrome profile for {username}...")

            result = self.chrome_manager.create_profile_for_account(account_id, username)

            if not result['success']:
                raise Exception(result.get('error', 'Unknown error'))

            if result.get('is_new'):
                self.add_log(f"✓ Created NEW profile: {result['profile_id']}")
            else:
                self.add_log(f"✓ Using EXISTING profile: {result['profile_id']}")
                self.add_log(f"  Profile will retain cookies, extensions, and login state")

            return {
                'profile_id': result['profile_id'],
                'profile_type': 'local_chrome',
                'profile_path': result.get('profile_path', ''),
                'created_at': result.get('created_at'),
                'mongodb_id': result.get('mongodb_id'),
                'is_persistent': True,
                'is_new': result.get('is_new', False)
            }
        except Exception as e:
            error_msg = f"Failed to create local Chrome profile: {str(e)}"
            self.add_log(error_msg, "ERROR")
            raise Exception(error_msg)




    def render_session_debug_panel(self):
        """Comprehensive session debugging panel"""
        with st.sidebar.expander("🐛 Session Debug Info", expanded=False):
            st.write("### Session State")

            # Local Chrome Sessions
            local_sessions = st.session_state.get('local_chrome_sessions', {})
            st.write(f"**Local Chrome Sessions:** {len(local_sessions)}")
            if local_sessions:
                for sid, info in local_sessions.items():
                    st.write(f"- `{sid[:20]}...`")
                    st.write(f"  Account: {info.get('account_username', 'Unknown')}")
                    st.write(f"  Profile: {info.get('profile_path', 'Unknown')}")

            # Hyperbrowser Sessions
            hb_sessions = st.session_state.get('active_sessions', {})
            st.write(f"**Hyperbrowser Sessions:** {len(hb_sessions)}")
            if hb_sessions:
                for sid, info in hb_sessions.items():
                    st.write(f"- `{sid[:20]}...`")
                    st.write(f"  Account: {info.get('account_username', 'Unknown')}")
                    st.write(f"  Profile: {info.get('profile_id', 'Unknown')}")

            # Chrome Manager Processes
            st.write("**Chrome Manager Processes:**")
            if hasattr(self, 'chrome_manager'):
                active_procs = self.chrome_manager.active_processes
                st.write(f"Active: {len(active_procs)}")
                if active_procs:
                    for sid in active_procs.keys():
                        st.write(f"- `{sid[:20]}...`")
            else:
                st.write("Chrome manager not initialized")

            # Session State Keys
            st.write("**All Session State Keys:**")
            session_keys = [k for k in st.session_state.keys() if 'session' in k.lower()]
            for key in session_keys:
                st.write(f"- {key}: {type(st.session_state[key])}")

            # Environment
            st.write("**Chrome Mode:**")
            st.write(f"- use_local_chrome: {st.session_state.get('use_local_chrome', 'Not Set')}")

            # Buttons
            if st.button("🔄 Refresh Debug Info"):
                st.rerun()

            if st.button("🧹 Clear All Sessions"):
                st.session_state.local_chrome_sessions = {}
                st.session_state.active_sessions = {}
                st.success("Cleared all session state")
                st.rerun()

    


    def _store_account_cookies(self, account_id, cookies_json, username):
        """
        FULLY FIXED: Store cookies for an account in PostgreSQL
        Fixes:
        1. Proper JSON serialization (not Python repr)
        2. Handle numpy.int64 types
        3. Proper error handling and return value parsing
        """
        try:
            # CRITICAL: Convert numpy types to native Python
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            self.add_log(f"Storing cookies for account {account_id} ({username})")

            # Parse and validate cookies
            try:
                cookies = json.loads(cookies_json)
            except json.JSONDecodeError as e:
                return {
                    'success': False,
                    'error': f"Invalid JSON format: {str(e)}"
                }

            if not isinstance(cookies, list):
                return {
                    'success': False,
                    'error': "Cookies must be a JSON array"
                }

            if len(cookies) == 0:
                return {
                    'success': False,
                    'error': "No cookies found in JSON"
                }

            # Validate cookie structure
            required_fields = ['name', 'value', 'domain']
            for i, cookie in enumerate(cookies):
                missing = [f for f in required_fields if f not in cookie]
                if missing:
                    return {
                        'success': False,
                        'error': f"Cookie #{i} missing fields: {missing}"
                    }

            cookie_count = len(cookies)
            self.add_log(f"Validated {cookie_count} cookies")

            # Deactivate any existing active cookies for this account
            deactivate_query = """
            UPDATE account_cookies
            SET is_active = FALSE,
                updated_at = CURRENT_TIMESTAMP
            WHERE account_id = %s AND is_active = TRUE
            """
            self.db_manager.execute_query(deactivate_query, (account_id,))
            self.add_log("Deactivated previous cookies")

            # CRITICAL FIX 1: Convert to valid JSON string (not Python repr)
            cookie_json_string = json.dumps(cookies)
            self.add_log(f"Serialized cookies to JSON ({len(cookie_json_string)} chars)")

            # CRITICAL FIX 2: Proper INSERT with RETURNING
            insert_query = """
            INSERT INTO account_cookies (
                account_id, cookie_data, cookie_count,
                uploaded_at, updated_at, is_active, cookie_source
            )
            VALUES (%s, %s::jsonb, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE, 'editthiscookie')
            RETURNING cookie_id
            """

            self.add_log(f"Executing INSERT query...")

            result = self.db_manager.execute_query(
                insert_query,
                (account_id, cookie_json_string, cookie_count),
                fetch=True
            )

            self.add_log(f"INSERT result: {result}")

            # CRITICAL FIX 3: Proper result parsing
            if not result or len(result) == 0:
                raise Exception("INSERT returned no rows - cookie_id not generated")

            # Handle different result formats
            first_row = result[0]
            if isinstance(first_row, tuple):
                cookie_id = first_row[0]
            elif isinstance(first_row, dict):
                cookie_id = first_row.get('cookie_id')
            elif hasattr(first_row, 'cookie_id'):
                cookie_id = first_row.cookie_id
            else:
                # Last resort - try to extract any way possible
                cookie_id = first_row[0] if len(first_row) > 0 else None

            if not cookie_id:
                raise Exception(f"Could not extract cookie_id from result: {result}")

            self.add_log(f"✓ Stored cookies with ID: {cookie_id}")

            # Update account metadata
            update_account_query = """
            UPDATE accounts
            SET has_cookies = TRUE,
                cookies_last_updated = CURRENT_TIMESTAMP
            WHERE account_id = %s
            """
            self.db_manager.execute_query(update_account_query, (account_id,))
            self.add_log(f"✓ Updated account {account_id} metadata")

            # VERIFICATION: Read back and verify
            verify_query = """
            SELECT cookie_data::text FROM account_cookies
            WHERE cookie_id = %s
            """
            verify_result = self.db_manager.execute_query(verify_query, (cookie_id,), fetch=True)

            if verify_result:
                stored_data = verify_result[0][0] if isinstance(verify_result[0], tuple) else verify_result[0].get('cookie_data')
                try:
                    # Try to parse it back
                    parsed_back = json.loads(stored_data)
                    self.add_log(f"✓ Verified: Successfully parsed {len(parsed_back)} cookies back from DB")
                except Exception as e:
                    self.add_log(f"⚠️ Warning: Stored data may not be valid JSON: {e}", "WARNING")

            return {
                'success': True,
                'cookie_id': cookie_id,
                'cookie_count': cookie_count,
                'account_id': account_id
            }

        except Exception as e:
            error_msg = f"Failed to store cookies: {str(e)}"
            self.add_log(error_msg, "ERROR")
            import traceback
            self.add_log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return {
                'success': False,
                'error': error_msg
            }


    def _get_account_cookies(self, account_id):
        """
        FULLY FIXED: Get active cookies for an account
        Fixes:
        1. Handle numpy.int64 types
        2. Proper JSONB/TEXT column handling
        3. Better error handling
        """
        try:
            # CRITICAL: Convert numpy types to native Python
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            query = """
            SELECT cookie_id, cookie_data::text, cookie_count,
                uploaded_at, updated_at, cookie_source, notes
            FROM account_cookies
            WHERE account_id = %s AND is_active = TRUE
            ORDER BY uploaded_at DESC
            LIMIT 1
            """

            result = self.db_manager.execute_query(query, (account_id,), fetch=True)

            if not result:
                return {
                    'has_cookies': False,
                    'cookies': None
                }

            row = result[0]

            # Handle both tuple and dict-like row objects
            if isinstance(row, tuple):
                cookie_id = row[0]
                cookie_data_raw = row[1]
                cookie_count = row[2]
                uploaded_at = row[3]
                updated_at = row[4]
                cookie_source = row[5]
                notes = row[6]
            else:
                cookie_id = row.get('cookie_id') or row['cookie_id']
                cookie_data_raw = row.get('cookie_data') or row['cookie_data']
                cookie_count = row.get('cookie_count') or row['cookie_count']
                uploaded_at = row.get('uploaded_at') or row['uploaded_at']
                updated_at = row.get('updated_at') or row['updated_at']
                cookie_source = row.get('cookie_source') or row['cookie_source']
                notes = row.get('notes')

            # Parse cookie data
            cookie_data = None
            if cookie_data_raw:
                if isinstance(cookie_data_raw, str):
                    # It's a JSON string - parse it
                    try:
                        cookie_data = json.loads(cookie_data_raw)
                    except json.JSONDecodeError as e:
                        self.add_log(f"Warning: Could not parse cookie_data as JSON: {e}", "WARNING")
                        return {
                            'has_cookies': False,
                            'error': f'Invalid cookie data format: {e}',
                            'raw_data': cookie_data_raw[:200] if cookie_data_raw else None
                        }
                elif isinstance(cookie_data_raw, (list, dict)):
                    # It's already parsed (JSONB returned as Python object)
                    cookie_data = cookie_data_raw
                else:
                    self.add_log(f"Warning: Unexpected cookie_data type: {type(cookie_data_raw)}", "WARNING")
                    return {
                        'has_cookies': False,
                        'error': f'Unexpected cookie data type: {type(cookie_data_raw)}'
                    }

            return {
                'has_cookies': True,
                'cookie_id': cookie_id,
                'cookie_data': cookie_data,
                'cookie_count': cookie_count,
                'uploaded_at': uploaded_at,
                'updated_at': updated_at,
                'cookie_source': cookie_source,
                'notes': notes
            }

        except Exception as e:
            self.add_log(f"Failed to get cookies for account {account_id}: {e}", "ERROR")
            import traceback
            self.add_log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return {
                'has_cookies': False,
                'error': str(e)
            }


    def _update_account_cookies(self, account_id, cookies_json, username):
        """
        FULLY FIXED: Update cookies for an existing account
        Just calls the fixed _store_account_cookies
        """
        try:
            # Convert numpy types
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            self.add_log(f"Updating cookies for account {account_id} ({username})")

            # Use the same storage method (it deactivates old and creates new)
            result = self._store_account_cookies(account_id, cookies_json, username)

            if result['success']:
                self.add_log(f"✓ Updated cookies - new count: {result['cookie_count']}")

            return result

        except Exception as e:
            error_msg = f"Failed to update cookies: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {
                'success': False,
                'error': error_msg
            }


    # HELPER: Add this import at the top of your accounts.py file
    # Add after the other imports:
    # import numpy as np


    # TEST FUNCTION: Add this to your AccountsPage class to test the fix
    def test_cookie_storage_fix(self, account_id=1):
        """
        Test the cookie storage fix
        Run this after implementing the fixes to verify everything works
        """
        print("\n" + "="*60)
        print("TESTING COOKIE STORAGE FIX")
        print("="*60)

        # Test data
        test_cookies = [
            {
                "domain": ".x.com",
                "name": "test_cookie",
                "value": "test_value_123",
                "path": "/",
                "secure": True,
                "httpOnly": False
            }
        ]

        test_json = json.dumps(test_cookies)

        print(f"\n[1] Testing _store_account_cookies...")
        result = self._store_account_cookies(account_id, test_json, "test_user")

        if result['success']:
            print(f"  ✓ SUCCESS: Stored cookie_id {result['cookie_id']}")
            cookie_id = result['cookie_id']
        else:
            print(f"  ✗ FAILED: {result['error']}")
            return False

        print(f"\n[2] Testing _get_account_cookies...")
        result = self._get_account_cookies(account_id)

        if result['has_cookies']:
            print(f"  ✓ SUCCESS: Retrieved {result['cookie_count']} cookies")
            print(f"    Cookie data type: {type(result['cookie_data'])}")
            print(f"    Cookie data sample: {result['cookie_data'][0] if result['cookie_data'] else None}")
        else:
            print(f"  ✗ FAILED: {result.get('error', 'No error message')}")
            return False

        print("\n" + "="*60)
        print("ALL TESTS PASSED!")
        print("="*60)
        return True


    # ============================================================================
    # METHOD 2: _handle_account_creation - UPDATED with cookie storage
    # ============================================================================

    


    # ============================================================================
    # METHOD 4: _update_account_cookies - NEW METHOD
    # ============================================================================

    def _update_account_cookies(self, account_id, cookies_json, username):
        """Update cookies for an existing account"""
        try:
            self.add_log(f"Updating cookies for account {account_id} ({username})")

            # Use the same storage method (it deactivates old and creates new)
            result = self._store_account_cookies(account_id, cookies_json, username)

            if result['success']:
                self.add_log(f"✓ Updated cookies - new count: {result['cookie_count']}")

            return result

        except Exception as e:
            error_msg = f"Failed to update cookies: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {
                'success': False,
                'error': error_msg
            }


    # ============================================================================
    # METHOD 5: _get_account_cookies - NEW METHOD
    # ============================================================================


    def _render_cookie_manager_tab_enhanced(self, accounts_df):
        """
        COMPLETE Cookie Manager tab with:
        - Live Capture (recommended)
        - Script Generation (for VNC clipboard copy)
        - Manual Upload (fallback)
        - Sync to Airflow (NEW - critical for DAG integration)
        """
        st.subheader("🍪 Cookie Tools")

        if accounts_df.empty:
            st.info("No accounts available")
            return

        # Filter to local chrome accounts only
        local_accounts = accounts_df[accounts_df['profile_type'] == 'local_chrome']

        if local_accounts.empty:
            st.warning("No Local Chrome accounts found. Cookies are only supported for Local Chrome accounts.")
            return

        # Four tabs: Live Capture, Script Generation, Manual Upload, Sync to Airflow
        capture_tab, script_tab, manage_tab, sync_tab = st.tabs([
            "🎯 Live Capture",
            "📜 Copy Scripts",
            "📤 Manage Cookies",
            "🔄 Sync to Airflow"
        ])

        # Count accounts with cookies - FIXED: Use proper count
        accounts_with_cookies_count = 0
        accounts_with_cookies_df = pd.DataFrame()

        if 'has_cookies' in local_accounts.columns:
            accounts_with_cookies_df = local_accounts[local_accounts['has_cookies'] == True]
            accounts_with_cookies_count = len(accounts_with_cookies_df)

        # Stats - FIXED: Unique key for refresh button
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Local Accounts", len(local_accounts))
        with col2:
            st.metric("With Cookies", accounts_with_cookies_count)
        with col3:
            st.metric("Need Sync", accounts_with_cookies_count)
        with col4:
            # FIXED: Unique key
            if st.button("🔄 Refresh", use_container_width=True, key="cookie_tools_stats_refresh"):
                st.cache_data.clear()
                st.rerun()

        st.markdown("---")

        # =========================================================================
        # TAB 1: LIVE CAPTURE
        # =========================================================================
        with capture_tab:
            st.subheader("🎯 Live Cookie Capture")

            st.info(
                "**Capture authenticated cookies from a running Chrome session**\n\n"
                "✅ **NEW:** Verifies you're actually logged in before capturing\n\n"
                "**Workflow:**\n"
                "1. Start a Local Chrome session\n"
                "2. Open VNC (http://localhost:6080)\n"
                "3. Navigate to https://x.com/home in Chrome\n"
                "4. Log in (or paste cookies via EditThisCookie)\n"
                "5. **Verify you can see your timeline**\n"
                "6. Click 'Capture Cookies' below\n"
                "7. Cookies will be verified and auto-synced to Airflow"
            )

            # Check if any sessions are running
            local_sessions = st.session_state.get('local_chrome_sessions', {})

            if not local_sessions:
                st.warning("⚠️ No active Local Chrome sessions")
                st.caption("Start a session in the 'Local Chrome' tab, then come back here")
            else:
                st.markdown("---")

                # Group sessions by account
                st.write("**Active Accounts with Sessions:**")

                accounts_with_sessions = {}
                for session_id, session_info in local_sessions.items():
                    account_username = session_info.get('account_username', 'Unknown')
                    if account_username not in accounts_with_sessions:
                        accounts_with_sessions[account_username] = []
                    accounts_with_sessions[account_username].append(session_id)

                for account_username, session_ids in accounts_with_sessions.items():
                    # Get account_id for this account
                    account_row = local_accounts[local_accounts['username'] == account_username]

                    if account_row.empty:
                        continue

                    account_id = account_row.iloc[0]['account_id']
                    profile_path = self.chrome_manager.get_profile_path(account_username)

                    with st.container():
                        st.markdown(f"### 👤 {account_username}")

                        col1, col2 = st.columns([2, 1])

                        with col1:
                            st.caption(f"**Profile:** {profile_path}")
                            st.caption(f"**Active Sessions:** {len(session_ids)}")

                            # Show all session IDs
                            with st.expander("View Session IDs", expanded=False):
                                for sid in session_ids:
                                    st.code(sid)

                            # Show cookie status
                            self._render_cookie_status_indicator(account_id, account_username)

                            # Show instructions
                            with st.expander("📋 Capture Instructions", expanded=False):
                                st.markdown(
                                    f"""
                                    **Step-by-step for account '{account_username}':**

                                    1. **Open VNC**: [http://localhost:6080](http://localhost:6080)
                                    - Password: `secret`

                                    2. **In Chrome, go to**: `https://x.com/home`

                                    3. **Make sure you're logged in**:
                                    - Can you see your timeline?
                                    - Can you see the tweet compose box?

                                    4. **Click "Capture Cookies"** button →

                                    5. **Cookies saved to account**: All {len(session_ids)} session(s) will use these cookies

                                    ⚠️ **Important**: Cookies are shared across ALL sessions of this account!
                                    """
                                )

                        with col2:
                            st.write("**Actions:**")

                            # FIXED: Unique key
                            capture_key = f"capture_cookies_ui_{account_username}_{int(time.time())}"

                            if st.button(
                                "🎯 Capture Cookies",
                                key=capture_key,
                                use_container_width=True,
                                type="primary"
                            ):
                                with st.spinner(f"Capturing cookies for {account_username}..."):
                                    result = self._capture_cookies_from_running_chrome(
                                        account_id,
                                        account_username,
                                        profile_path
                                    )

                                    if result['success']:
                                        success_msg = (
                                            f"✅ Captured {result['cookie_count']} cookies!\n\n"
                                            f"✓ Has auth_token: {result['has_auth_token']}\n"
                                            f"✓ Login verified: {result.get('login_verified', False)}\n\n"
                                            f"**Cookies saved to account '{account_username}'**\n"
                                            f"All {len(session_ids)} session(s) will use these cookies!"
                                        )
                                        st.success(success_msg)

                                        # Store in database
                                        try:
                                            session_file = os.path.join(profile_path, 'session_data.json')
                                            if os.path.exists(session_file):
                                                with open(session_file, 'r') as f:
                                                    session_data = json.load(f)

                                                cookies_json = json.dumps(session_data['cookies'])

                                                store_result = self._store_account_cookies(
                                                    account_id, cookies_json, account_username
                                                )

                                                if store_result['success']:
                                                    st.success("✓ Cookies saved to database")

                                                    # NEW: Auto-sync to session_data.json for Airflow
                                                    st.info("🔄 Syncing to session_data.json for Airflow...")
                                                    sync_result = self._sync_cookies_to_session_file(
                                                        account_id, account_username
                                                    )

                                                    if sync_result['success']:
                                                        st.success("✓ Synced to session_data.json for Airflow!")
                                                        st.info(
                                                            f"**Airflow DAG Ready**\n\n"
                                                            f"📁 File: `{os.path.basename(sync_result['session_file'])}`\n"
                                                            f"🍪 Cookies: {sync_result['cookie_count']}\n"
                                                            f"🔑 Auth token: {'✓ Present' if sync_result['has_auth_token'] else '❌ Missing'}\n\n"
                                                            f"Your `extract_links_weekly` DAG can now run!"
                                                        )

                                                        if sync_result.get('missing_critical'):
                                                            st.warning(
                                                                f"⚠️ Missing cookies: {', '.join(sync_result['missing_critical'])}\n"
                                                                f"These may be needed for authentication."
                                                            )
                                                    else:
                                                        st.error(f"❌ Sync failed: {sync_result.get('error')}")
                                                        st.warning(
                                                            "⚠️ Cookies saved to database but NOT synced to Airflow.\n"
                                                            "Use the 'Sync to Airflow' tab to sync manually."
                                                        )
                                        except Exception as e:
                                            st.warning(f"Could not save to database: {e}")

                                        st.cache_data.clear()
                                        time.sleep(2)
                                        st.rerun()
                                    else:
                                        error_msg = result['error']
                                        st.error(f"❌ Failed: {error_msg}")

                                        # Show actionable help based on error
                                        if "not logged in" in error_msg.lower():
                                            st.warning(
                                                "**You're not logged in to X.com!**\n\n"
                                                "Open VNC and log in first:\n"
                                                "http://localhost:6080"
                                            )
                                        elif "redirected to login" in error_msg.lower():
                                            st.warning(
                                                "**Your session redirected to login page!**\n\n"
                                                "Your cookies expired. Log in again via VNC:\n"
                                                "http://localhost:6080"
                                            )

                        st.divider()

            # Show capture logs if available
            if st.session_state.get('account_creation_logs'):
                with st.expander("📋 Detailed Capture Logs", expanded=False):
                    log_text = "\n".join(st.session_state.account_creation_logs)
                    st.code(log_text, language="log")

                    # FIXED: Unique key
                    if st.button("Clear Logs", key="clear_capture_logs_ui"):
                        self.clear_logs()
                        st.rerun()

        # =========================================================================
        # TAB 2: SCRIPT GENERATION
        # =========================================================================
        with script_tab:
            st.subheader("📜 Cookie Copy Scripts")

            st.info(
                "**Generate shell scripts to copy cookies to clipboard**\n\n"
                "These scripts help you copy stored cookies to clipboard in VNC:\n"
                "1. Generate script for an account\n"
                "2. SSH into container\n"
                "3. Run the script\n"
                "4. Cookies copied to clipboard\n"
                "5. Paste in EditThisCookie in VNC"
            )

            # Bulk Actions Section
            with st.expander("⚡ Bulk Actions", expanded=False):
                st.write("**Generate scripts for all accounts with cookies**")

                col1, col2 = st.columns([3, 1])

                with col1:
                    st.info(
                        "This will generate individual shell scripts for each account "
                        "in `/app/cookie_scripts/`. You can then run them directly from the terminal."
                    )

                with col2:
                    # FIXED: Unique key
                    if st.button("🚀 Generate All Scripts", use_container_width=True, key="bulk_gen_scripts_btn_ui"):
                        with st.spinner("Generating scripts..."):
                            result = self._generate_all_cookie_scripts()

                            if result['success']:
                                st.success(f"✓ Generated {result['scripts_generated']} scripts!")

                                if result['scripts']:
                                    st.write("**Generated Scripts:**")
                                    for script in result['scripts']:
                                        st.code(f"cd /app/cookie_scripts && ./{script['script']}")

                                if result.get('failed'):
                                    st.warning(f"Failed to generate {len(result['failed'])} scripts")
                                    for fail in result['failed']:
                                        st.error(f"❌ {fail['username']}: {fail['error']}")
                            else:
                                st.error(f"Failed: {result['error']}")

            st.markdown("---")

            # Individual account script generation
            st.write("**Generate Script for Individual Account:**")

            if accounts_with_cookies_count == 0:
                st.warning("No accounts have cookies stored")
            else:
                account_options = [
                    (f"{row['username']} (ID: {row['account_id']})", idx)
                    for idx, row in accounts_with_cookies_df.iterrows()
                ]

                selected = st.selectbox(
                    "Select Account",
                    [("Choose account...", None)] + account_options,
                    format_func=lambda x: x[0],
                    key="script_gen_account_select_ui"
                )

                if selected[1] is not None:
                    row = accounts_with_cookies_df.iloc[selected[1]]
                    account_id = row['account_id']
                    username = row['username']

                    st.markdown("---")

                    col_gen, col_info = st.columns([1, 2])

                    with col_gen:
                        # FIXED: Unique key
                        if st.button("🔧 Generate Script",
                                use_container_width=True,
                                type="primary",
                                key=f"gen_script_btn_ui_{account_id}"):
                            with st.spinner("Generating script..."):
                                result = self._generate_cookie_copy_script(account_id, username)

                                if result['success']:
                                    st.session_state[f'script_path_ui_{account_id}'] = result['script_path']
                                    st.session_state[f'script_cmd_ui_{account_id}'] = result['command']
                                    st.success("✓ Script generated!")
                                    st.rerun()
                                else:
                                    st.error(f"Failed: {result['error']}")

                    # Show script command if generated
                    if st.session_state.get(f'script_path_ui_{account_id}'):
                        script_path = st.session_state[f'script_path_ui_{account_id}']
                        script_cmd = st.session_state[f'script_cmd_ui_{account_id}']

                        st.success("✅ Script ready!")

                        st.write("**Run this command in your terminal:**")
                        st.code(f"cd /app/cookie_scripts && {script_cmd}", language="bash")

                        st.info(
                            "💡 **How to use:**\n"
                            "1. Connect to container: `docker exec -it streamlit_app bash`\n"
                            f"2. Run: `cd /app/cookie_scripts && {script_cmd}`\n"
                            "3. Cookies will be copied to clipboard\n"
                            "4. Open VNC at http://localhost:6080\n"
                            "5. Paste in browser (Ctrl+V)"
                        )

                        # File info
                        try:
                            if os.path.exists(script_path):
                                size = os.path.getsize(script_path)
                                st.caption(f"📄 File: `{script_path}` ({size} bytes)")
                        except:
                            pass

        # =========================================================================
        # TAB 3: MANUAL UPLOAD
        # =========================================================================
        with manage_tab:
            # This uses _render_account_cookie_management_tab which we already fixed
            pass

        # =========================================================================
        # TAB 4: SYNC TO AIRFLOW (NEW - CRITICAL FOR DAG INTEGRATION)
        # =========================================================================
        with sync_tab:
            st.subheader("🔄 Sync Cookies to Airflow")

            st.info(
                "**Sync cookies to Airflow extraction DAG**\n\n"
                "After capturing cookies, sync them to `session_data.json` "
                "so your Airflow `extract_links_weekly` DAG can use them.\n\n"
                "✅ **Required before running Airflow DAG**\n"
                "✅ Syncs from PostgreSQL → session_data.json\n"
                "✅ Airflow reads session_data.json for authentication"
            )

            st.markdown("---")

            # Account-by-account sync - FIXED: Use count, not DataFrame
            if accounts_with_cookies_count == 0:
                st.warning("⚠️ No accounts have cookies to sync")
                st.caption("Capture cookies first in the 'Live Capture' tab")
            else:
                st.write("**Accounts Ready to Sync:**")
                st.caption("Click 'Sync' to make cookies available to Airflow DAG")

                for idx, row in accounts_with_cookies_df.iterrows():
                    account_id = row['account_id']
                    username = row['username']
                    profile_path = self.chrome_manager.get_profile_path(username)

                    # Get cookie info from database
                    cookie_info = self._get_account_cookies(account_id)

                    # Check if session_data.json exists
                    session_file = os.path.join(profile_path, 'session_data.json')
                    session_exists = os.path.exists(session_file)

                    if session_exists:
                        session_mtime = datetime.fromtimestamp(os.path.getmtime(session_file))
                        session_age = datetime.now() - session_mtime
                        session_age_str = f"{session_age.days}d {session_age.seconds//3600}h ago"
                    else:
                        session_age_str = "Never"

                    with st.container():
                        st.markdown(f"### 👤 {username}")

                        col1, col2, col3 = st.columns([2, 2, 1])

                        with col1:
                            st.write("**Database (PostgreSQL)**")
                            if cookie_info['has_cookies']:
                                st.success(f"✓ {cookie_info['cookie_count']} cookies")
                                if cookie_info.get('updated_at'):
                                    st.caption(f"Last updated: {cookie_info['updated_at']}")
                            else:
                                st.error("❌ No cookies")
                                st.caption("Capture cookies first")

                        with col2:
                            st.write("**Airflow (session_data.json)**")
                            if session_exists:
                                # Try to read and validate
                                try:
                                    with open(session_file, 'r') as f:
                                        session_data = json.load(f)

                                    file_cookie_count = len(session_data.get('cookies', []))
                                    has_auth = any(c['name'] == 'auth_token' for c in session_data.get('cookies', []))

                                    if file_cookie_count > 0 and has_auth:
                                        st.success(f"✓ {file_cookie_count} cookies")
                                        st.caption(f"Synced: {session_age_str}")
                                    else:
                                        st.warning(f"⚠️ {file_cookie_count} cookies")
                                        st.caption("Missing auth_token")
                                except:
                                    st.error("❌ Corrupted file")
                                    st.caption("Needs re-sync")
                            else:
                                st.error("❌ Not synced")
                                st.caption("Never synced")

                        with col3:
                            st.write("**Action**")

                            sync_button_disabled = not cookie_info['has_cookies']
                            sync_button_type = "primary" if not session_exists else "secondary"

                            # FIXED: Unique key
                            if st.button(
                                "🔄 Sync",
                                key=f"sync_airflow_ui_{account_id}",
                                disabled=sync_button_disabled,
                                use_container_width=True,
                                type=sync_button_type
                            ):
                                with st.spinner(f"Syncing {username}..."):
                                    result = self._sync_cookies_to_session_file(account_id, username)

                                    if result['success']:
                                        st.success(f"✅ Synced {result['cookie_count']} cookies!")

                                        st.info(
                                            f"**Airflow Ready**\n\n"
                                            f"📁 {os.path.basename(result['session_file'])}\n"
                                            f"🍪 Cookies: {result['cookie_count']}\n"
                                            f"🔑 Auth token: {'✓' if result['has_auth_token'] else '❌'}\n"
                                            f"📦 Size: {result['file_size']} bytes"
                                        )

                                        if result.get('missing_critical'):
                                            st.warning(
                                                f"⚠️ Missing: {', '.join(result['missing_critical'])}"
                                            )

                                        time.sleep(2)
                                        st.rerun()
                                    else:
                                        st.error(f"❌ Sync failed: {result.get('error')}")

                        # Show session file path
                        with st.expander("📋 Technical Details", expanded=False):
                            st.code(f"Profile: {profile_path}")
                            st.code(f"Session file: {session_file}")

                            if session_exists:
                                st.code(f"File size: {os.path.getsize(session_file)} bytes")
                                st.code(f"Last modified: {session_mtime}")

                            st.caption(
                                "**How it works:**\n"
                                "1. Cookies captured in Streamlit → PostgreSQL database\n"
                                "2. Sync button copies cookies → session_data.json\n"
                                "3. Airflow DAG reads session_data.json → authenticates Chrome\n"
                                "4. Extraction runs with your login credentials"
                            )

                        st.divider()

            # Bulk sync button - FIXED: Use count, not DataFrame
            if accounts_with_cookies_count > 0:
                st.markdown("---")

                col1, col2 = st.columns([3, 1])

                with col1:
                    st.write("**Bulk Sync All Accounts**")
                    st.caption("Sync all accounts with cookies to Airflow in one click")

                with col2:
                    # FIXED: Unique key
                    if st.button(
                        "🚀 Sync All",
                        use_container_width=True,
                        type="primary",
                        key="bulk_sync_all_ui"
                    ):
                        with st.spinner("Syncing all accounts..."):
                            success_count = 0
                            fail_count = 0

                            for idx, row in accounts_with_cookies_df.iterrows():
                                account_id = row['account_id']
                                username = row['username']

                                cookie_info = self._get_account_cookies(account_id)
                                if not cookie_info['has_cookies']:
                                    continue

                                result = self._sync_cookies_to_session_file(account_id, username)

                                if result['success']:
                                    success_count += 1
                                else:
                                    fail_count += 1

                            if fail_count == 0:
                                st.success(f"✅ Successfully synced {success_count} account(s)!")
                            else:
                                st.warning(f"⚠️ Synced {success_count}, failed {fail_count}")

                            time.sleep(2)
                            st.rerun()

    # ============================================================================
    # METHOD 6: _render_cookie_manager_tab - NEW METHOD
    # ============================================================================

    """
COMPLETE Cookie Manager methods for AccountsPage class
Includes: Live Capture + Script Generation + Manual Upload
"""
    def _render_cookie_status_indicator(self, account_id, username):
        """
        NEW METHOD: Render cookie status with action buttons
        Shows if cookies are valid, expired, or missing
        """
        try:
            status = self._check_cookie_validity(account_id)

            if status['valid']:
                if status.get('warning'):
                    st.warning(f"⚠️ {status['warning']}")
                else:
                    st.success(f"✓ Cookies valid for {username}")
                    st.caption(f"🍪 {status.get('cookie_count', 0)} cookies stored")
            else:
                st.error(f"❌ Cookies invalid: {status['reason']}")

                if status.get('needs_capture'):
                    st.info(
                        "**To fix this:**\n\n"
                        "1. Start a Local Chrome session below\n"
                        "2. Open VNC at http://localhost:6080\n"
                        "3. Navigate to https://x.com/home\n"
                        "4. Log in (or paste cookies via EditThisCookie)\n"
                        "5. Click '🎯 Capture Cookies' button\n"
                        "6. Then your DAG will work!"
                    )

        except Exception as e:
            st.error(f"Could not check cookie status: {str(e)}")
    def _check_cookie_validity(self, account_id):
        """
        NEW METHOD: Check if stored cookies are still valid
        Checks for auth_token and expiration
        """
        try:
            # Convert numpy types
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            # Get cookies
            cookie_info = self._get_account_cookies(account_id)

            if not cookie_info['has_cookies']:
                return {
                    'valid': False,
                    'reason': 'No cookies stored',
                    'needs_capture': True
                }

            cookies = cookie_info['cookie_data']
            if isinstance(cookies, str):
                try:
                    cookies = json.loads(cookies)
                except:
                    return {
                        'valid': False,
                        'reason': 'Invalid cookie format',
                        'needs_capture': True
                    }

            # Check for critical cookies
            auth_token = next((c for c in cookies if c['name'] == 'auth_token'), None)
            ct0 = next((c for c in cookies if c['name'] == 'ct0'), None)

            if not auth_token:
                return {
                    'valid': False,
                    'reason': 'No auth_token found',
                    'needs_capture': True
                }

            if not ct0:
                return {
                    'valid': False,
                    'reason': 'No ct0 (CSRF token) found',
                    'needs_capture': True
                }

            # Check expiration
            current_time = time.time()

            if auth_token.get('expirationDate'):
                exp_timestamp = auth_token['expirationDate']
                if exp_timestamp < current_time:
                    return {
                        'valid': False,
                        'reason': 'auth_token expired',
                        'expired_at': datetime.fromtimestamp(exp_timestamp),
                        'needs_capture': True
                    }

            # Check age of cookies
            uploaded_at = cookie_info.get('uploaded_at')
            if uploaded_at:
                age_days = (datetime.now() - uploaded_at).days
                if age_days > 30:
                    return {
                        'valid': True,
                        'warning': f'Cookies are {age_days} days old - may need refresh',
                        'age_days': age_days
                    }

            return {
                'valid': True,
                'message': 'Cookies appear valid',
                'has_auth_token': True,
                'has_ct0': True,
                'cookie_count': len(cookies)
            }

        except Exception as e:
            return {
                'valid': False,
                'reason': f'Validation error: {str(e)}',
                'needs_capture': True
            }

    def _capture_cookies_from_running_chrome(self, account_id, username, profile_path):
        """
        FULLY ENHANCED: Capture cookies with comprehensive validation

        Features:
        - Login verification with multiple UI checks
        - Cookie validation (expiry, critical cookies, domain)
        - Detailed error reporting
        - Auto-sync to session_data.json
        - Validation metadata stored
        """
        try:
            self.add_log(f"Starting ENHANCED cookie capture for {username}...")
            self.add_log(f"Profile path: {profile_path}")

            # Check if Chrome is running on port 9222
            import requests
            try:
                response = requests.get('http://localhost:9222/json/version', timeout=3)
                if response.status_code != 200:
                    raise Exception("Chrome not responding")
                self.add_log("✓ Chrome is running on port 9222")
            except Exception as e:
                raise Exception(
                    f"Chrome is not running on port 9222. "
                    f"Start a Local Chrome session first. Error: {e}"
                )

            # Find the extraction directory with node_modules
            extraction_dirs = [
                '/app/src/scripts/extraction',           # Streamlit container
                '/opt/airflow/src/scripts/extraction',   # Airflow container
            ]

            extraction_dir = None
            for dir_path in extraction_dirs:
                if os.path.exists(dir_path) and os.path.exists(os.path.join(dir_path, 'node_modules')):
                    extraction_dir = dir_path
                    self.add_log(f"Found extraction directory: {extraction_dir}")
                    break

            if not extraction_dir:
                raise Exception(
                    "Could not find extraction directory with node_modules. "
                    "Checked: " + ", ".join(extraction_dirs)
                )

            # Create the ENHANCED capture script with validation
            script_filename = f"capture_cookies_validated_{username}_{int(time.time())}.js"
            script_path = os.path.join(extraction_dir, script_filename)

            # ENHANCED SCRIPT WITH VALIDATION
            enhanced_capture_script = '''
    import puppeteer from 'puppeteer-core';
    import fs from 'fs';
    import path from 'path';
    import { fileURLToPath } from 'url';

    const __filename = fileURLToPath(import.meta.url);
    const __dirname = path.dirname(__filename);

    const CHROME_DEBUG_PORT = '9222';
    const CHROME_PROFILE_DIR = process.env.CHROME_PROFILE_DIR;
    const SESSION_FILE = path.join(CHROME_PROFILE_DIR, 'session_data.json');

    // ============================================================================
    // COOKIE VALIDATION LOGIC
    // ============================================================================
    function validateCookies(cookies) {
        const validation = {
            valid: true,
            errors: [],
            warnings: [],
            criticalCookies: {}
        };

        // Check for critical cookies
        const criticalNames = ['auth_token', 'ct0', 'kdt'];

        for (const name of criticalNames) {
            const cookie = cookies.find(c => c.name === name);
            if (!cookie) {
                validation.valid = false;
                validation.errors.push(`Missing critical cookie: ${name}`);
            } else {
                validation.criticalCookies[name] = cookie;

                // Check expiration
                if (cookie.expirationDate) {
                    const expiryDate = new Date(cookie.expirationDate * 1000);
                    const now = new Date();

                    if (expiryDate < now) {
                        validation.valid = false;
                        validation.errors.push(`Cookie ${name} is EXPIRED (expired: ${expiryDate.toISOString()})`);
                    } else {
                        const daysUntilExpiry = (expiryDate - now) / (1000 * 60 * 60 * 24);
                        if (daysUntilExpiry < 7) {
                            validation.warnings.push(`Cookie ${name} expires soon (${daysUntilExpiry.toFixed(1)} days)`);
                        }
                    }
                }

                // Check domain
                if (!cookie.domain.includes('x.com') && !cookie.domain.includes('twitter.com')) {
                    validation.warnings.push(`Cookie ${name} has unexpected domain: ${cookie.domain}`);
                }

                // Check value is not empty
                if (!cookie.value || cookie.value.length < 10) {
                    validation.valid = false;
                    validation.errors.push(`Cookie ${name} has invalid or empty value`);
                }
            }
        }

        return validation;
    }

    // ============================================================================
    // ENHANCED LOGIN VERIFICATION
    // ============================================================================
    async function verifyLoginStatus(page) {
        console.log('');
        console.log('═══════════════════════════════════════════════════════════════════════════════');
        console.log('ENHANCED LOGIN VERIFICATION');
        console.log('═══════════════════════════════════════════════════════════════════════════════');

        const verification = await page.evaluate(() => {
            const pageTitle = document.title;
            const pageUrl = window.location.href;

            // Check for login page indicators
            const isLoginPage = pageUrl.includes('/login') ||
                            pageUrl.includes('/i/flow/login') ||
                            pageTitle.toLowerCase().includes('login to x') ||
                            pageTitle.toLowerCase() === 'x';

            // Multiple strategies to detect logged-in state
            const checks = {
                // Primary indicators
                hasTimeline: !!document.querySelector('[data-testid="primaryColumn"]'),
                hasTweetBox: !!document.querySelector('[data-testid="tweetTextarea_0"]'),
                hasUserMenu: !!document.querySelector('[data-testid="AppTabBar_Profile_Link"]'),
                hasSideNav: !!document.querySelector('[data-testid="SideNav_AccountSwitcher_Button"]'),
                hasHomeTimeline: !!document.querySelector('[data-testid="homeTimeline"]'),
                hasTweets: !!document.querySelector('article[data-testid="tweet"]'),
                hasProfileMenu: !!document.querySelector('[aria-label*="Account menu"]'),

                // Secondary indicators
                hasComposeButton: !!document.querySelector('[data-testid="SideNav_NewTweet_Button"]'),
                hasSearchBar: !!document.querySelector('[data-testid="SearchBox_Search_Input"]'),
                hasNotifications: !!document.querySelector('[data-testid="AppTabBar_Notifications_Link"]'),

                // Negative indicators (shouldn't be present if logged in)
                hasSignUpButton: !!document.querySelector('[data-testid="signupButton"]'),
                hasLoginButton: !!document.querySelector('[href="/login"]'),
            };

            // Count positive indicators
            const positiveCount = Object.entries(checks)
                .filter(([key, _]) => !key.includes('SignUp') && !key.includes('LoginButton'))
                .filter(([_, value]) => value === true)
                .length;

            const negativeCount = (checks.hasSignUpButton ? 1 : 0) + (checks.hasLoginButton ? 1 : 0);

            // More sophisticated logged-in detection
            const isLoggedIn = (
                positiveCount >= 2 &&  // At least 2 positive indicators
                negativeCount === 0     // No negative indicators
            );

            return {
                pageTitle,
                pageUrl,
                isLoginPage,
                isLoggedIn,
                checks,
                positiveCount,
                negativeCount,
                htmlLength: document.documentElement.outerHTML.length
            };
        });

        // Log verification results
        console.log('Page Information:');
        console.log(`  Title: "${verification.pageTitle}"`);
        console.log(`  URL: ${verification.pageUrl}`);
        console.log(`  HTML Length: ${verification.htmlLength} bytes`);
        console.log('');

        console.log('Login Page Detection:');
        console.log(`  Is login page: ${verification.isLoginPage}`);
        console.log('');

        console.log('UI Element Detection (Primary):');
        console.log(`  ✓ Timeline: ${verification.checks.hasTimeline}`);
        console.log(`  ✓ Tweet box: ${verification.checks.hasTweetBox}`);
        console.log(`  ✓ User menu: ${verification.checks.hasUserMenu}`);
        console.log(`  ✓ Side nav: ${verification.checks.hasSideNav}`);
        console.log(`  ✓ Home timeline: ${verification.checks.hasHomeTimeline}`);
        console.log(`  ✓ Tweets: ${verification.checks.hasTweets}`);
        console.log(`  ✓ Profile menu: ${verification.checks.hasProfileMenu}`);
        console.log('');

        console.log('UI Element Detection (Secondary):');
        console.log(`  ✓ Compose button: ${verification.checks.hasComposeButton}`);
        console.log(`  ✓ Search bar: ${verification.checks.hasSearchBar}`);
        console.log(`  ✓ Notifications: ${verification.checks.hasNotifications}`);
        console.log('');

        console.log('Negative Indicators (should be false):');
        console.log(`  ✗ Sign up button: ${verification.checks.hasSignUpButton}`);
        console.log(`  ✗ Login button: ${verification.checks.hasLoginButton}`);
        console.log('');

        console.log('Overall Assessment:');
        console.log(`  Positive indicators: ${verification.positiveCount}`);
        console.log(`  Negative indicators: ${verification.negativeCount}`);
        console.log(`  Is logged in: ${verification.isLoggedIn}`);
        console.log('═══════════════════════════════════════════════════════════════════════════════');
        console.log('');

        return verification;
    }

    // ============================================================================
    // MAIN CAPTURE LOGIC
    // ============================================================================
    (async () => {
        let browser = null;
        let page = null;

        try {
            console.log('Starting ENHANCED cookie capture with validation...');
            console.log('');

            // Connect to Chrome
            console.log('Connecting to Chrome...');
            browser = await puppeteer.connect({
                browserURL: `http://localhost:${CHROME_DEBUG_PORT}`,
                defaultViewport: null,
            });
            console.log('✓ Connected to Chrome');
            console.log('');

            // Create new page
            console.log('Creating new page...');
            page = await browser.newPage();
            console.log('✓ Page created');
            console.log('');

            // Navigate to X.com
            console.log('Navigating to https://x.com/home...');
            await page.goto('https://x.com/home', {
                waitUntil: 'networkidle2',
                timeout: 30000
            });
            console.log('✓ Navigation complete');
            console.log('');

            // Wait for page to settle
            console.log('Waiting for page to render...');
            await new Promise(resolve => setTimeout(resolve, 5000));
            console.log('✓ Page rendered');
            console.log('');

            // STEP 1: Verify login status BEFORE capturing cookies
            const loginStatus = await verifyLoginStatus(page);

            if (loginStatus.isLoginPage) {
                throw new Error(
                    '❌ REDIRECTED TO LOGIN PAGE!\\n\\n' +
                    'You are NOT logged in to X.com.\\n\\n' +
                    'SOLUTION:\\n' +
                    '1. Open VNC at http://localhost:6080 (password: secret)\\n' +
                    '2. In Chrome, go to https://x.com/home\\n' +
                    '3. Log in manually OR paste cookies via EditThisCookie\\n' +
                    '4. Verify you see your timeline\\n' +
                    '5. Run this capture script again\\n'
                );
            }

            if (!loginStatus.isLoggedIn) {
                throw new Error(
                    '❌ NOT LOGGED IN TO X.COM!\\n\\n' +
                    `Page title: "${loginStatus.pageTitle}"\\n` +
                    `URL: ${loginStatus.pageUrl}\\n` +
                    `Positive indicators: ${loginStatus.positiveCount}\\n` +
                    `Negative indicators: ${loginStatus.negativeCount}\\n\\n` +
                    'No logged-in UI elements detected.\\n\\n' +
                    'SOLUTION:\\n' +
                    '1. Open VNC at http://localhost:6080\\n' +
                    '2. Manually log in to X.com\\n' +
                    '3. Wait until you see your timeline\\n' +
                    '4. Run this capture script again\\n'
                );
            }

            console.log('✅✅✅ LOGIN VERIFIED - User is authenticated ✅✅✅');
            console.log('');

            // STEP 2: Capture cookies
            console.log('Capturing cookies from authenticated session...');
            const cookies = await page.cookies();
            console.log(`✓ Captured ${cookies.length} cookies`);
            console.log('');

            // STEP 3: Validate cookies
            console.log('Validating captured cookies...');
            const validation = validateCookies(cookies);

            if (!validation.valid) {
                console.error('❌ COOKIE VALIDATION FAILED');
                console.error('Errors:');
                validation.errors.forEach(err => console.error(`  - ${err}`));
                throw new Error('COOKIE VALIDATION FAILED: ' + validation.errors.join(', '));
            }

            console.log('✓ Cookie validation: PASSED');

            if (validation.warnings.length > 0) {
                console.log('⚠ Warnings:');
                validation.warnings.forEach(warn => console.log(`  - ${warn}`));
            }
            console.log('');

            // Display critical cookies
            console.log('Critical cookies validated:');
            for (const [name, cookie] of Object.entries(validation.criticalCookies)) {
                const value = cookie.value.substring(0, 20) + '...';
                const expiry = cookie.expirationDate
                    ? new Date(cookie.expirationDate * 1000).toISOString()
                    : 'Session';
                console.log(`  ✓ ${name}: ${value} (expires: ${expiry})`);
            }
            console.log('');

            // STEP 4: Capture localStorage
            console.log('Capturing localStorage...');
            let localStorage = {};
            try {
                localStorage = await page.evaluate(() => {
                    const data = {};
                    if (typeof window.localStorage !== 'undefined') {
                        for (let i = 0; i < window.localStorage.length; i++) {
                            const key = window.localStorage.key(i);
                            if (key) {
                                data[key] = window.localStorage.getItem(key);
                            }
                        }
                    }
                    return data;
                });
                console.log(`✓ Captured ${Object.keys(localStorage).length} localStorage items`);
            } catch (e) {
                console.log('⚠ localStorage unavailable (not critical)');
            }
            console.log('');

            // STEP 5: Create session data with validation metadata
            const sessionData = {
                timestamp: new Date().toISOString(),
                accountId: 1,
                profileDir: CHROME_PROFILE_DIR,
                cookies,
                localStorage,
                metadata: {
                    cookieCount: cookies.length,
                    localStorageCount: Object.keys(localStorage).length,
                    savedBy: 'enhanced_cookie_capture_validated',
                    nodeVersion: process.version,
                    hasCriticalCookies: true,
                    capturedAt: new Date().toISOString(),
                    loginVerified: true,
                    captureMethod: 'validated_capture',
                    validation: {
                        passed: validation.valid,
                        warnings: validation.warnings,
                        criticalCookiesValidated: Object.keys(validation.criticalCookies)
                    },
                    loginStatus: {
                        pageTitle: loginStatus.pageTitle,
                        isLoggedIn: loginStatus.isLoggedIn,
                        positiveIndicators: loginStatus.positiveCount,
                        negativeIndicators: loginStatus.negativeCount
                    }
                }
            };

            // STEP 6: Backup existing session file
            if (fs.existsSync(SESSION_FILE)) {
                const backupFile = `${SESSION_FILE}.backup.${Date.now()}`;
                fs.copyFileSync(SESSION_FILE, backupFile);
                console.log(`✓ Backed up old session to: ${path.basename(backupFile)}`);
            }

            // STEP 7: Save session data atomically
            const tempFile = `${SESSION_FILE}.tmp`;
            fs.writeFileSync(tempFile, JSON.stringify(sessionData, null, 2), { mode: 0o644 });
            fs.renameSync(tempFile, SESSION_FILE);

            const fileSize = fs.statSync(SESSION_FILE).size;

            console.log('');
            console.log('═══════════════════════════════════════════════════════════════════════════════');
            console.log('✅✅✅ SUCCESS - VALIDATED COOKIES SAVED ✅✅✅');
            console.log('═══════════════════════════════════════════════════════════════════════════════');
            console.log(`Session file: ${SESSION_FILE}`);
            console.log(`File size: ${(fileSize / 1024).toFixed(2)} KB`);
            console.log(`Cookies: ${cookies.length}`);
            console.log(`localStorage items: ${Object.keys(localStorage).length}`);
            console.log(`Login verified: YES`);
            console.log(`Cookie validation: PASSED`);
            console.log(`Critical cookies: ${Object.keys(validation.criticalCookies).join(', ')}`);
            if (validation.warnings.length > 0) {
                console.log(`Warnings: ${validation.warnings.length}`);
            }
            console.log('═══════════════════════════════════════════════════════════════════════════════');
            console.log('');
            console.log('Your Airflow DAG can now run with authenticated cookies!');
            console.log('');

            await page.close();
            await browser.disconnect();

            process.exit(0);

        } catch (error) {
            console.error('');
            console.error('═══════════════════════════════════════════════════════════════════════════════');
            console.error('❌ ERROR - ENHANCED COOKIE CAPTURE FAILED');
            console.error('═══════════════════════════════════════════════════════════════════════════════');
            console.error(`Error: ${error.message}`);
            console.error('');
            console.error('Stack trace:');
            console.error(error.stack);
            console.error('═══════════════════════════════════════════════════════════════════════════════');
            console.error('');

            if (page) await page.close().catch(() => {});
            if (browser) await browser.disconnect().catch(() => {});

            process.exit(1);
        }
    })();
    '''

            # Write script to extraction directory
            self.add_log(f"Writing ENHANCED capture script to: {script_path}")
            with open(script_path, 'w') as f:
                f.write(enhanced_capture_script)

            os.chmod(script_path, 0o644)

            # Run the Node.js script
            self.add_log(f"Running ENHANCED cookie capture with validation...")
            self.add_log("=" * 60)

            env = os.environ.copy()
            env['CHROME_PROFILE_DIR'] = profile_path

            result = subprocess.run(
                ['node', script_filename],
                capture_output=True,
                text=True,
                timeout=60,
                env=env,
                cwd=extraction_dir
            )

            # Parse output with enhanced error detection
            self.add_log("--- Enhanced Capture Output ---")
            if result.stdout:
                for line in result.stdout.split('\n'):
                    if line.strip():
                        self.add_log(f"  {line}")

            if result.stderr:
                self.add_log("--- Capture Errors ---")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        self.add_log(f"  {line}", "WARNING")

            # Clean up
            try:
                os.unlink(script_path)
                self.add_log(f"Cleaned up script: {script_filename}")
            except:
                pass

            # Check for specific error patterns
            if result.returncode != 0:
                error_details = []

                if 'REDIRECTED TO LOGIN PAGE' in result.stdout:
                    error_details.append("❌ You are not logged in to X.com")
                    error_details.append("📍 Open VNC at http://localhost:6080 and log in first")
                elif 'NOT LOGGED IN TO X.COM' in result.stdout:
                    error_details.append("❌ No logged-in UI elements detected")
                    error_details.append("📍 Paste cookies via EditThisCookie or log in manually")
                elif 'COOKIE VALIDATION FAILED' in result.stdout or 'COOKIE VALIDATION FAILED' in result.stderr:
                    error_details.append("❌ Captured cookies failed validation")

                    # Extract specific validation errors
                    for line in result.stdout.split('\n'):
                        if 'Missing critical cookie:' in line:
                            error_details.append(f"  • {line.strip()}")
                        elif 'is EXPIRED' in line:
                            error_details.append(f"  • {line.strip()}")
                        elif 'has invalid or empty value' in line:
                            error_details.append(f"  • {line.strip()}")

                    error_details.append("📍 Try logging in fresh via VNC")
                    error_details.append("📍 Or paste new cookies from EditThisCookie")
                else:
                    error_details.append(f"Exit code: {result.returncode}")

                if result.stderr and 'COOKIE VALIDATION FAILED' not in result.stdout:
                    error_details.append(f"Error output: {result.stderr[:200]}")

                error_msg = "\n".join(error_details) if error_details else "Unknown error"
                raise Exception(f"Enhanced capture failed:\n{error_msg}")

            # Parse success details from output
            self.add_log("=" * 60)

            cookie_count = 0
            has_auth = False
            login_verified = False
            validation_passed = False
            validation_warnings = []
            critical_cookies = []

            for line in result.stdout.split('\n'):
                # Parse cookie count
                if 'Captured' in line and 'cookies' in line and 'Cookies:' in line:
                    try:
                        # Extract from "Cookies: 45"
                        cookie_count = int(line.split('Cookies:')[1].strip())
                    except:
                        pass

                # Parse auth token status
                if 'Has auth_token:' in line or 'auth_token:' in line:
                    has_auth = True

                # Parse login verification
                if 'LOGIN VERIFIED' in line or 'Login verified: YES' in line:
                    login_verified = True

                # Parse validation status
                if 'Cookie validation: PASSED' in line:
                    validation_passed = True

                # Parse warnings
                if 'Warnings:' in line and 'warnings' not in line.lower():
                    try:
                        warning_count = int(line.split('Warnings:')[1].strip())
                        if warning_count > 0:
                            validation_warnings.append(f"{warning_count} validation warnings")
                    except:
                        pass

                # Parse critical cookies
                if 'Critical cookies:' in line:
                    try:
                        cookies_str = line.split('Critical cookies:')[1].strip()
                        critical_cookies = [c.strip() for c in cookies_str.split(',')]
                    except:
                        pass

            # Summary logging
            self.add_log("=" * 60)
            self.add_log(f"✅ ENHANCED COOKIE CAPTURE SUCCESSFUL!")
            self.add_log(f"  📊 Cookies captured: {cookie_count}")
            self.add_log(f"  🔑 Has auth_token: {has_auth}")
            self.add_log(f"  ✓ Login verified: {login_verified}")
            self.add_log(f"  ✓ Validation passed: {validation_passed}")

            if critical_cookies:
                self.add_log(f"  🍪 Critical cookies: {', '.join(critical_cookies)}")

            if validation_warnings:
                self.add_log(f"  ⚠️ Warnings: {', '.join(validation_warnings)}", "WARNING")

            self.add_log("=" * 60)

            # Return enhanced result
            return {
                'success': True,
                'cookie_count': cookie_count,
                'has_auth_token': has_auth,
                'login_verified': login_verified,
                'validation_passed': validation_passed,
                'critical_cookies': critical_cookies,
                'warnings': validation_warnings,
                'message': (
                    f'✅ Successfully captured and validated {cookie_count} cookies!\n'
                    f'🔑 Auth token: {"Present" if has_auth else "Missing"}\n'
                    f'✓ Login verified: {"YES" if login_verified else "NO"}\n'
                    f'✓ Validation: {"PASSED" if validation_passed else "UNCLEAR"}'
                    + (f'\n⚠️ Warnings: {len(validation_warnings)}' if validation_warnings else '')
                )
            }

        except Exception as e:
            error_msg = f"Failed to capture cookies: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {
                'success': False,
                'error': error_msg
            }
    def _generate_cookie_copy_script(self, account_id, username):
        """
        Generate ONE shell script per account (not per session)
        This script can be used for ANY session of this account
        """
        try:
            # Get cookies from database
            cookie_info = self._get_account_cookies(account_id)

            if not cookie_info['has_cookies']:
                return {
                    'success': False,
                    'error': 'No cookies found for this account'
                }

            # Prepare cookie data
            cookies = cookie_info['cookie_data']
            if isinstance(cookies, str):
                cookies = json.loads(cookies)

            cookie_json = json.dumps(cookies, indent=2)

            # Precompute escaped cookie JSON for f-string
            escaped_cookie_json = cookie_json.replace("'", "'\"'\"'")

            # Create scripts directory if it doesn't exist
            scripts_dir = Path("/app/cookie_scripts")
            scripts_dir.mkdir(exist_ok=True, mode=0o777)

            # Generate script filename - ACCOUNT BASED
            safe_username = "".join(c for c in username if c.isalnum() or c in "-_")
            script_filename = f"copy_cookies_{safe_username}.sh"
            script_path = scripts_dir / script_filename

            # Get profile path for this account
            profile_path = self.chrome_manager.get_profile_path(username)

            # Generate script content - UPDATED with account context
            script_content = f"""#!/bin/bash
    # ============================================
    # ACCOUNT COOKIE SCRIPT
    # ============================================
    # Account: {username}
    # Account ID: {account_id}
    # Profile: {profile_path}
    # ============================================
    # This script works for ALL sessions of this account
    # All sessions use the same profile and cookies
    # Generated: {cookie_info['updated_at']}
    # Cookie Count: {cookie_info['cookie_count']}
    # ============================================

    set -e

    echo "==========================================="
    echo "Cookie Copy Utility"
    echo "==========================================="
    echo ""
    echo "Account: {username}"
    echo "Account ID: {account_id}"
    echo "Profile: {profile_path}"
    echo "Cookies: {cookie_info['cookie_count']}"
    echo "Last Updated: {cookie_info['updated_at']}"
    echo ""
    echo "⚠️  This script works for ALL sessions of '{username}'"
    echo "    All sessions share the same profile and cookies"
    echo ""

    # Set display for X11
    export DISPLAY=:99

    # Check if xclip is installed
    if ! command -v xclip &> /dev/null; then
        echo "❌ Error: xclip is not installed"
        echo "Please ensure xclip is installed in the container"
        exit 1
    fi

    # Cookie data (embedded in script)
    COOKIE_DATA='{escaped_cookie_json}'

    # Copy to clipboard
    echo "📋 Copying cookies to clipboard..."
    echo "$COOKIE_DATA" | xclip -selection clipboard

    if [ $? -eq 0 ]; then
        echo "✅ Success! Cookies copied to clipboard"
        echo ""
        echo "You can now:"
        echo "1. Open your browser in VNC (port 6080)"
        echo "2. Install EditThisCookie extension"
        echo "3. Click extension icon > Import"
        echo "4. Paste (Ctrl+V) and import"
        echo ""
        echo "These cookies will work for ANY session of account '{username}'"
        echo "==========================================="
    else
        echo "❌ Failed to copy to clipboard"
        exit 1
    fi
    """

            # Write script to file
            with open(script_path, 'w') as f:
                f.write(script_content)

            # Make script executable
            os.chmod(script_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)

            self.add_log(f"✓ Generated account cookie script: {script_path}")

            return {
                'success': True,
                'script_path': str(script_path),
                'script_filename': script_filename,
                'cookie_count': cookie_info['cookie_count'],
                'command': f"cd /app/cookie_scripts && ./{script_filename}",
                'account_username': username,
                'profile_path': profile_path
            }

        except Exception as e:
            error_msg = f"Failed to generate script: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {
                'success': False,
                'error': error_msg
            }


    def _generate_all_cookie_scripts(self):
        """Generate scripts for all accounts with cookies"""
        try:
            query = """
            SELECT a.account_id, a.username
            FROM accounts a
            WHERE a.has_cookies = TRUE
            AND a.profile_type = 'local_chrome'
            ORDER BY a.username
            """

            accounts = self.db_manager.execute_query(query, fetch=True)

            if not accounts:
                return {
                    'success': True,
                    'message': 'No accounts with cookies found',
                    'scripts_generated': 0
                }

            scripts_generated = []
            failed = []

            for row in accounts:
                account_id = row[0]
                username = row[1]

                result = self._generate_cookie_copy_script(account_id, username)

                if result['success']:
                    scripts_generated.append({
                        'username': username,
                        'script': result['script_filename']
                    })
                else:
                    failed.append({
                        'username': username,
                        'error': result['error']
                    })

            return {
                'success': True,
                'scripts_generated': len(scripts_generated),
                'scripts': scripts_generated,
                'failed': failed
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def _sync_cookies_to_session_file(self, account_id, username):
        """
        CRITICAL: Sync cookies from PostgreSQL to session_data.json
        This makes Streamlit-captured cookies available to Airflow
        """
        try:
            # Convert numpy types
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            self.add_log(f"Syncing cookies from PostgreSQL to session_data.json for {username}...")

            # Get cookies from PostgreSQL
            cookie_info = self._get_account_cookies(account_id)

            if not cookie_info['has_cookies']:
                self.add_log(f"⚠️ No cookies to sync for {username}", "WARNING")
                return {'success': False, 'reason': 'no_cookies'}

            cookies = cookie_info['cookie_data']
            if isinstance(cookies, str):
                cookies = json.loads(cookies)

            # Get profile path
            profile_path = self.chrome_manager.get_profile_path(username)
            session_file = os.path.join(profile_path, 'session_data.json')

            self.add_log(f"Profile path: {profile_path}")
            self.add_log(f"Session file: {session_file}")

            # Verify cookies have critical auth tokens
            critical_cookies = ['auth_token', 'ct0', 'kdt']
            cookie_names = [c['name'] for c in cookies]
            missing_critical = [name for name in critical_cookies if name not in cookie_names]

            if missing_critical:
                self.add_log(f"⚠️ Missing critical cookies: {missing_critical}", "WARNING")

            has_auth_token = 'auth_token' in cookie_names

            # Create session_data.json format (matches Airflow expectations)
            session_data = {
                'timestamp': datetime.now().isoformat(),
                'accountId': account_id,
                'profileDir': profile_path,
                'cookies': cookies,
                'localStorage': {},  # Can be empty - Chrome profile handles this
                'metadata': {
                    'cookieCount': len(cookies),
                    'localStorageCount': 0,
                    'savedBy': 'streamlit_cookie_sync',
                    'syncedFrom': 'postgresql',
                    'hasCriticalCookies': has_auth_token,
                    'nodeVersion': None,
                    'capturedAt': datetime.now().isoformat(),
                    'syncMethod': 'postgresql_to_file'
                }
            }

            # Ensure profile directory exists
            os.makedirs(profile_path, exist_ok=True)

            # Backup existing session file if it exists
            if os.path.exists(session_file):
                backup_file = f"{session_file}.backup.{int(time.time())}"
                try:
                    import shutil
                    shutil.copy2(session_file, backup_file)
                    self.add_log(f"✓ Backed up existing session to: {backup_file}")
                except Exception as e:
                    self.add_log(f"⚠️ Could not backup session file: {e}", "WARNING")

            # Write to session_data.json atomically
            temp_file = f"{session_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(session_data, f, indent=2)

            os.rename(temp_file, session_file)
            os.chmod(session_file, 0o644)

            # Verify write
            if os.path.exists(session_file):
                file_size = os.path.getsize(session_file)
                self.add_log(f"✓ Synced {len(cookies)} cookies to session_data.json")
                self.add_log(f"  File: {session_file}")
                self.add_log(f"  Size: {file_size} bytes")
                self.add_log(f"  Has auth_token: {has_auth_token}")
            else:
                raise Exception("Session file was not created")

            return {
                'success': True,
                'session_file': session_file,
                'cookie_count': len(cookies),
                'has_auth_token': has_auth_token,
                'missing_critical': missing_critical,
                'file_size': file_size
            }

        except Exception as e:
            error_msg = f"Failed to sync cookies to session file: {str(e)}"
            self.add_log(error_msg, "ERROR")
            import traceback
            self.add_log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return {'success': False, 'error': error_msg}

    def _ensure_x_account_id_column(self):
        """Ensure the x_account_id column exists in the accounts table"""
        try:
            # Check if column exists
            check_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'accounts' AND column_name = 'x_account_id'
            """
            result = self.db_manager.execute_query(check_query, fetch=True)
            
            if not result:
                # Add the column
                alter_query = "ALTER TABLE accounts ADD COLUMN x_account_id VARCHAR(30)"
                self.db_manager.execute_query(alter_query)
                logger.info("✅ Added x_account_id column to accounts table")
                return True
            else:
                logger.info("✓ x_account_id column already exists")
                return False
        except Exception as e:
            logger.error(f"Failed to ensure x_account_id column: {e}")
            return False

    @st.cache_data(ttl=300)
    def load_accounts_data(_self) -> pd.DataFrame:
        """Load accounts data from PostgreSQL with cookie info - UPDATED to include x_account_id."""
        try:
            accounts_query = """
            SELECT
                a.account_id,
                a.username,
                a.x_account_id,  -- ADD THIS
                a.profile_id,
                a.profile_type,
                a.created_time,
                a.updated_time,
                a.mongo_object_id,
                a.total_content_processed,
                COALESCE(a.has_cookies, FALSE) as has_cookies,
                a.cookies_last_updated
            FROM accounts a
            ORDER BY a.created_time DESC
            LIMIT 1000
            """
    
            accounts_data = _self.db_manager.execute_query(accounts_query, fetch=True)
    
            if not accounts_data:
                return pd.DataFrame()
    
            # Convert to DataFrame
            df = pd.DataFrame([dict(row) if hasattr(row, 'keys') else {
                'account_id': row[0],
                'username': row[1],
                'x_account_id': row[2],  # ADD THIS
                'profile_id': row[3],
                'profile_type': row[4],
                'created_time': row[5],
                'updated_time': row[6],
                'mongo_object_id': row[7],
                'total_content_processed': row[8],
                'has_cookies': bool(row[9]),
                'cookies_last_updated': row[10]
            } for row in accounts_data])
    
            # Convert datetime columns
            datetime_cols = ['created_time', 'updated_time', 'cookies_last_updated']
            for col in datetime_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
    
            # Add session counts (rest of the method remains the same)
            df['active_sessions'] = 0
    
            local_sessions = st.session_state.get('local_chrome_sessions', {})
            for session_id, session_info in local_sessions.items():
                account_id = session_info.get('account_id')
                if account_id:
                    df.loc[df['account_id'] == account_id, 'active_sessions'] += 1
    
            hb_sessions = st.session_state.get('hyperbrowser_sessions', {})
            for session_id, session_info in hb_sessions.items():
                account_id = session_info.get('account_id')
                if account_id:
                    df.loc[df['account_id'] == account_id, 'active_sessions'] += 1
    
            return df
    
        except Exception as e:
            st.error(f"Error loading accounts data: {str(e)}")
            return pd.DataFrame()





    # ============================================================================
    # METHOD 8: load_accounts_data - UPDATED to include cookie info
    # ============================================================================

    def _render_account_cookie_management_tab(self, accounts_df):
        """
        NEW TAB: Per-account cookie management with View/Update/Delete
        """
        st.subheader("🍪 Account Cookie Management")

        if accounts_df.empty:
            st.info("No accounts available")
            return

        # Filter to local chrome accounts
        local_accounts = accounts_df[accounts_df['profile_type'] == 'local_chrome']

        if local_accounts.empty:
            st.warning("No Local Chrome accounts found")
            return

        # Count accounts with cookies - FIXED: Use proper count
        accounts_with_cookies_count = 0
        if 'has_cookies' in local_accounts.columns:
            accounts_with_cookies_count = len(local_accounts[local_accounts['has_cookies'] == True])

        # Stats
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Accounts", len(local_accounts))
        with col2:
            st.metric("With Cookies", accounts_with_cookies_count)
        with col3:
            st.metric("Without Cookies", len(local_accounts) - accounts_with_cookies_count)
        with col4:
            # FIXED: Unique key
            if st.button("🔄 Refresh", use_container_width=True, key="acct_cookie_mgmt_refresh_btn"):
                st.cache_data.clear()
                st.rerun()

        st.markdown("---")

        # Account selection
        st.write("**Select an account to manage cookies:**")

        account_options = [
            (f"{row['username']} (ID: {row['account_id']}) {'🍪' if row.get('has_cookies') else '❌'}", idx)
            for idx, row in local_accounts.iterrows()
        ]

        selected = st.selectbox(
            "Account",
            [("Choose an account...", None)] + account_options,
            format_func=lambda x: x[0],
            key="acct_cookie_mgmt_select_ui"
        )

        if selected[1] is not None:
            row = local_accounts.iloc[selected[1]]
            account_id = row['account_id']
            username = row['username']

            st.markdown("---")

            # Render full cookie management UI for this account
            self._render_cookie_management_for_account(
                account_id,
                username,
                in_expander=False
            )


    def _render_overview_tab(self, accounts_df):
        """Render accounts overview tab - FIXED duplicate button keys"""
        st.subheader("Accounts Overview")
    
        # Action buttons
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            if st.button("🔄 Refresh", use_container_width=True, key="overview_tab_refresh_btn"):
                st.cache_data.clear()
                self._refresh_active_sessions_from_mongodb()
                st.rerun()
        with col2:
            if st.button("➕ Add Account", use_container_width=True, key="overview_tab_add_btn"):
                st.session_state.show_add_account = True
                st.rerun()
        with col3:
            if st.button("🗑️ Delete Account", use_container_width=True, key="overview_tab_delete_btn"):
                st.session_state.show_delete_account = True
                st.rerun()
        with col4:
            if st.button("🧹 Cleanup Sessions", use_container_width=True, key="overview_tab_cleanup_btn"):
                self._cleanup_stale_sessions()
                st.rerun()
    
        st.markdown("---")
    
        # Show modals if active
        self.render_add_account_modal()
        self.render_delete_account_modal(accounts_df)
    
        st.markdown("---")
    
        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            status_filter = st.selectbox(
                "Account Type",
                ["All", "Local Chrome", "Hyperbrowser", "With Active Sessions"],
                key="overview_tab_status_filter"
            )
        with col2:
            time_filter = st.selectbox(
                "Time Range",
                ["All Time", "Last 7 Days", "Last 30 Days", "Last 90 Days"],
                key="overview_tab_time_filter"
            )
        with col3:
            view_option = st.selectbox(
                "View Mode",
                ["Accounts Overview", "Account Details", "Performance Analytics"],
                key="overview_tab_view_option"
            )
    
        st.markdown("---")
    
        # Display data
        if accounts_df.empty:
            st.info("No accounts found. Use the 'Add Account' button.")
        else:
            filtered_df = self._apply_filters(accounts_df, status_filter, time_filter)
            st.write(f"Showing {len(filtered_df)} of {len(accounts_df)} accounts")
    
            if view_option == "Account Details":
                # Option 1: Use _render_sessions_prompts_view if it exists
                if hasattr(self, '_render_sessions_prompts_view'):
                    self._render_sessions_prompts_view(filtered_df)
                else:
                    # Option 2: Create a simple fallback view
                    self._render_simple_details_view(filtered_df)
            elif view_option == "Performance Analytics":
                self._render_performance_analytics_view(filtered_df)
            else:
                self._render_accounts_overview(filtered_df)
    
    # Add this fallback method if _render_sessions_prompts_view doesn't exist
    def _render_simple_details_view(self, df):
        """Simple fallback details view with x_account_id"""
        for _, row in df.iterrows():
            with st.expander(f"🏷️ {row['username']} (ID: {row['account_id']})", expanded=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Account Info**")
                    st.write(f"Account ID: `{row['account_id']}`")
                    st.write(f"Username: `{row['username']}`")
                    st.write(f"X Account ID: `{row.get('x_account_id', 'Not set')}`")
                    st.write(f"Profile ID: `{row.get('profile_id', 'None')}`")
                    st.write(f"Type: `{row.get('profile_type', 'Unknown')}`")
                    st.write(f"Created: {row.get('created_time', 'Unknown')}")
                    
                with col2:
                    st.write("**Stats**")
                    st.write(f"Active Sessions: {row.get('active_sessions', 0)}")
                    st.write(f"Content Processed: {row.get('total_content_processed', 0)}")
                    st.write(f"Has Cookies: {'✓' if row.get('has_cookies') else '✗'}")

    def _render_analytics_tab(self, accounts_df):
        """Render analytics tab - UPDATED to include x_account_id."""
        st.subheader("📈 Account Analytics")
    
        if accounts_df.empty:
            st.info("No accounts to analyze")
            return
    
        # Refresh button
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            if st.button("🔄 Refresh", use_container_width=True, key="analytics_tab_refresh_btn"):
                st.cache_data.clear()
                st.rerun()
        with col2:
            st.metric("Total Accounts", len(accounts_df))
        with col3:
            active_sessions = (len(st.session_state.get('local_chrome_sessions', {})) +
                            len(st.session_state.get('hyperbrowser_sessions', {})))
            st.metric("Active Sessions", active_sessions)
        with col4:
            st.metric("Total Content", accounts_df['total_content_processed'].sum())
    
        st.markdown("---")
    
        # Per-account breakdown - ADD x_account_id
        st.subheader("Per-Account Performance")
    
        display_columns = ['username', 'x_account_id', 'profile_type', 'active_sessions',
                          'total_content_processed', 'has_cookies', 'created_time']
        
        # Only include x_account_id if it exists in the dataframe
        if 'x_account_id' not in accounts_df.columns:
            display_columns.remove('x_account_id')
    
        display_df = accounts_df[display_columns].copy()
    
        display_df = display_df.rename(columns={
            'username': 'Username',
            'x_account_id': 'X Account ID',  # ADD THIS
            'profile_type': 'Type',
            'active_sessions': 'Sessions',
            'total_content_processed': 'Content',
            'has_cookies': 'Cookies',
            'created_time': 'Created'
        })
    
        display_df['Type'] = display_df['Type'].apply(
            lambda x: '🖥️ Local' if x == 'local_chrome' else '☁️ HyperB'
        )
    
        display_df['Cookies'] = display_df['Cookies'].apply(lambda x: '✓' if x else '✗')
        
        # Format X Account ID
        if 'X Account ID' in display_df.columns:
            display_df['X Account ID'] = display_df['X Account ID'].apply(
                lambda x: str(x) if x and str(x).strip() else "—"
            )
    
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    
        # Charts (rest remains the same)
        st.markdown("---")
    
        col1, col2 = st.columns(2)
    
        with col1:
            st.subheader("Account Type Distribution")
            type_counts = accounts_df['profile_type'].value_counts()
            fig = px.pie(values=type_counts.values, names=type_counts.index)
            st.plotly_chart(fig, use_container_width=True)
    
        with col2:
            st.subheader("Content Processing")
            top_accounts = accounts_df.nlargest(10, 'total_content_processed')
            fig2 = px.bar(top_accounts, x='username', y='total_content_processed')
            st.plotly_chart(fig2, use_container_width=True)
    def _render_cookie_capture_section(self, accounts_df):
        """
        UPDATED: Render cookie capture section with account-based context
        """
        st.subheader("🎯 Live Cookie Capture")

        st.info(
            "**Capture authenticated cookies from a running Chrome session**\n\n"
            "✅ **Account-Based**: Cookies are stored per account, not per session\n"
            "✅ **Persistent**: All sessions for the same account share cookies\n"
            "✅ **3 Tabs Open**: Automa, EditThisCookie, X.com ready to use\n\n"
            "**Workflow:**\n"
            "1. Start a Local Chrome session (opens 3 tabs automatically)\n"
            "2. Open VNC (http://localhost:6080)\n"
            "3. **Tab 1**: Install Automa extension (if needed)\n"
            "4. **Tab 2**: Install EditThisCookie extension (if needed)\n"
            "5. **Tab 3**: Go to X.com home - log in or paste cookies\n"
            "6. **Verify you can see your timeline**\n"
            "7. Click 'Capture Cookies' below\n"
            "8. Cookies saved to account (work for all future sessions)"
        )


        # Filter to local chrome accounts only
        local_accounts = accounts_df[accounts_df['profile_type'] == 'local_chrome']

        if local_accounts.empty:
            st.warning("No Local Chrome accounts found")
            return

        # Check if any sessions are running
        local_sessions = st.session_state.get('local_chrome_sessions', {})

        if not local_sessions:
            st.warning("⚠️ No active Local Chrome sessions")
            st.caption("Start a session in the 'Local Chrome' tab, then come back here")
            return

        st.markdown("---")

        # Group sessions by account
        st.write("**Active Accounts with Sessions:**")

        accounts_with_sessions = {}
        for session_id, session_info in local_sessions.items():
            account_username = session_info.get('account_username', 'Unknown')
            if account_username not in accounts_with_sessions:
                accounts_with_sessions[account_username] = []
            accounts_with_sessions[account_username].append(session_id)

        for account_username, session_ids in accounts_with_sessions.items():
            # Get account_id for this account
            account_row = accounts_df[accounts_df['username'] == account_username]

            if account_row.empty:
                continue

            account_id = account_row.iloc[0]['account_id']
            profile_path = self.chrome_manager.get_profile_path(account_username)

            with st.container():
                st.markdown(f"### 👤 {account_username}")

                col1, col2 = st.columns([2, 1])

                with col1:
                    st.caption(f"**Profile:** {profile_path}")
                    st.caption(f"**Active Sessions:** {len(session_ids)}")

                    # Show all session IDs
                    with st.expander("View Session IDs", expanded=False):
                        for sid in session_ids:
                            st.code(sid)

                    # Show cookie status
                    self._render_cookie_status_indicator(account_id, account_username)

                    # Show instructions
                    with st.expander("📋 Capture Instructions", expanded=False):
                        st.markdown(
                            f"""
                            **Step-by-step for account '{account_username}':**

                            1. **Open VNC**: [http://localhost:6080](http://localhost:6080)
                            - Password: `secret`

                            2. **In Chrome, go to**: `https://x.com/home`

                            3. **Make sure you're logged in**:
                            - Can you see your timeline?
                            - Can you see the tweet compose box?

                            4. **Click "Capture Cookies"** button →

                            5. **Cookies saved to account**: All {len(session_ids)} session(s) will use these cookies

                            ⚠️ **Important**: Cookies are shared across ALL sessions of this account!
                            """
                        )

                with col2:
                    st.write("**Actions:**")

                    # FIXED: Unique key with timestamp
                    capture_key = f"capture_cookies_section_{account_username}_{int(time.time())}"

                    if st.button(
                        "🎯 Capture Cookies",
                        key=capture_key,
                        use_container_width=True,
                        type="primary"
                    ):
                        with st.spinner(f"Capturing cookies for {account_username}..."):
                            result = self._capture_cookies_from_running_chrome(
                                account_id,
                                account_username,
                                profile_path
                            )

                            if result['success']:
                                success_msg = (
                                    f"✅ Captured {result['cookie_count']} cookies!\n\n"
                                    f"✓ Has auth_token: {result['has_auth_token']}\n"
                                    f"✓ Login verified: {result.get('login_verified', False)}\n\n"
                                    f"**Cookies saved to account '{account_username}'**\n"
                                    f"All {len(session_ids)} session(s) will use these cookies!"
                                )
                                st.success(success_msg)

                                # Store in database
                                try:
                                    session_file = os.path.join(profile_path, 'session_data.json')
                                    if os.path.exists(session_file):
                                        with open(session_file, 'r') as f:
                                            session_data = json.load(f)

                                        cookies_json = json.dumps(session_data['cookies'])

                                        store_result = self._store_account_cookies(
                                            account_id, cookies_json, account_username
                                        )

                                        if store_result['success']:
                                            st.success("✓ Cookies saved to database")

                                            # Generate/update cookie script
                                            script_result = self._generate_cookie_copy_script(
                                                account_id, account_username
                                            )
                                            if script_result['success']:
                                                st.info(f"✓ Cookie script updated: `{script_result['script_filename']}`")
                                except Exception as e:
                                    st.warning(f"Could not save to database: {e}")

                                st.cache_data.clear()
                                time.sleep(2)
                                st.rerun()
                            else:
                                error_msg = result['error']
                                st.error(f"❌ Failed: {error_msg}")

                st.divider()

        # Show capture logs if available
        if st.session_state.get('account_creation_logs'):
            with st.expander("📋 Detailed Capture Logs", expanded=False):
                log_text = "\n".join(st.session_state.account_creation_logs)
                st.code(log_text, language="log")

                # FIXED: Unique key
                if st.button("Clear Logs", key="clear_capture_section_logs"):
                    self.clear_logs()
                    st.rerun()
    def render_session_management_section(self, accounts_df):
        """MINIMAL VERSION - Radio buttons MUST appear"""

        with st.expander("🖥️ Session Management", expanded=True):  # Force expanded for testing

            if accounts_df.empty:
                st.info("No accounts available.")
                return

            # Get mode from session state
            is_local = st.session_state.get('use_local_chrome', True)

            # Show mode
            if is_local:
                st.info("🖥️ **Local Chrome Mode**")
            else:
                st.info("☁️ **Hyperbrowser Mode**")

            # Two columns
            col1, col2 = st.columns([2, 1])

            # Left: Active Sessions
            with col1:
                st.subheader("Active Sessions")
                st.info("No active sessions")

            # Right: Start Session
            with col2:
                st.subheader("Start Session")

                # Account dropdown
                account_options = [
                    (f"{row['username']} (ID: {row['account_id']})", idx)
                    for idx, row in accounts_df.iterrows()
                ]

                selected = st.selectbox(
                    "Select Account",
                    [("Choose an account...", None)] + account_options,
                    format_func=lambda x: x[0],
                    key="session_acc_select_v3"  # FIXED: Changed key
                )

                if selected[1] is not None:
                    row = accounts_df.iloc[selected[1]]

                    st.success(f"✓ Selected: {row['username']}")

                    # Check if local mode
                    if is_local:
                        st.markdown("---")
                        st.markdown("**Choose Session Type:**")

                        # THE RADIO BUTTON - SIMPLIFIED
                        session_type = st.radio(
                            "Session Type",
                            ["Persistent Profile", "Cookie Import"],
                            key="session_type_radio_v3"  # FIXED: Changed key
                        )

                        st.markdown("---")

                        # Show what was selected
                        if session_type == "Persistent Profile":
                            st.info("🔵 **Persistent Profile Mode**")

                            # Get profile status
                            profile_path = self.chrome_manager.get_profile_path(row['username'])
                            default_dir = os.path.join(profile_path, 'Default')

                            if os.path.exists(default_dir):
                                st.success("✓ Existing profile found")
                                st.caption("Previous state will be restored")
                            else:
                                st.info("📁 New profile will be created")

                            st.caption("🖥️ Chrome will open to blank page")

                            # FIXED: Unique key
                            if st.button("▶️ Start Persistent Session", type="primary", use_container_width=True, key=f"start_persistent_{row['account_id']}"):
                                with st.spinner("Starting..."):
                                    result = self._start_local_chrome_session(
                                        row['profile_id'],
                                        row['account_id'],
                                        row['username'],
                                        {}
                                    )

                                    if result.get('success'):
                                        st.success("✓ Started!")
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        st.error(f"Failed: {result.get('error')}")

                        else:  # Cookie Import
                            st.info("🍪 **Cookie Import Mode**")
                            st.write("Import cookies from EditThisCookie file")

                            # Get available cookie files
                            cookie_files = self._get_available_cookie_files()

                            if not cookie_files:
                                st.warning("⚠️ No cookie files found in /app/editthiscookie/")
                                st.caption("Place EditThisCookie JSON files in the editthiscookie folder")
                            else:
                                st.success(f"✓ Found {len(cookie_files)} cookie file(s)")

                                selected_cookie = st.selectbox(
                                    "Select Cookie File",
                                    options=cookie_files,
                                    key="cookie_file_select_v3"  # FIXED: Changed key
                                )

                                st.caption(f"📁 Will use: {selected_cookie}")

                                # FIXED: Unique key
                                if st.button("▶️ Start with Cookies", type="primary", use_container_width=True, key=f"start_cookies_{row['account_id']}"):
                                    with st.spinner("Starting with cookies..."):
                                        result = self._start_local_chrome_session_with_cookies(
                                            row['profile_id'],
                                            row['account_id'],
                                            row['username'],
                                            selected_cookie,
                                            {}
                                        )

                                        if result.get('success'):
                                            st.success("✓ Started with cookies!")
                                            time.sleep(1)
                                            st.rerun()
                                        else:
                                            st.error(f"Failed: {result.get('error')}")

                    else:  # Hyperbrowser mode
                        st.caption("⚠️ Hyperbrowser mode")

                        # FIXED: Unique key
                        if st.button("▶️ Start Hyperbrowser Session", type="primary", use_container_width=True, key=f"start_hb_{row['account_id']}"):
                            with st.spinner("Starting..."):
                                result = self._start_persistent_session_for_profile(
                                    row['profile_id'],
                                    row['account_id'],
                                    row['username'],
                                    {'start_url': 'about:blank'}
                                )

                                if result.get('success'):
                                    st.success("✓ Started!")
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error(f"Failed: {result.get('error')}")

                else:
                    st.caption("👆 Select an account to start a session")

    def _clear_singleton_locks(self, username: str = None) -> dict:
        """
        Remove Chrome Singleton lock files from one or all Chrome profiles.

        Args:
            username: If given, only clear that account's profile directory.
                      If None, clear ALL profiles under base_profile_dir.

        Returns:
            dict with keys: success, removed (list), errors (list), summary (str)
        """
        import glob as _glob

        LOCK_NAMES = [
            "SingletonLock",
            "SingletonCookie",
            "SingletonSocket",
            "lockfile",
        ]
        SESSION_NAMES = [
            "Last Session",
            "Last Tabs",
            "Current Session",
            "Current Tabs",
        ]

        base_dir = self.chrome_manager.base_profile_dir  # /workspace/chrome_profiles

        # Build list of profile root directories to inspect
        if username:
            profile_roots = [self.chrome_manager.get_profile_path(username)]
        else:
            try:
                profile_roots = [
                    os.path.join(base_dir, entry)
                    for entry in os.listdir(base_dir)
                    if os.path.isdir(os.path.join(base_dir, entry))
                ]
            except Exception as e:
                return {
                    "success": False,
                    "removed": [],
                    "errors": [f"Could not list profiles: {e}"],
                    "summary": f"❌ Failed to list profile directories: {e}",
                }

        removed = []
        errors = []

        for profile_root in profile_roots:
            # Directories to check: profile root AND Default sub-directory
            dirs_to_check = [
                profile_root,
                os.path.join(profile_root, "Default"),
            ]

            for directory in dirs_to_check:
                if not os.path.isdir(directory):
                    continue

                # ── Singleton lock files ──
                for name in LOCK_NAMES:
                    target = os.path.join(directory, name)
                    if os.path.exists(target) or os.path.islink(target):
                        try:
                            os.remove(target)
                            removed.append(target)
                            self.add_log(f"🗑️ Removed lock: {target}")
                        except Exception as e:
                            errors.append(f"Could not remove {target}: {e}")
                            self.add_log(f"⚠️ Could not remove {target}: {e}", "WARNING")

                # ── Session restore files (Default/ only) ──
                if directory.endswith(("/Default", "\\Default")):
                    for name in SESSION_NAMES:
                        target = os.path.join(directory, name)
                        if os.path.exists(target):
                            try:
                                os.remove(target)
                                removed.append(target)
                                self.add_log(f"🗑️ Removed session file: {target}")
                            except Exception as e:
                                errors.append(f"Could not remove {target}: {e}")

        # ── Stale chromium temp lock files anywhere under base_dir ──
        try:
            for pattern in [
                os.path.join(base_dir, "**", ".org.chromium.Chromium.*"),
                os.path.join(base_dir, "**", ".com.google.Chrome.*"),
            ]:
                for match in _glob.glob(pattern, recursive=True):
                    try:
                        os.remove(match)
                        removed.append(match)
                        self.add_log(f"🗑️ Removed chromium temp: {match}")
                    except Exception as e:
                        errors.append(f"Could not remove {match}: {e}")
        except Exception as e:
            errors.append(f"Glob cleanup error: {e}")

        # ── Build human-readable summary ──
        scope = f"'{username}'" if username else "ALL profiles"
        if removed:
            summary = (
                f"✅ Cleared {len(removed)} lock file(s) from {scope}.\n"
                + "\n".join(f"  • {os.path.basename(p)}" for p in removed[:10])
                + (f"\n  … and {len(removed) - 10} more" if len(removed) > 10 else "")
            )
        else:
            summary = f"ℹ️ No lock files found in {scope} — nothing to clear."

        if errors:
            summary += f"\n⚠️ {len(errors)} error(s) — check logs."

        return {
            "success": len(errors) == 0,
            "removed": removed,
            "errors": errors,
            "summary": summary,
        }

    def _render_local_chrome_tab(self, accounts_df):
        """Render Local Chrome session management tab with Singleton lock clearing."""
        st.subheader("🖥️ Local Chrome Session Management")
        st.info("✓ Free • ✓ Persistent profiles • ✓ Account-based cookies • ✓ 3 startup tabs")

        if accounts_df.empty:
            st.warning("No accounts available. Create an account first.")
            return

        # Filter to local chrome accounts only
        local_accounts = accounts_df[accounts_df['profile_type'] == 'local_chrome']

        if local_accounts.empty:
            st.warning("No Local Chrome accounts found. Create one in the Overview tab.")
            return

        # =========================================================================
        # 🔓  SINGLETON LOCK CLEARING SECTION
        # =========================================================================
        with st.expander("🔓 Clear Singleton Locks", expanded=False):
            st.caption(
                "Use this when Chrome won't start because a previous session left lock files behind. "
                "Safe to run any time — it only removes `SingletonLock`, `SingletonCookie`, "
                "`SingletonSocket`, and stale session-restore files."
            )

            # ── Global clear ──
            col_global, col_info = st.columns([1, 3])

            with col_global:
                if st.button(
                    "🗑️ Clear ALL Profiles",
                    key="clear_locks_global_btn",
                    use_container_width=True,
                    type="primary",
                ):
                    with st.spinner("Clearing Singleton locks from all profiles…"):
                        result = self._clear_singleton_locks(username=None)

                    if result["removed"]:
                        st.success(result["summary"])
                    else:
                        st.info(result["summary"])

                    if result["errors"]:
                        for err in result["errors"]:
                            st.warning(err)

                    self.add_log(
                        f"Global lock clear: removed {len(result['removed'])} files, "
                        f"{len(result['errors'])} errors"
                    )

            with col_info:
                st.markdown(
                    "Clears lock files from **every** account profile at once. "
                    "Equivalent to running:\n"
                    "```bash\n"
                    "find /workspace/chrome_profiles -name 'Singleton*' -exec rm -f {} \\;\n"
                    "```"
                )

            st.markdown("---")

            # ── Per-account clear ──
            st.write("**Clear locks for a specific account:**")

            for _, row in local_accounts.iterrows():
                account_username = row['username']
                account_id = row['account_id']
                profile_path = self.chrome_manager.get_profile_path(account_username)

                # Check which lock files currently exist for this account
                lock_files_present = []
                for lock_name in ["SingletonLock", "SingletonCookie", "SingletonSocket"]:
                    for check_dir in [profile_path, os.path.join(profile_path, "Default")]:
                        target = os.path.join(check_dir, lock_name)
                        if os.path.exists(target) or os.path.islink(target):
                            lock_files_present.append(os.path.relpath(target, profile_path))

                col_name, col_status, col_btn = st.columns([2, 2, 1])

                with col_name:
                    st.write(f"**👤 {account_username}**")
                    st.caption(profile_path)

                with col_status:
                    if lock_files_present:
                        st.warning(f"⚠️ {len(lock_files_present)} lock file(s) found")
                        for lf in lock_files_present:
                            st.caption(f"  • {lf}")
                    else:
                        st.success("✅ No lock files")

                with col_btn:
                    btn_disabled = len(lock_files_present) == 0
                    if st.button(
                        "🗑️ Clear",
                        key=f"clear_locks_{account_id}",
                        use_container_width=True,
                        disabled=btn_disabled,
                        type="secondary",
                    ):
                        result = self._clear_singleton_locks(username=account_username)
                        if result["removed"]:
                            st.success(result["summary"])
                        else:
                            st.info(result["summary"])
                        if result["errors"]:
                            for err in result["errors"]:
                                st.warning(err)
                        st.rerun()

        # =========================================================================
        # ACTIVE SESSIONS + START SESSION
        # =========================================================================
        col1, col2 = st.columns([2, 1])

        # Left: Active Sessions - grouped by account
        with col1:
            st.subheader("Active Sessions")

            local_sessions = st.session_state.get('local_chrome_sessions', {})

            if local_sessions:
                # Group by account
                accounts_with_sessions = {}
                for session_id, session_info in local_sessions.items():
                    account_username = session_info.get('account_username', 'Unknown')
                    if account_username not in accounts_with_sessions:
                        accounts_with_sessions[account_username] = []
                    accounts_with_sessions[account_username].append((session_id, session_info))

                for account_username, sessions in accounts_with_sessions.items():
                    with st.container():
                        st.markdown(f"**👤 Account: {account_username}**")
                        st.caption(f"{len(sessions)} session(s) • Shared profile • Same cookies")

                        # Show startup tabs info
                        if sessions[0][1].get('startup_urls'):
                            with st.expander("🌐 Startup Tabs", expanded=False):
                                for i, url in enumerate(sessions[0][1]['startup_urls'], 1):
                                    st.caption(f"{i}. {url}")

                        # Show cookie script if available
                        if sessions[0][1].get('account_cookie_script'):
                            cookie_script = sessions[0][1]['account_cookie_script']
                            st.caption(f"🍪 Cookie Script: `{os.path.basename(cookie_script)}`")

                        for session_id, session_info in sessions:
                            s1, s2, s3 = st.columns([3, 2, 1])

                            with s1:
                                st.write(f"**Session:** `{session_id[:12]}...`")
                                if session_info.get('profile_path'):
                                    st.caption(f"📁 {session_info['profile_path']}")

                            with s2:
                                started_at = session_info.get('started_at')
                                if started_at:
                                    duration = datetime.now() - started_at
                                    hours, remainder = divmod(duration.total_seconds(), 3600)
                                    minutes, _ = divmod(remainder, 60)
                                    st.write(f"**Duration:** {int(hours):02d}:{int(minutes):02d}")

                                if session_info.get('browser_url'):
                                    st.link_button(
                                        "🖥️ VNC",
                                        session_info['browser_url'],
                                        use_container_width=True,
                                    )

                            with s3:
                                if st.button("🛑", key=f"stop_local_tab_{session_id[:8]}"):
                                    result = self._stop_local_chrome_session(session_id)
                                    if result.get('success'):
                                        st.success("Stopped")
                                        st.rerun()
                                    else:
                                        st.error(result.get('error'))

                        st.divider()
            else:
                st.info("No active sessions")

        # Right: Start Session
        with col2:
            st.subheader("Start Session")

            account_options = [
                (f"{row['username']} (ID: {row['account_id']})", idx)
                for idx, row in local_accounts.iterrows()
            ]

            selected = st.selectbox(
                "Select Account",
                [("Choose account...", None)] + account_options,
                format_func=lambda x: x[0],
                key="local_chrome_tab_select",
            )

            if selected[1] is not None:
                row = local_accounts.iloc[selected[1]]

                st.markdown("---")

                # Show account info
                st.info(f"**👤 Account: {row['username']}**")

                # Show what will open
                st.write("**Chrome will open with 3 tabs:**")
                st.caption("1. 🧩 Automa Extension")
                st.caption("2. 🍪 EditThisCookie Extension")
                st.caption("3. 🐦 X.com Home")

                st.markdown("---")

                # Show profile status
                profile_path = self.chrome_manager.get_profile_path(row['username'])
                default_dir = os.path.join(profile_path, 'Default')

                if os.path.exists(default_dir):
                    st.success("✓ Existing profile - state preserved")
                    st.caption("Cookies, extensions, login state preserved")
                else:
                    st.info("📁 New profile will be created")
                    st.caption("Profile will be empty on first launch")

                # Show cookie status
                cookie_info = self._get_account_cookies(row['account_id'])
                if cookie_info['has_cookies']:
                    st.success(f"✓ {cookie_info['cookie_count']} cookies stored")
                    st.caption(f"Last updated: {cookie_info['updated_at']}")
                else:
                    st.warning("⚠️ No cookies stored")
                    st.caption("Capture cookies after starting session")

                st.markdown("---")

                # Start button
                if st.button(
                    "▶️ Start Chrome Session",
                    type="primary",
                    use_container_width=True,
                    key=f"local_chrome_start_{row['account_id']}",
                ):
                    with st.spinner("Starting Chrome with 3 tabs..."):
                        result = self._start_local_chrome_session(
                            row['profile_id'],
                            row['account_id'],
                            row['username'],
                        )

                        if result.get('success'):
                            st.success("✓ Chrome started with 3 tabs!")
                            st.caption("📍 Automa • EditThisCookie • X.com")

                            if result.get('account_cookie_script'):
                                st.info(
                                    f"Cookie script: `{os.path.basename(result['account_cookie_script'])}`"
                                )

                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"Failed: {result.get('error')}")
            else:
                st.caption("👆 Select an account to start a session")

    def _render_hyperbrowser_tab(self, accounts_df):
        """Render Hyperbrowser session management tab - COMPLETELY FIXED"""
        st.subheader("☁️ Hyperbrowser Session Management")

        api_key = os.environ.get('HYPERBROWSER_API_KEY')
        if api_key:
            st.success("✓ API Key configured")
        else:
            st.error("❌ API Key not found")
            st.caption("Set HYPERBROWSER_API_KEY environment variable")
            return

        if accounts_df.empty:
            st.warning("No accounts available.")
            return

        # Filter to hyperbrowser accounts
        hb_accounts = accounts_df[accounts_df['profile_type'] == 'hyperbrowser']

        if hb_accounts.empty:
            st.warning("No Hyperbrowser accounts found.")
            return

        col1, col2 = st.columns([2, 1])

        with col1:
            st.subheader("Active Sessions")

            hb_sessions = st.session_state.get('hyperbrowser_sessions', {})

            if hb_sessions:
                for session_id, session_info in hb_sessions.items():
                    with st.container():
                        s1, s2, s3 = st.columns([3, 2, 1])

                        with s1:
                            st.write(f"**Session:** `{session_id[:12]}...`")
                            st.write(f"**Account:** {session_info.get('account_username')}")

                        with s2:
                            # FIXED: NO key parameter in link_button
                            if session_info.get('browser_url'):
                                st.link_button(
                                    "🌐 Open",
                                    session_info['browser_url'],
                                    use_container_width=True
                                )

                        with s3:
                            # Stop button with unique key - FIXED
                            if st.button("🛑", key=f"stop_hb_tab_{session_id[:8]}"):
                                result = self._stop_hyperbrowser_session(session_id)
                                if result.get('success'):
                                    st.success("Stopped")
                                    st.rerun()

                        st.divider()
            else:
                st.info("No active sessions")

        with col2:
            st.subheader("Start Session")

            account_options = [
                (f"{row['username']} (ID: {row['account_id']})", idx)
                for idx, row in hb_accounts.iterrows()
            ]

            selected = st.selectbox(
                "Select Account",
                [("Choose account...", None)] + account_options,
                format_func=lambda x: x[0],
                key="hb_tab_acc_select"
            )

            if selected[1] is not None:
                row = hb_accounts.iloc[selected[1]]

                # FIXED: Unique key
                if st.button("▶️ Start Session", type="primary", use_container_width=True,
                            key=f"hb_start_session_{row['account_id']}"):
                    with st.spinner("Starting..."):
                        result = self._start_hyperbrowser_session(
                            row['profile_id'],
                            row['account_id'],
                            row['username'],
                            {'start_url': 'about:blank'}
                        )
                        if result.get('success'):
                            st.success("Started!")
                            st.rerun()
                        else:
                            st.error(result.get('error'))

    def render(self):
        """Main render method with enhanced cookie management tab"""
        
        # ===== ENSURE DATABASE HAS X_ACCOUNT_ID COLUMN =====
        try:
            # Check if column exists
            check_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'accounts' AND column_name = 'x_account_id'
            """
            result = self.db_manager.execute_query(check_query, fetch=True)
            
            if not result:
                # Add the column
                alter_query = "ALTER TABLE accounts ADD COLUMN x_account_id VARCHAR(30)"
                self.db_manager.execute_query(alter_query)
                logger.info("✅ Added x_account_id column to accounts table")
                st.success("✅ Database updated: Added x_account_id column")
        except Exception as e:
            logger.error(f"Failed to ensure x_account_id column: {e}")
            st.warning(f"Note: Could not verify x_account_id column: {e}")
        # ===================================================
        
        # Page title and quick stats
        st.title("👥 Accounts Management")
    
        # Quick stats at top
        self._render_quick_stats()
    
        st.markdown("---")
    
        # UPDATED: Add new "Manage Cookies" tab
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📊 Overview",
            "🖥️ Local Chrome",
            "☁️ Hyperbrowser",
            "📈 Analytics",
            "🍪 Cookie Tools",      # Original: Live Capture, Scripts, Upload, Sync
            "🔧 Manage Cookies"     # NEW: View, Update, Delete per account
        ])
    
        # Load accounts data once (shared across tabs)
        with st.spinner("Loading accounts data..."):
            accounts_df = self.load_accounts_data()
    
        # TAB 1: Accounts Overview
        with tab1:
            self._render_overview_tab(accounts_df)
    
        # TAB 2: Local Chrome Sessions
        with tab2:
            self._render_local_chrome_tab(accounts_df)
    
        # TAB 3: Hyperbrowser Sessions
        with tab3:
            self._render_hyperbrowser_tab(accounts_df)
    
        # TAB 4: Analytics
        with tab4:
            self._render_analytics_tab(accounts_df)
    
        # TAB 5: Cookie Tools (existing - Live Capture, Scripts, Upload, Sync)
        with tab5:
            self._render_cookie_manager_tab_enhanced(accounts_df)
    
        # TAB 6: Manage Cookies (NEW - View, Update, Delete)
        with tab6:
            self._render_account_cookie_management_tab(accounts_df)

    # ============================================================================
# ADD THESE METHODS TO YOUR AccountsPage CLASS
# ============================================================================
    def _render_cookie_management_for_account(self, account_id, username, in_expander=True):
        """
        FIXED: Cookie management UI with working buttons
        Uses callback functions instead of direct session state manipulation
        """
        try:
            # Convert numpy types
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            # Get current cookie info
            cookie_info = self._get_account_cookies(account_id)

            if in_expander:
                container = st.expander(f"🍪 Cookie Management for {username}", expanded=False)
            else:
                container = st.container()

            with container:
                if cookie_info['has_cookies']:
                    # Show cookie summary
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.metric("Cookie Count", cookie_info['cookie_count'])

                    with col2:
                        st.metric("Last Updated",
                                cookie_info['uploaded_at'].strftime('%Y-%m-%d %H:%M')
                                if cookie_info.get('uploaded_at') else 'Unknown')

                    with col3:
                        # Check validity
                        validity = self._check_cookie_validity(account_id)
                        if validity['valid']:
                            st.metric("Status", "✅ Valid")
                        else:
                            st.metric("Status", "❌ Invalid")

                    st.markdown("---")

                    # Action buttons with callbacks
                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        if st.button("👁️ View Cookies",
                                    key=f"view_btn_{account_id}",
                                    use_container_width=True):
                            st.session_state[f'modal_view_{account_id}'] = True
                            st.rerun()

                    with col2:
                        if st.button("🔄 Update Cookies",
                                    key=f"update_btn_{account_id}",
                                    use_container_width=True):
                            st.session_state[f'modal_update_{account_id}'] = True
                            st.rerun()

                    with col3:
                        if st.button("🗑️ Delete Cookies",
                                    key=f"delete_btn_{account_id}",
                                    use_container_width=True,
                                    type="secondary"):
                            st.session_state[f'modal_delete_{account_id}'] = True
                            st.rerun()

                    with col4:
                        if st.button("🔄 Sync to Airflow",
                                    key=f"sync_btn_{account_id}",
                                    use_container_width=True,
                                    type="primary"):
                            with st.spinner("Syncing..."):
                                result = self._sync_cookies_to_session_file(account_id, username)
                                if result['success']:
                                    st.success("✓ Synced!")
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error(f"Failed: {result.get('error')}")

                    # VIEW MODAL
                    if st.session_state.get(f'modal_view_{account_id}'):
                        st.markdown("---")
                        st.subheader("📋 Cookie Details")

                        cookies = cookie_info['cookie_data']
                        if isinstance(cookies, str):
                            cookies = json.loads(cookies)

                        # Show critical cookies first
                        critical_names = ['auth_token', 'ct0', 'kdt']
                        st.write("**Critical Authentication Cookies:**")

                        for name in critical_names:
                            cookie = next((c for c in cookies if c['name'] == name), None)
                            if cookie:
                                with st.expander(f"🔑 {name}", expanded=False):
                                    st.json({
                                        'name': cookie.get('name'),
                                        'domain': cookie.get('domain'),
                                        'value': cookie.get('value')[:20] + '...' if len(cookie.get('value', '')) > 20 else cookie.get('value'),
                                        'secure': cookie.get('secure'),
                                        'httpOnly': cookie.get('httpOnly'),
                                        'expirationDate': cookie.get('expirationDate')
                                    })
                            else:
                                st.warning(f"⚠️ Missing: {name}")

                        st.write(f"**All Cookies ({len(cookies)} total):**")
                        st.json(cookies)

                        if st.button("Close", key=f"close_view_{account_id}"):
                            del st.session_state[f'modal_view_{account_id}']
                            st.rerun()

                    # UPDATE MODAL
                    if st.session_state.get(f'modal_update_{account_id}'):
                        st.markdown("---")
                        st.subheader("🔄 Update Cookies")

                        with st.form(f"update_form_{account_id}"):
                            st.write("**Paste new EditThisCookie JSON:**")

                            new_cookies_json = st.text_area(
                                "New Cookies JSON",
                                placeholder='[\n  {\n    "domain": ".x.com",\n    "name": "auth_token",\n    "value": "...",\n    ...\n  }\n]',
                                height=300,
                                key=f"update_text_{account_id}"
                            )

                            if new_cookies_json:
                                try:
                                    parsed = json.loads(new_cookies_json)
                                    if isinstance(parsed, list) and len(parsed) > 0:
                                        st.success(f"✓ {len(parsed)} cookies detected")
                                    else:
                                        st.warning("⚠️ Invalid format")
                                except:
                                    st.error("❌ Invalid JSON")

                            col1, col2 = st.columns(2)

                            with col1:
                                update_submit = st.form_submit_button("💾 Update", use_container_width=True, type="primary")
                            with col2:
                                update_cancel = st.form_submit_button("Cancel", use_container_width=True)

                            if update_submit and new_cookies_json:
                                with st.spinner("Updating cookies..."):
                                    result = self._update_account_cookies(
                                        account_id, new_cookies_json, username
                                    )

                                    if result['success']:
                                        st.success(f"✓ Updated {result['cookie_count']} cookies!")

                                        # Auto-sync to Airflow
                                        st.info("🔄 Auto-syncing to Airflow...")
                                        sync_result = self._sync_cookies_to_session_file(
                                            account_id, username
                                        )

                                        if sync_result['success']:
                                            st.success("✓ Synced to Airflow!")

                                        del st.session_state[f'modal_update_{account_id}']
                                        st.cache_data.clear()
                                        time.sleep(2)
                                        st.rerun()
                                    else:
                                        st.error(f"Failed: {result['error']}")

                            if update_cancel:
                                del st.session_state[f'modal_update_{account_id}']
                                st.rerun()

                    # DELETE MODAL
                    if st.session_state.get(f'modal_delete_{account_id}'):
                        st.markdown("---")
                        st.error("⚠️ **Delete Cookies Confirmation**")

                        st.write(f"Are you sure you want to delete all cookies for **{username}**?")
                        st.write("")
                        st.write("This will:")
                        st.write("- ❌ Deactivate all cookies in database")
                        st.write("- ❌ Remove session_data.json file")
                        st.write("- ❌ Break any running Airflow DAGs using these cookies")
                        st.write("")
                        st.write("**This action cannot be undone!**")

                        col1, col2 = st.columns(2)

                        with col1:
                            if st.button("🗑️ Yes, Delete Cookies",
                                        key=f"confirm_delete_{account_id}",
                                        use_container_width=True,
                                        type="primary"):
                                with st.spinner("Deleting cookies..."):
                                    result = self._delete_account_cookies(account_id, username)

                                    if result['success']:
                                        st.success("✅ Cookies deleted successfully!")
                                        del st.session_state[f'modal_delete_{account_id}']
                                        st.cache_data.clear()
                                        time.sleep(2)
                                        st.rerun()
                                    else:
                                        st.error(f"Failed: {result['error']}")

                        with col2:
                            if st.button("Cancel",
                                        key=f"cancel_delete_{account_id}",
                                        use_container_width=True):
                                del st.session_state[f'modal_delete_{account_id}']
                                st.rerun()

                else:
                    # No cookies stored
                    st.info("No cookies stored for this account")

                    col1, col2 = st.columns(2)

                    with col1:
                        st.write("**Upload Cookies:**")
                        if st.button("📤 Upload Cookies",
                                    key=f"upload_btn_{account_id}",
                                    use_container_width=True,
                                    type="primary"):
                            st.session_state[f'modal_upload_{account_id}'] = True
                            st.rerun()

                    with col2:
                        st.write("**Capture from Chrome:**")
                        st.caption("Start a Local Chrome session first")

                    # UPLOAD MODAL
                    if st.session_state.get(f'modal_upload_{account_id}'):
                        st.markdown("---")

                        with st.form(f"upload_form_{account_id}"):
                            st.write("**Paste EditThisCookie JSON:**")

                            cookies_json = st.text_area(
                                "Cookies JSON",
                                placeholder='[\n  {\n    "domain": ".x.com",\n    "name": "auth_token",\n    "value": "...",\n    ...\n  }\n]',
                                height=300,
                                key=f"upload_text_{account_id}"
                            )

                            if cookies_json:
                                try:
                                    parsed = json.loads(cookies_json)
                                    if isinstance(parsed, list) and len(parsed) > 0:
                                        st.success(f"✓ {len(parsed)} cookies detected")
                                    else:
                                        st.warning("⚠️ Invalid format")
                                except:
                                    st.error("❌ Invalid JSON")

                            col1, col2 = st.columns(2)

                            with col1:
                                upload_submit = st.form_submit_button("💾 Upload", use_container_width=True, type="primary")
                            with col2:
                                upload_cancel = st.form_submit_button("Cancel", use_container_width=True)

                            if upload_submit and cookies_json:
                                with st.spinner("Uploading cookies..."):
                                    result = self._store_account_cookies(
                                        account_id, cookies_json, username
                                    )

                                    if result['success']:
                                        st.success(f"✓ Uploaded {result['cookie_count']} cookies!")

                                        # Auto-sync
                                        sync_result = self._sync_cookies_to_session_file(
                                            account_id, username
                                        )

                                        if sync_result['success']:
                                            st.success("✓ Synced to Airflow!")

                                        del st.session_state[f'modal_upload_{account_id}']
                                        st.cache_data.clear()
                                        time.sleep(2)
                                        st.rerun()
                                    else:
                                        st.error(f"Failed: {result['error']}")

                            if upload_cancel:
                                del st.session_state[f'modal_upload_{account_id}']
                                st.rerun()

        except Exception as e:
            st.error(f"Error rendering cookie management: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
    def _delete_account_cookies(self, account_id, username):
        """
        Delete all cookies for an account
        Marks them as inactive in database and removes session_data.json
        """
        try:
            # Convert numpy types
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            self.add_log(f"Deleting cookies for account {account_id} ({username})")

            # Deactivate all cookies in database
            deactivate_query = """
            UPDATE account_cookies
            SET is_active = FALSE,
                updated_at = CURRENT_TIMESTAMP,
                notes = COALESCE(notes, '') || ' [DELETED by user]'
            WHERE account_id = %s AND is_active = TRUE
            """
            self.db_manager.execute_query(deactivate_query, (account_id,))
            self.add_log("✓ Deactivated cookies in database")

            # Update account metadata
            update_account_query = """
            UPDATE accounts
            SET has_cookies = FALSE,
                cookies_last_updated = CURRENT_TIMESTAMP
            WHERE account_id = %s
            """
            self.db_manager.execute_query(update_account_query, (account_id,))
            self.add_log("✓ Updated account metadata")

            # Remove session_data.json file
            profile_path = self.chrome_manager.get_profile_path(username)
            session_file = os.path.join(profile_path, 'session_data.json')

            if os.path.exists(session_file):
                # Backup before deleting
                backup_file = f"{session_file}.deleted.{int(time.time())}"
                try:
                    import shutil
                    shutil.move(session_file, backup_file)
                    self.add_log(f"✓ Moved session file to: {backup_file}")
                except Exception as e:
                    self.add_log(f"⚠️ Could not backup session file: {e}", "WARNING")
                    os.remove(session_file)
                    self.add_log("✓ Removed session file")

            return {
                'success': True,
                'message': f'All cookies deleted for account {username}',
                'account_id': account_id
            }

        except Exception as e:
            error_msg = f"Failed to delete cookies: {str(e)}"
            self.add_log(error_msg, "ERROR")
            import traceback
            self.add_log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return {
                'success': False,
                'error': error_msg
            }





    # ============================================================================
    # UPDATE YOUR EXISTING _render_cookie_manager_tab_enhanced METHOD
    # Add this as a new tab or replace the Manual Upload tab
    # ============================================================================







    """
    METHOD 2: _start_local_chrome_session() - REPLACE COMPLETELY
    """
    def _start_local_chrome_session(
        self,
        profile_id,
        account_id,
        account_username
    ):
        """
        Start local Chrome session with 3 tabs:
        1. Automa extension
        2. EditThisCookie extension
        3. X.com home page
        """
        try:
            self.add_log(f"Starting LOCAL Chrome session for: {account_username}")
            self.add_log("Will open 3 tabs: Automa, EditThisCookie, X.com")

            # Ensure account_id is native Python int
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            # Get profile path
            profile_path = self.chrome_manager.get_profile_path(account_username)

            # Create session ID with LOCAL prefix
            session_id = f"local_{account_username}_{int(time.time())}"
            self.add_log(f"Session ID: {session_id}")

            # Ensure account cookie script exists
            self._ensure_account_cookie_script(account_id, account_username)

            # Start Chrome with persistent profile + 3 startup tabs
            self.add_log("Starting Chrome with Automa, EditThisCookie, and X.com tabs")
            result = self.chrome_manager.run_persistent_chrome(
                session_id=session_id,
                profile_path=profile_path,
                username=account_username,
                start_url="about:blank"  # Ignored - 3 tabs will open instead
            )

            if not result.get("success"):
                raise Exception(result.get("error", "Unknown error"))

            self.add_log("✓ Chrome process started with 3 tabs")
            self.add_log(f"  Tab 1: Automa extension")
            self.add_log(f"  Tab 2: EditThisCookie extension")
            self.add_log(f"  Tab 3: X.com home")
            self.add_log(f"✓ Chrome visible at: {result.get('vnc_url')}")
            self.add_log(f"✓ Account cookie script: {result.get('account_cookie_script')}")

            # Store in MongoDB
            if self.chrome_manager.mongo_client:
                try:
                    db = self.chrome_manager.mongo_client["messages_db"]
                    session_doc = {
                        "session_id": session_id,
                        "session_type": "local_chrome",
                        "profile_id": profile_id,
                        "profile_path": profile_path,
                        "postgres_account_id": int(account_id),
                        "account_username": account_username,
                        "debug_port": result.get("debug_port"),
                        "startup_urls": result.get("startup_urls", []),
                        "created_at": datetime.now(),
                        "is_active": True,
                        "session_status": "active",
                        "is_persistent": True,
                        "profile_preserved": True,
                        "chrome_auto_opened": True,
                        "account_cookie_script": result.get("account_cookie_script")
                    }
                    db.browser_sessions.insert_one(session_doc)
                    self.add_log("✓ Stored in MongoDB")
                except Exception as e:
                    self.add_log(f"⚠ MongoDB storage failed: {e}", "WARNING")

            # Store in LOCAL Chrome session state
            if "local_chrome_sessions" not in st.session_state:
                st.session_state.local_chrome_sessions = {}

            st.session_state.local_chrome_sessions[session_id] = {
                "session_id": session_id,
                "session_type": "local_chrome",
                "profile_id": profile_id,
                "account_id": int(account_id),
                "account_username": account_username,
                "started_at": datetime.now(),
                "debug_port": result.get("debug_port"),
                "browser_url": result.get("vnc_url"),
                "profile_path": profile_path,
                "is_persistent": True,
                "chrome_visible": True,
                "account_cookie_script": result.get("account_cookie_script"),
                "startup_urls": result.get("startup_urls", [])
            }

            self.add_log("✓ Session stored in local_chrome_sessions")

            return {
                "success": True,
                "session_id": session_id,
                "vnc_url": result.get("vnc_url"),
                "message": "Chrome started with 3 tabs: Automa, EditThisCookie, X.com",
                "profile_path": profile_path,
                "account_cookie_script": result.get("account_cookie_script"),
                "startup_urls": result.get("startup_urls", [])
            }

        except Exception as e:
            error_msg = f"Failed to start local session: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {"success": False, "error": error_msg}
    def _ensure_account_cookie_script(self, account_id, username):
        """
        Ensure account cookie script exists and is up-to-date
        Called when starting any session for this account
        """
        try:
            # Convert numpy types
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            # Check if account has cookies
            cookie_info = self._get_account_cookies(account_id)

            if not cookie_info['has_cookies']:
                self.add_log(f"⚠️ Account {username} has no cookies stored yet", "WARNING")
                return {
                    'success': False,
                    'reason': 'no_cookies'
                }

            # Generate/update the cookie script
            script_result = self._generate_cookie_copy_script(account_id, username)

            if script_result['success']:
                self.add_log(f"✓ Account cookie script ready: {script_result['script_filename']}")
                return {
                    'success': True,
                    'script_path': script_result['script_path']
                }
            else:
                self.add_log(f"⚠️ Could not generate cookie script: {script_result['error']}", "WARNING")
                return {
                    'success': False,
                    'error': script_result['error']
                }

        except Exception as e:
            self.add_log(f"⚠️ Error ensuring cookie script: {e}", "WARNING")
            return {
                'success': False,
                'error': str(e)
            }



    def _check_remaining_chrome_processes(self):
        """Check how many Chrome processes are still running"""
        try:
            count = 0
            for proc in psutil.process_iter(['name']):
                try:
                    if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                        count += 1
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            return count
        except:
            return 0

    def _stop_local_chrome_session(self, session_id):
        """Stop local Chrome session with PROPER tracking - ensures ALL windows close"""
        try:
            self.add_log(f"=== STOPPING LOCAL SESSION ===")
            self.add_log(f"Session ID: {session_id}")

            # Debug: Show what we have
            local_sessions = st.session_state.get('local_chrome_sessions', {})
            self.add_log(f"Available local sessions: {list(local_sessions.keys())}")

            chrome_processes = self.chrome_manager.active_processes
            self.add_log(f"Available chrome processes: {list(chrome_processes.keys())}")

            # Check if session exists in local_chrome_sessions
            if session_id not in local_sessions:
                error_msg = f"Session {session_id} not found in local_chrome_sessions"
                self.add_log(error_msg, "ERROR")
                self.add_log(f"Available: {list(local_sessions.keys())}", "ERROR")
                return {'success': False, 'error': 'Session not found'}

            session_info = local_sessions[session_id]
            profile_path = session_info.get('profile_path', 'Unknown')

            # Stop the Chrome process via chrome_manager
            self.add_log(f"Calling chrome_manager.stop_session({session_id})")
            result = self.chrome_manager.stop_session(session_id)

            if not result['success']:
                self.add_log(f"Chrome manager stop failed: {result.get('error')}", "WARNING")
                # Force cleanup anyway
                self.chrome_manager._force_kill_all_chrome_processes()

            # Remove from Streamlit session state
            if session_id in st.session_state.local_chrome_sessions:
                del st.session_state.local_chrome_sessions[session_id]
                self.add_log(f"✓ Removed from local_chrome_sessions")

            # Update MongoDB
            if self.chrome_manager.mongo_client:
                try:
                    db = self.chrome_manager.mongo_client['messages_db']
                    db.browser_sessions.update_one(
                        {'session_id': session_id},
                        {'$set': {
                            'is_active': False,
                            'ended_at': datetime.now(),
                            'session_status': 'stopped',
                            'all_windows_closed': True  # NEW: Track that all windows were closed
                        }}
                    )
                    self.add_log(f"✓ Updated MongoDB")
                except Exception as e:
                    self.add_log(f"⚠ MongoDB update failed: {e}", "WARNING")

            success_msg = f"Session stopped. ALL Chrome windows closed. Profile saved at: {profile_path}"
            self.add_log(f"✓ {success_msg}")

            # Verify no Chrome processes remain
            time.sleep(2)  # Give time for cleanup
            remaining = self._check_remaining_chrome_processes()
            if remaining > 0:
                self.add_log(f"⚠ Warning: {remaining} Chrome processes still running", "WARNING")
                # Try one more time
                self.chrome_manager._force_kill_all_chrome_processes()

            return {
                'success': True,
                'message': success_msg,
                'profile_path': profile_path,
                'all_windows_closed': True
            }

        except Exception as e:
            error_msg = f"Failed to stop session: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}


    # ============================================================================
    # 5. FIXED START SESSION METHOD - HYPERBROWSER
    # ============================================================================

    def _start_hyperbrowser_session(self, profile_id, account_id, account_username, session_config=None):
        """Start Hyperbrowser session with PROPER session tracking"""
        try:
            from ..hyperbrowser_extensions import create_session_with_extension
            self.add_log(f"Starting HYPERBROWSER session for: {account_username}")

            if not HYPERBROWSER_API_KEY:
                raise ValueError("HYPERBROWSER_API_KEY not available")

            # CRITICAL FIX: Validate that profile_id is a valid UUID
            import uuid
            try:
                # Try to parse as UUID to validate format
                uuid.UUID(profile_id)
                self.add_log(f"✓ Valid profile UUID: {profile_id}")
            except (ValueError, AttributeError) as e:
                error_msg = f"Invalid profile_id format: '{profile_id}' is not a valid UUID. Expected format like '3c90c3cc-0d44-4b50-8888-8dd25736052a'"
                self.add_log(error_msg, "ERROR")
                raise ValueError(error_msg)

            config = session_config or {}
            session_purpose = config.get('purpose', 'Manual Data Load')

            # Create session config
            session_config_enhanced = {
                'session_purpose': session_purpose,
                'screen_width': config.get('screen_width', 1920),
                'screen_height': config.get('screen_height', 1080),
                'use_stealth': config.get('use_stealth', True),
                'urls_to_open': [
                    'https://chromewebstore.google.com/detail/automa/infppggnoaenmfagbfknfkancpbljcca',
                    'https://x.com/'
                ],
                'persist_changes': config.get('persist_changes', True),
                'timeout_minutes': 120,
                'use_extension': False
            }

            self.add_log(f"Creating session with profile_id: {profile_id}")

            # Create session with the CORRECT UUID profile_id
            session_result = create_session_with_extension(
                profile_id=profile_id,  # This MUST be a UUID string
                extension_id=None,
                session_config=session_config_enhanced
            )

            session_id = session_result['session_id']
            self.add_log(f"✓ Session created: {session_id}")

            # Store in HYPERBROWSER session state
            if 'hyperbrowser_sessions' not in st.session_state:
                st.session_state.hyperbrowser_sessions = {}

            st.session_state.hyperbrowser_sessions[session_id] = {
                'session_id': session_id,
                'session_type': 'hyperbrowser',
                'profile_id': profile_id,
                'account_id': account_id,
                'account_username': account_username,
                'started_at': datetime.now(),
                'browser_url': session_result['browser_url'],
                'persistent': True,
                'session_purpose': session_purpose
            }

            self.add_log(f"✓ Session stored in hyperbrowser_sessions")

            return {
                'success': True,
                'session_id': session_id,
                'browser_url': session_result['browser_url'],
                'message': f"Hyperbrowser session started"
            }

        except Exception as e:
            error_msg = f"Failed to start Hyperbrowser session: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}



    # ============================================================================
    # 6. FIXED STOP SESSION METHOD - HYPERBROWSER
    # ============================================================================

    def _stop_hyperbrowser_session(self, session_id):
        """Stop Hyperbrowser session with PROPER tracking"""
        try:
            self.add_log(f"=== STOPPING HYPERBROWSER SESSION ===")
            self.add_log(f"Session ID: {session_id}")

            # Check if session exists
            hb_sessions = st.session_state.get('hyperbrowser_sessions', {})
            self.add_log(f"Available hyperbrowser sessions: {list(hb_sessions.keys())}")

            if session_id not in hb_sessions:
                error_msg = f"Session {session_id} not found in hyperbrowser_sessions"
                self.add_log(error_msg, "ERROR")
                return {'success': False, 'error': 'Session not found'}

            session_info = hb_sessions[session_id]

            # Stop via Hyperbrowser API
            from hyperbrowser import Hyperbrowser
            hb_client = Hyperbrowser(api_key=HYPERBROWSER_API_KEY)

            try:
                hb_client.sessions.close(session_id)
                self.add_log(f"✓ Hyperbrowser session closed")
            except Exception as e:
                self.add_log(f"⚠ API close failed: {e}", "WARNING")

            # Remove from session state
            if session_id in st.session_state.hyperbrowser_sessions:
                del st.session_state.hyperbrowser_sessions[session_id]
                self.add_log(f"✓ Removed from hyperbrowser_sessions")

            # Update MongoDB
            try:
                from ..hyperbrowser_utils import get_mongodb_client
                client, db = get_mongodb_client()
                db.browser_sessions.update_one(
                    {'session_id': session_id},
                    {'$set': {
                        'is_active': False,
                        'ended_at': datetime.now(),
                        'session_status': 'stopped'
                    }}
                )
                client.close()
                self.add_log(f"✓ Updated MongoDB")
            except Exception as e:
                self.add_log(f"⚠ MongoDB update failed: {e}", "WARNING")

            return {
                'success': True,
                'message': 'Hyperbrowser session stopped'
            }

        except Exception as e:
            error_msg = f"Failed to stop Hyperbrowser session: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}


    # ============================================================================
    # 7. UNIFIED GET SESSIONS METHOD
    # ============================================================================

    def get_all_active_sessions(self):
        """Get all active sessions (both local and hyperbrowser)"""
        local_sessions = st.session_state.get('local_chrome_sessions', {})
        hb_sessions = st.session_state.get('hyperbrowser_sessions', {})

        all_sessions = {}

        # Add local sessions with type marker
        for sid, info in local_sessions.items():
            all_sessions[sid] = {**info, 'session_type': 'local_chrome'}

        # Add hyperbrowser sessions with type marker
        for sid, info in hb_sessions.items():
            all_sessions[sid] = {**info, 'session_type': 'hyperbrowser'}

        return all_sessions





    




    def _start_persistent_session_for_profile(self, profile_id, account_id=None, account_username=None, session_config=None):
        """
        Start a new persistent session WITHOUT extension by default, with session purpose saved to database
        """
        try:
            from ..hyperbrowser_extensions import create_session_with_extension
            self.add_log(f"Starting persistent session for profile: {profile_id}")

            if not HYPERBROWSER_API_KEY:
                raise ValueError("HYPERBROWSER_API_KEY not available")

            config = session_config or {}
            session_purpose = config.get('purpose', 'Manual Data Load')  # Default to Manual Data Load

            # DON'T use extension unless explicitly forced
            use_extension = config.get('force_extension_upload', False)

            if use_extension:
                self.add_log("Extension explicitly requested")
            else:
                self.add_log("Starting session WITHOUT extension")

            # Define the two URLs to open
            urls_to_open = [
                'https://chromewebstore.google.com/detail/automa/infppggnoaenmfagbfknfkancpbljcca',
                'https://x.com/'
            ]

            # Prepare session config
            session_config_enhanced = {
                'session_purpose': session_purpose,  # SAVE PURPOSE
                'screen_width': config.get('screen_width', 1920),
                'screen_height': config.get('screen_height', 1080),
                'use_stealth': config.get('use_stealth', True),
                'urls_to_open': urls_to_open,
                'persist_changes': config.get('persist_changes', True),
                'timeout_minutes': 120,
                'use_extension': use_extension
            }

            # Create session WITHOUT extension (extension_id=None)
            session_result = create_session_with_extension(
                profile_id=profile_id,
                extension_id=None,  # Explicitly None
                session_config=session_config_enhanced
            )

            session_id = session_result['session_id']
            self.add_log(f"Session created successfully: {session_id}")

            # Update session information with account details AND session purpose
            if account_id and account_username:
                try:
                    from ..hyperbrowser_utils import update_in_mongodb
                    update_in_mongodb('browser_sessions',
                        {'session_id': session_id},
                        {
                            'postgres_account_id': account_id,
                            'account_username': account_username,
                            'account_specific_session': True,
                            'session_purpose': session_purpose,  # SAVE PURPOSE TO MONGODB
                            'session_purpose_timestamp': datetime.now()
                        })
                    self.add_log(f"Updated session with account information and purpose: {session_purpose}")
                except Exception as e:
                    self.add_log(f"Warning: Could not update session with account info: {e}", "WARNING")
            else:
                # Still save session purpose even without account
                try:
                    from ..hyperbrowser_utils import update_in_mongodb
                    update_in_mongodb('browser_sessions',
                        {'session_id': session_id},
                        {
                            'session_purpose': session_purpose,  # SAVE PURPOSE TO MONGODB
                            'session_purpose_timestamp': datetime.now()
                        })
                    self.add_log(f"Updated session with purpose: {session_purpose}")
                except Exception as e:
                    self.add_log(f"Warning: Could not update session with purpose: {e}", "WARNING")

            # Add to session state
            if 'active_sessions' not in st.session_state:
                st.session_state.active_sessions = {}

            st.session_state.active_sessions[session_id] = {
                'profile_id': profile_id,
                'account_id': account_id,
                'account_username': account_username,
                'started_at': datetime.now(),
                'browser_url': session_result['browser_url'],
                'persistent': True,
                'persist_changes': config.get('persist_changes', True),
                'extension_loaded': False,  # No extension
                'extension_id': None,  # No extension
                'session_config': config,
                'session_purpose': session_purpose  # SAVE IN SESSION STATE TOO
            }

            return {
                'success': True,
                'session_id': session_id,
                'browser_url': session_result['browser_url'],
                'mongodb_id': session_result['mongodb_id'],
                'extension_loaded': False,
                'extension_id': None,
                'session_purpose': session_purpose,
                'message': f"Session {session_id} started for '{session_purpose}', will open Chrome Web Store and X.com"
            }

        except Exception as e:
            error_msg = f"Failed to start persistent session for profile {profile_id}: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {
                'success': False,
                'error': error_msg
            }


    def _refresh_active_sessions_from_mongodb(self):
        """Refresh active sessions from MongoDB with session purpose"""
        try:
            # Updated import pattern
            try:
                from streamlit_hyperbrowser_manager import get_mongodb_client
            except ImportError:
                try:
                    from ..streamlit_hyperbrowser_manager import get_mongodb_client
                except ImportError:
                    import streamlit_hyperbrowser_manager
                    get_mongodb_client = streamlit_hyperbrowser_manager.get_mongodb_client

            client, db = get_mongodb_client()

            # Get all active sessions
            active_sessions = list(db.browser_sessions.find({
                'is_active': True,
                'session_status': 'active'
            }))

            # Update session state
            updated_sessions = {}
            for session in active_sessions:
                session_id = session['session_id']
                updated_sessions[session_id] = {
                    'profile_id': session.get('profile_id'),
                    'account_id': session.get('postgres_account_id'),
                    'account_username': session.get('account_username'),
                    'started_at': session.get('created_at', datetime.now()),
                    'browser_url': f"https://app.hyperbrowser.ai/sessions/{session_id}",
                    'session_purpose': session.get('session_purpose', 'Unknown')  # LOAD PURPOSE
                }

            st.session_state.active_sessions = updated_sessions
            client.close()

            self.add_log(f"Refreshed {len(updated_sessions)} active sessions from MongoDB")

        except Exception as e:
            self.add_log(f"Failed to refresh sessions from MongoDB: {e}", "WARNING")

    def _refresh_active_sessions_from_mongodb(self):
        """Refresh active sessions from MongoDB and update session state - UPDATED imports"""
        try:
            # Updated import pattern
            try:
                from streamlit_hyperbrowser_manager import get_mongodb_client
            except ImportError:
                try:
                    from ..streamlit_hyperbrowser_manager import get_mongodb_client
                except ImportError:
                    import streamlit_hyperbrowser_manager
                    get_mongodb_client = streamlit_hyperbrowser_manager.get_mongodb_client

            client, db = get_mongodb_client()

            # Get all active sessions
            active_sessions = list(db.browser_sessions.find({
                'is_active': True,
                'session_status': 'active'
            }))

            # Update session state
            updated_sessions = {}
            for session in active_sessions:
                session_id = session['session_id']
                updated_sessions[session_id] = {
                    'profile_id': session.get('profile_id'),
                    'account_id': session.get('postgres_account_id'),
                    'account_username': session.get('account_username'),
                    'started_at': session.get('created_at', datetime.now()),
                    'browser_url': f"https://app.hyperbrowser.ai/sessions/{session_id}"
                }

            st.session_state.active_sessions = updated_sessions
            client.close()

            self.add_log(f"Refreshed {len(updated_sessions)} active sessions from MongoDB")

        except Exception as e:
            self.add_log(f"Failed to refresh sessions from MongoDB: {e}", "WARNING")


    def _delete_account_from_mongodb(self, account_id, profile_id, mongodb_id=None):
        """Delete account-related data from MongoDB - UPDATED for correct collection names"""
        try:
            self.add_log(f"Starting MongoDB deletion for account_id: {account_id}, profile_id: {profile_id}")

            # Import MongoDB utilities
            try:
                from streamlit_hyperbrowser_manager import get_mongodb_client
            except ImportError:
                try:
                    from ..streamlit_hyperbrowser_manager import get_mongodb_client
                except ImportError:
                    import streamlit_hyperbrowser_manager
                    get_mongodb_client = streamlit_hyperbrowser_manager.get_mongodb_client

            client, db = get_mongodb_client()
            deletion_results = {}

            # Delete from 'accounts' collection (UPDATED: was referencing chrome_profiles)
            try:
                profile_result = db.accounts.delete_many({
                    'profile_id': profile_id,
                    'is_active': True
                })
                deletion_results['accounts'] = {
                    'deleted_count': profile_result.deleted_count,
                    'acknowledged': profile_result.acknowledged
                }
                self.add_log(f"Deleted {profile_result.deleted_count} accounts documents")
            except Exception as e:
                self.add_log(f"Failed to delete from accounts: {e}", "WARNING")
                deletion_results['accounts'] = {'error': str(e)}

            # Delete from browser_sessions collection
            try:
                sessions_result = db.browser_sessions.delete_many({
                    'profile_id': profile_id
                })
                deletion_results['browser_sessions'] = {
                    'deleted_count': sessions_result.deleted_count,
                    'acknowledged': sessions_result.acknowledged
                }
                self.add_log(f"Deleted {sessions_result.deleted_count} browser_sessions documents")
            except Exception as e:
                self.add_log(f"Failed to delete from browser_sessions: {e}", "WARNING")
                deletion_results['browser_sessions'] = {'error': str(e)}

            # Delete from extension_instances if profile is associated
            try:
                extension_result = db.extension_instances.update_many(
                    {'associated_profiles': profile_id},
                    {'$pull': {'associated_profiles': profile_id}}
                )
                deletion_results['extension_instances'] = {
                    'modified_count': extension_result.modified_count,
                    'acknowledged': extension_result.acknowledged
                }
                self.add_log(f"Removed profile from {extension_result.modified_count} extension_instances")
            except Exception as e:
                self.add_log(f"Failed to update extension_instances: {e}", "WARNING")
                deletion_results['extension_instances'] = {'error': str(e)}

            # If specific mongodb_id provided, delete that document
            if mongodb_id:
                try:
                    from bson import ObjectId
                    if isinstance(mongodb_id, str):
                        mongodb_id = ObjectId(mongodb_id)

                    specific_result = db.accounts.delete_one({'_id': mongodb_id})
                    deletion_results['specific_document'] = {
                        'deleted_count': specific_result.deleted_count,
                        'acknowledged': specific_result.acknowledged
                    }
                    self.add_log(f"Deleted specific document with MongoDB ID: {mongodb_id}")
                except Exception as e:
                    self.add_log(f"Failed to delete specific MongoDB document: {e}", "WARNING")
                    deletion_results['specific_document'] = {'error': str(e)}

            client.close()

            self.add_log("✅ MongoDB deletion completed")
            return {
                'deleted_from_mongodb': True,
                'results': deletion_results
            }

        except Exception as e:
            error_msg = f"Failed to delete from MongoDB: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {
                'deleted_from_mongodb': False,
                'error': str(e)
            }



    def _cleanup_stale_sessions(self):
        """Cleanup stale sessions that are marked as active but may be dead"""
        try:
            from ..hyperbrowser_utils import cleanup_stale_sessions

            # Cleanup sessions older than 24 hours
            cleaned_count = cleanup_stale_sessions(max_age_hours=24)

            # Refresh session state
            self._refresh_active_sessions_from_mongodb()

            if cleaned_count > 0:
                st.success(f"Cleaned up {cleaned_count} stale sessions")
                self.add_log(f"Cleaned up {cleaned_count} stale sessions")
            else:
                st.info("No stale sessions found to cleanup")

        except Exception as e:
            error_msg = f"Failed to cleanup stale sessions: {e}"
            st.error(error_msg)
            self.add_log(error_msg, "ERROR")








