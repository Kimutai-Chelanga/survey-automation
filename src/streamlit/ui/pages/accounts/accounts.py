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
from src.core.database.postgres import accounts as pg_accounts
from src.streamlit.ui.pages.accounts.chrome_session_manager import ChromeSessionManager
import os
import logging
import tempfile
import stat
from pathlib import Path
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AccountsPage:
    """Accounts management page for survey automation."""

    def __init__(self, db_manager):
        """Initialize the AccountsPage with database manager"""
        self.db_manager = db_manager

        # Session state defaults
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
        if 'country_to_create' not in st.session_state:
            st.session_state.country_to_create = None
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

        # Local Chrome sessions only
        if 'local_chrome_sessions' not in st.session_state:
            st.session_state.local_chrome_sessions = {}

        if 'selected_profile_for_session' not in st.session_state:
            st.session_state.selected_profile_for_session = None
        if 'session_management_logs' not in st.session_state:
            st.session_state.session_management_logs = []

        # Initialize Chrome manager
        try:
            from ..hyperbrowser_utils import get_mongodb_client
            client, db = get_mongodb_client()
            self.chrome_manager = ChromeSessionManager(db_manager, client)
        except Exception as e:
            logger.warning(f"Could not initialize Chrome manager: {e}")
            self.chrome_manager = ChromeSessionManager(db_manager, None)

        # Ensure database tables/columns exist
        self._ensure_survey_columns()
        self._ensure_survey_sites_table()
        self._ensure_demographic_columns()
        self._ensure_account_urls_table()
        self._ensure_question_columns()

    # --------------------------------------------------------------------------
    # Database schema migration helpers
    # --------------------------------------------------------------------------
    def _ensure_survey_columns(self):
        """Ensure the accounts table has country and total_surveys_processed columns."""
        try:
            # Add country column if missing
            check_country = """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'accounts' AND column_name = 'country'
            """
            result = self.db_manager.execute_query(check_country, fetch=True)
            if not result:
                alter_query = "ALTER TABLE accounts ADD COLUMN country VARCHAR(100)"
                self.db_manager.execute_query(alter_query)
                logger.info("✅ Added country column to accounts table")

            # Rename or add total_surveys_processed column
            check_surveys = """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'accounts' AND column_name = 'total_surveys_processed'
            """
            result = self.db_manager.execute_query(check_surveys, fetch=True)
            if not result:
                # If old total_content_processed exists, rename it; otherwise create new
                check_old = """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'accounts' AND column_name = 'total_content_processed'
                """
                old_exists = self.db_manager.execute_query(check_old, fetch=True)
                if old_exists:
                    alter_query = "ALTER TABLE accounts RENAME COLUMN total_content_processed TO total_surveys_processed"
                    self.db_manager.execute_query(alter_query)
                    logger.info("✅ Renamed total_content_processed to total_surveys_processed")
                else:
                    alter_query = "ALTER TABLE accounts ADD COLUMN total_surveys_processed INTEGER DEFAULT 0"
                    self.db_manager.execute_query(alter_query)
                    logger.info("✅ Added total_surveys_processed column")
        except Exception as e:
            logger.error(f"Failed to ensure survey columns: {e}")

    def _ensure_demographic_columns(self):
        """Ensure demographic columns exist in accounts table (all optional)."""
        try:
            demographic_columns = [
                ("age", "INTEGER"),
                ("date_of_birth", "DATE"),
                ("gender", "VARCHAR(50)"),
                ("city", "VARCHAR(100)"),
                ("education_level", "VARCHAR(100)"),
                ("email", "VARCHAR(255)"),
                ("phone", "VARCHAR(50)"),
                ("job_status", "VARCHAR(100)"),
                ("industry", "VARCHAR(100)"),
                ("income_range", "VARCHAR(50)"),
                ("marital_status", "VARCHAR(50)"),
                ("household_size", "INTEGER"),
                ("has_children", "BOOLEAN"),
                ("shopping_habits", "TEXT"),
                ("brands_used", "TEXT"),
                ("hobbies", "TEXT"),
                ("internet_usage", "VARCHAR(100)"),
                ("device_type", "VARCHAR(100)"),
                ("owns_laptop", "BOOLEAN"),
                ("owns_tv", "BOOLEAN"),
                ("internet_provider", "VARCHAR(100)"),
                ("demographic_data", "JSONB")
            ]

            for col_name, col_type in demographic_columns:
                check_query = f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'accounts' AND column_name = '{col_name}'
                """
                result = self.db_manager.execute_query(check_query, fetch=True)
                if not result:
                    alter_query = f"ALTER TABLE accounts ADD COLUMN {col_name} {col_type}"
                    self.db_manager.execute_query(alter_query)
                    logger.info(f"✅ Added {col_name} column to accounts table")
        except Exception as e:
            logger.error(f"Failed to ensure demographic columns: {e}")

    def _ensure_survey_sites_table(self):
        """Create survey_sites table with name as primary identifier."""
        try:
            # First check if old table exists and migrate
            check_old = """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'survey_sites' AND column_name = 'country'
            """
            result = self.db_manager.execute_query(check_old, fetch=True)
            
            if result:
                # Old table exists - we need to add name column if it doesn't exist
                check_name = """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'survey_sites' AND column_name = 'site_name'
                """
                name_exists = self.db_manager.execute_query(check_name, fetch=True)
                if not name_exists:
                    alter_query = "ALTER TABLE survey_sites ADD COLUMN site_name VARCHAR(255)"
                    self.db_manager.execute_query(alter_query)
                    logger.info("✅ Added site_name column to survey_sites table")
                    
                # Also add is_active if missing
                check_active = """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'survey_sites' AND column_name = 'is_active'
                """
                active_exists = self.db_manager.execute_query(check_active, fetch=True)
                if not active_exists:
                    alter_query = "ALTER TABLE survey_sites ADD COLUMN is_active BOOLEAN DEFAULT TRUE"
                    self.db_manager.execute_query(alter_query)
                    logger.info("✅ Added is_active column to survey_sites table")
            else:
                # Create new table
                create_table_query = """
                CREATE TABLE IF NOT EXISTS survey_sites (
                    site_id SERIAL PRIMARY KEY,
                    site_name VARCHAR(255) UNIQUE NOT NULL,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE
                )
                """
                self.db_manager.execute_query(create_table_query)
                logger.info("✅ Ensured survey_sites table exists with site_name")
        except Exception as e:
            logger.error(f"Failed to create survey_sites table: {e}")

    def _ensure_account_urls_table(self):
        """Create account_urls table to store per-account URLs for survey sites."""
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS account_urls (
                url_id SERIAL PRIMARY KEY,
                account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                site_id INTEGER NOT NULL REFERENCES survey_sites(site_id) ON DELETE CASCADE,
                url TEXT NOT NULL,
                is_default BOOLEAN DEFAULT FALSE,
                is_used BOOLEAN DEFAULT FALSE,
                used_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notes TEXT,
                UNIQUE(account_id, site_id, url)
            )
            """
            self.db_manager.execute_query(create_table_query)
            logger.info("✅ Ensured account_urls table exists")
            
            # Create indexes
            self.db_manager.execute_query("""
                CREATE INDEX IF NOT EXISTS idx_account_urls_account_site 
                ON account_urls(account_id, site_id)
            """)
            self.db_manager.execute_query("""
                CREATE INDEX IF NOT EXISTS idx_account_urls_used 
                ON account_urls(is_used) WHERE is_used = FALSE
            """)
        except Exception as e:
            logger.error(f"Failed to create account_urls table: {e}")

    def _ensure_question_columns(self):
        """Ensure questions table has click_element and usage tracking columns."""
        try:
            # Check if click_element column exists
            check_click = """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'questions' AND column_name = 'click_element'
            """
            result = self.db_manager.execute_query(check_click, fetch=True)
            if not result:
                alter_query = "ALTER TABLE questions ADD COLUMN click_element TEXT"
                self.db_manager.execute_query(alter_query)
                logger.info("✅ Added click_element column to questions table")
            
            # Check if used_in_workflow column exists
            check_used = """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'questions' AND column_name = 'used_in_workflow'
            """
            result = self.db_manager.execute_query(check_used, fetch=True)
            if not result:
                alter_query = "ALTER TABLE questions ADD COLUMN used_in_workflow BOOLEAN DEFAULT FALSE"
                self.db_manager.execute_query(alter_query)
                logger.info("✅ Added used_in_workflow column to questions table")
            
            # Check if used_at column exists
            check_used_at = """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'questions' AND column_name = 'used_at'
            """
            result = self.db_manager.execute_query(check_used_at, fetch=True)
            if not result:
                alter_query = "ALTER TABLE questions ADD COLUMN used_at TIMESTAMP"
                self.db_manager.execute_query(alter_query)
                logger.info("✅ Added used_at column to questions table")
                
            # Check if metadata column exists
            check_metadata = """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'questions' AND column_name = 'metadata'
            """
            result = self.db_manager.execute_query(check_metadata, fetch=True)
            if not result:
                alter_query = "ALTER TABLE questions ADD COLUMN metadata JSONB"
                self.db_manager.execute_query(alter_query)
                logger.info("✅ Added metadata column to questions table")
                
        except Exception as e:
            logger.error(f"Failed to ensure question columns: {e}")

    # --------------------------------------------------------------------------
    # Helper methods
    # --------------------------------------------------------------------------
    def _clear_account_creation_state(self, keep_message=False):
        """Clear account creation session state"""
        st.session_state.show_add_account = False
        st.session_state.creating_account = False
        st.session_state.creation_completed = False
        st.session_state.username_to_create = None
        st.session_state.country_to_create = None
        st.session_state.creation_in_progress = False

        if not keep_message:
            st.session_state.account_creation_message = None
            st.session_state.account_creation_error = False
            self.clear_logs()

    def _clear_account_deletion_state(self):
        """Clear account deletion session state"""
        st.session_state.show_delete_account = False
        st.session_state.deleting_account = False
        st.session_state.deletion_completed = False
        st.session_state.account_id_to_delete = None
        st.session_state.deletion_in_progress = False
        st.session_state.account_deletion_message = None
        st.session_state.account_deletion_error = False

    def add_log(self, message, level="INFO"):
        """Add a log message to both Python logging and Streamlit session state"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] {level}: {message}"

        if 'account_creation_logs' not in st.session_state:
            st.session_state.account_creation_logs = []
        st.session_state.account_creation_logs.append(log_entry)

        if len(st.session_state.account_creation_logs) > 50:
            st.session_state.account_creation_logs = st.session_state.account_creation_logs[-50:]

        if level == "ERROR":
            logger.error(message)
        elif level == "WARNING":
            logger.warning(message)
        else:
            logger.info(message)

    def clear_logs(self):
        """Clear the creation logs"""
        st.session_state.account_creation_logs = []

    # --------------------------------------------------------------------------
    # Account URL management methods
    # --------------------------------------------------------------------------
    def _get_account_urls(self, account_id: int, site_id: Optional[int] = None, show_used: bool = False) -> List[Dict[str, Any]]:
        """Get all URLs for an account, optionally filtered by site."""
        try:
            query = """
                SELECT 
                    au.url_id,
                    au.account_id,
                    au.site_id,
                    ss.site_name,
                    au.url,
                    au.is_default,
                    au.is_used,
                    au.used_at,
                    au.created_at,
                    au.notes
                FROM account_urls au
                JOIN survey_sites ss ON au.site_id = ss.site_id
                WHERE au.account_id = %s
            """
            params = [account_id]
            
            if site_id:
                query += " AND au.site_id = %s"
                params.append(site_id)
            
            if not show_used:
                query += " AND (au.is_used = FALSE OR au.is_used IS NULL)"
            
            query += " ORDER BY ss.site_name, au.is_default DESC, au.created_at DESC"
            
            result = self.db_manager.execute_query(query, params, fetch=True)
            return [dict(row) for row in result] if result else []
        except Exception as e:
            self.add_log(f"Error getting account URLs: {e}", "ERROR")
            return []

    def _add_account_url(self, account_id: int, site_id: int, url: str, is_default: bool = False, notes: str = "") -> Dict[str, Any]:
        """Add a URL for an account."""
        try:
            # If setting as default, unset any existing default for this account/site
            if is_default:
                unset_query = """
                UPDATE account_urls
                SET is_default = FALSE
                WHERE account_id = %s AND site_id = %s AND is_default = TRUE
                """
                self.db_manager.execute_query(unset_query, (account_id, site_id))
            
            insert_query = """
            INSERT INTO account_urls (account_id, site_id, url, is_default, notes)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING url_id
            """
            result = self.db_manager.execute_query(insert_query, (account_id, site_id, url, is_default, notes), fetch=True)
            
            if result:
                url_id = result[0][0] if isinstance(result[0], tuple) else result[0]['url_id']
                self.add_log(f"✅ Added URL for account {account_id}")
                return {'success': True, 'url_id': url_id}
            return {'success': False, 'error': 'Failed to add URL'}
        except Exception as e:
            error_msg = f"Failed to add URL: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}

    def _update_account_url(self, url_id: int, url: str, is_default: bool, notes: str) -> Dict[str, Any]:
        """Update an account URL."""
        try:
            # Get account_id and site_id for this URL
            get_query = "SELECT account_id, site_id FROM account_urls WHERE url_id = %s"
            result = self.db_manager.execute_query(get_query, (url_id,), fetch=True)
            if not result:
                return {'success': False, 'error': 'URL not found'}
            
            account_id = result[0][0] if isinstance(result[0], tuple) else result[0]['account_id']
            site_id = result[0][1] if isinstance(result[0], tuple) else result[0]['site_id']
            
            # If setting as default, unset any existing default for this account/site
            if is_default:
                unset_query = """
                UPDATE account_urls
                SET is_default = FALSE
                WHERE account_id = %s AND site_id = %s AND is_default = TRUE AND url_id != %s
                """
                self.db_manager.execute_query(unset_query, (account_id, site_id, url_id))
            
            update_query = """
            UPDATE account_urls
            SET url = %s, is_default = %s, notes = %s, updated_at = CURRENT_TIMESTAMP
            WHERE url_id = %s
            """
            self.db_manager.execute_query(update_query, (url, is_default, notes, url_id))
            self.add_log(f"✅ Updated URL ID: {url_id}")
            return {'success': True}
        except Exception as e:
            error_msg = f"Failed to update URL: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}

    def _delete_account_url(self, url_id: int) -> Dict[str, Any]:
        """Delete an account URL."""
        try:
            delete_query = "DELETE FROM account_urls WHERE url_id = %s"
            self.db_manager.execute_query(delete_query, (url_id,))
            self.add_log(f"✅ Deleted URL ID: {url_id}")
            return {'success': True}
        except Exception as e:
            error_msg = f"Failed to delete URL: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}

    def _mark_url_used(self, url_id: int) -> Dict[str, Any]:
        """Mark a URL as used."""
        try:
            update_query = """
            UPDATE account_urls
            SET is_used = TRUE, used_at = CURRENT_TIMESTAMP
            WHERE url_id = %s
            """
            self.db_manager.execute_query(update_query, (url_id,))
            self.add_log(f"✅ Marked URL ID {url_id} as used")
            return {'success': True}
        except Exception as e:
            error_msg = f"Failed to mark URL as used: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}

    def _mark_url_unused(self, url_id: int) -> Dict[str, Any]:
        """Mark a URL as unused (reset)."""
        try:
            update_query = """
            UPDATE account_urls
            SET is_used = FALSE, used_at = NULL
            WHERE url_id = %s
            """
            self.db_manager.execute_query(update_query, (url_id,))
            self.add_log(f"✅ Marked URL ID {url_id} as unused")
            return {'success': True}
        except Exception as e:
            error_msg = f"Failed to mark URL as unused: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}

    def _get_default_url_for_account(self, account_id: int, site_id: int) -> Optional[str]:
        """Get the default URL for an account and site."""
        try:
            query = """
            SELECT url FROM account_urls
            WHERE account_id = %s AND site_id = %s AND is_default = TRUE
            LIMIT 1
            """
            result = self.db_manager.execute_query(query, (account_id, site_id), fetch=True)
            if result:
                return result[0][0] if isinstance(result[0], tuple) else result[0]['url']
            
            # If no default, get the most recent unused URL
            query = """
            SELECT url FROM account_urls
            WHERE account_id = %s AND site_id = %s AND (is_used = FALSE OR is_used IS NULL)
            ORDER BY created_at DESC
            LIMIT 1
            """
            result = self.db_manager.execute_query(query, (account_id, site_id), fetch=True)
            if result:
                return result[0][0] if isinstance(result[0], tuple) else result[0]['url']
            
            return None
        except Exception as e:
            self.add_log(f"Error getting default URL: {e}", "ERROR")
            return None

    # --------------------------------------------------------------------------
    # Account creation with optional demographic fields
    # --------------------------------------------------------------------------
    def _create_account_in_postgres_minimal(self, username, country=None, demographic_data=None):
        """Create account record with optional demographic fields."""
        try:
            current_time = datetime.now()
            
            # Build dynamic insert based on provided demographic data
            columns = ["username", "country", "created_time", "updated_time", "total_surveys_processed"]
            values = [username, country, current_time, current_time, 0]
            placeholders = ["%s", "%s", "%s", "%s", "%s"]
            
            if demographic_data:
                for key, value in demographic_data.items():
                    if value is not None and key not in columns:
                        columns.append(key)
                        values.append(value)
                        placeholders.append("%s")
            
            insert_query = f"""
            INSERT INTO accounts ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            RETURNING account_id
            """
            
            result = self.db_manager.execute_query(insert_query, values, fetch=True)

            if not result:
                raise Exception("Account creation failed")

            account_id = result[0][0] if isinstance(result[0], tuple) else result[0]['account_id']
            self.add_log(f"✅ Account created - ID: {account_id}, Country: {country}")
            return {'account_id': account_id}
        except Exception as e:
            raise Exception(f"Failed to create account: {str(e)}")

    def _create_local_chrome_profile_for_account(self, username, account_id):
        """Create a persistent local Chrome profile."""
        try:
            self.add_log(f"Creating persistent Chrome profile for {username}...")
            result = self.chrome_manager.create_profile_for_account(account_id, username)

            if not result['success']:
                raise Exception(result.get('error', 'Unknown error'))

            if result.get('is_new'):
                self.add_log(f"✓ Created NEW profile: {result['profile_id']}")
            else:
                self.add_log(f"✓ Using EXISTING profile: {result['profile_id']}")

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

    def _handle_account_creation(self, username, country=None, cookies_json=None, demographic_data=None):
        """Handle account creation with optional demographic data."""
        if st.session_state.get('creation_in_progress'):
            self.add_log("Creation already in progress, skipping...", "WARNING")
            return

        st.session_state.creation_in_progress = True

        try:
            self.clear_logs()
            self.add_log(f"=== Starting account creation for: {username} ===")
            if country:
                self.add_log(f"Country: {country}")

            status_placeholder = st.empty()
            with status_placeholder.container():
                with st.status("Creating account...", expanded=True) as status:
                    st.write("🖥️ Creating LOCAL Chrome account...")
                    self.add_log("Creating LOCAL Chrome account")

                    # Step 1: create account record with demographic data
                    st.write("🔄 Creating account record...")
                    account_result = self._create_account_in_postgres_minimal(username, country, demographic_data)
                    account_id = account_result['account_id']
                    self.add_log(f"✓ Account ID: {account_id}")

                    # Step 2: create local Chrome profile
                    st.write("🔄 Creating local Chrome profile...")
                    profile_result = self._create_local_chrome_profile_for_account(username, account_id)
                    profile_path = profile_result['profile_path']
                    profile_id = profile_result['profile_id']
                    self.add_log(f"✓ Profile: {profile_path}")

                    # Step 3: link profile to account
                    st.write("🔄 Linking profile to account...")
                    update_query = """
                    UPDATE accounts
                    SET profile_id = %s,
                        profile_type = 'local_chrome',
                        updated_time = CURRENT_TIMESTAMP
                    WHERE account_id = %s
                    """
                    self.db_manager.execute_query(update_query, (profile_id, account_id))
                    self.add_log("✓ Account configured for local Chrome")

                    success_message = (
                        f"✓ Local Chrome account '{username}' created!\n"
                        f"Account ID: {account_id}\n"
                        f"Country: {country}\n"
                        f"Profile: {profile_path}\n"
                    )
                    if demographic_data and any(demographic_data.values()):
                        success_message += f"Demographic info: {sum(1 for v in demographic_data.values() if v)} fields provided\n"

                    self.add_log("=== Account creation completed ===")
                    status.update(label="✓ Account created successfully!", state="complete")

                    st.session_state.account_creation_message = success_message
                    st.session_state.account_creation_error = False
                    st.session_state.creation_completed = True
                    st.session_state.creating_account = False
                    st.session_state.creation_in_progress = False

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

    # --------------------------------------------------------------------------
    # Account deletion
    # --------------------------------------------------------------------------
    def _delete_account_from_postgres(self, account_id):
        """Delete account and all related data from PostgreSQL"""
        try:
            self.add_log(f"Starting PostgreSQL deletion for account_id: {account_id}")

            account_query = "SELECT username, profile_id FROM accounts WHERE account_id = %s"
            account_info = self.db_manager.execute_query(account_query, (account_id,), fetch=True)
            if not account_info:
                raise Exception(f"Account with ID {account_id} not found")

            username, profile_id = account_info[0]
            self.add_log(f"Found account: {username} with profile_id: {profile_id}")

            deletion_results = {}

            # Delete from child tables
            tables = ['account_cookies', 'extraction_state', 'answers', 'questions', 'prompts', 'prompt_backups', 'account_urls', 'workflows']
            for table in tables:
                try:
                    self.db_manager.execute_query(f"DELETE FROM {table} WHERE account_id = %s", (account_id,))
                    deletion_results[table] = 'deleted'
                    self.add_log(f"Deleted {table} entries")
                except Exception as e:
                    self.add_log(f"Failed to delete from {table}: {e}", "WARNING")
                    deletion_results[table] = f'error: {str(e)}'

            # Delete the main account
            self.db_manager.execute_query("DELETE FROM accounts WHERE account_id = %s", (account_id,))
            deletion_results['accounts'] = 'deleted'
            self.add_log("✅ Deleted main account entry")

            return {
                'deleted_from_postgres': True,
                'username': username,
                'profile_id': profile_id,
                'results': deletion_results
            }
        except Exception as e:
            error_msg = f"Failed to delete from PostgreSQL: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'deleted_from_postgres': False, 'error': str(e)}

    def _delete_account_from_mongodb(self, account_id, profile_id, mongodb_id=None):
        """Delete account-related data from MongoDB (profiles, sessions)."""
        try:
            self.add_log(f"Starting MongoDB deletion for account_id: {account_id}, profile_id: {profile_id}")

            from streamlit_hyperbrowser_manager import get_mongodb_client
            client, db = get_mongodb_client()
            deletion_results = {}

            # Delete from accounts collection (profiles)
            profile_result = db.accounts.delete_many({'profile_id': profile_id, 'is_active': True})
            deletion_results['accounts'] = {
                'deleted_count': profile_result.deleted_count,
                'acknowledged': profile_result.acknowledged
            }
            self.add_log(f"Deleted {profile_result.deleted_count} accounts documents")

            # Delete from browser_sessions
            sessions_result = db.browser_sessions.delete_many({'profile_id': profile_id})
            deletion_results['browser_sessions'] = {
                'deleted_count': sessions_result.deleted_count,
                'acknowledged': sessions_result.acknowledged
            }
            self.add_log(f"Deleted {sessions_result.deleted_count} browser_sessions documents")

            if mongodb_id:
                from bson import ObjectId
                if isinstance(mongodb_id, str):
                    mongodb_id = ObjectId(mongodb_id)
                specific_result = db.accounts.delete_one({'_id': mongodb_id})
                deletion_results['specific_document'] = {
                    'deleted_count': specific_result.deleted_count,
                    'acknowledged': specific_result.acknowledged
                }
                self.add_log(f"Deleted specific document with MongoDB ID: {mongodb_id}")

            client.close()
            self.add_log("✅ MongoDB deletion completed")
            return {'deleted_from_mongodb': True, 'results': deletion_results}
        except Exception as e:
            error_msg = f"Failed to delete from MongoDB: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'deleted_from_mongodb': False, 'error': str(e)}

    def _handle_account_deletion(self, account_id):
        """Handle the complete account deletion process"""
        if st.session_state.get('deletion_in_progress'):
            self.add_log("Deletion already in progress, skipping...", "WARNING")
            return

        st.session_state.deletion_in_progress = True

        try:
            self.clear_logs()
            self.add_log(f"=== Starting account deletion process for account_id: {account_id} ===")

            status_placeholder = st.empty()
            with status_placeholder.container():
                with st.status("Deleting account...", expanded=True) as status:
                    # Step 1: get account info
                    st.write("🔍 Getting account information...")
                    account_query = "SELECT username, profile_id, mongo_object_id FROM accounts WHERE account_id = %s"
                    account_info = self.db_manager.execute_query(account_query, (account_id,), fetch=True)
                    if not account_info:
                        raise Exception(f"Account with ID {account_id} not found")
                    username, profile_id, mongo_object_id = account_info[0]
                    st.write(f"✅ Found account: {username}")

                    # Step 2: delete from MongoDB
                    st.write("🔄 Deleting from MongoDB...")
                    mongodb_result = self._delete_account_from_mongodb(account_id, profile_id, mongo_object_id)
                    if mongodb_result.get('deleted_from_mongodb'):
                        st.write("✅ Deleted from MongoDB")
                    else:
                        st.write("⚠️ MongoDB deletion issue")

                    # Step 3: delete from PostgreSQL (last)
                    st.write("🔄 Deleting from PostgreSQL...")
                    postgres_result = self._delete_account_from_postgres(account_id)
                    if postgres_result.get('deleted_from_postgres'):
                        st.write("✅ Deleted from PostgreSQL")
                    else:
                        raise Exception(postgres_result.get('error', 'Unknown PostgreSQL deletion error'))

                    self.add_log("=== Account deletion process completed successfully ===")
                    status.update(label=f"Account '{username}' deleted successfully!", state="complete")

                    st.session_state.account_deletion_message = f"Account '{username}' (ID: {account_id}) has been completely deleted."
                    st.session_state.account_deletion_error = False
                    st.session_state.deletion_completed = True
                    st.session_state.deleting_account = False
                    st.session_state.deletion_in_progress = False

                    st.cache_data.clear()
                    time.sleep(3)
                    st.rerun()

        except Exception as e:
            error_msg = str(e)
            self.add_log(f"=== Account deletion process failed: {error_msg} ===", "ERROR")
            st.session_state.account_deletion_message = f"Error deleting account: {error_msg}"
            st.session_state.account_deletion_error = True
            st.session_state.deletion_completed = True
            st.session_state.deleting_account = False
            st.session_state.deletion_in_progress = False
            st.error(f"Account deletion failed: {error_msg}")
            time.sleep(2)
            st.rerun()

    # --------------------------------------------------------------------------
    # Survey Sites management
    # --------------------------------------------------------------------------
    @st.cache_data(ttl=300)
    def load_survey_sites_data(_self) -> pd.DataFrame:
        """Load all survey sites from the database."""
        try:
            query = "SELECT site_id, site_name, description, created_at, updated_at, is_active FROM survey_sites ORDER BY site_name"
            data = _self.db_manager.execute_query(query, fetch=True)
            if not data:
                return pd.DataFrame()

            df = pd.DataFrame([dict(row) if hasattr(row, 'keys') else {
                'site_id': row[0],
                'site_name': row[1],
                'description': row[2],
                'created_at': row[3],
                'updated_at': row[4],
                'is_active': row[5]
            } for row in data])

            # Convert datetime columns
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at'])
            if 'updated_at' in df.columns:
                df['updated_at'] = pd.to_datetime(df['updated_at'])

            return df
        except Exception as e:
            st.error(f"Error loading survey sites: {str(e)}")
            return pd.DataFrame()

    def _add_or_update_survey_site(self, site_name: str, description: str = "", site_id: Optional[int] = None) -> Dict[str, Any]:
        """Insert or update a survey site by name."""
        try:
            if site_id:  # update
                query = """
                UPDATE survey_sites
                SET site_name = %s, description = %s, updated_at = CURRENT_TIMESTAMP
                WHERE site_id = %s
                RETURNING site_id
                """
                result = self.db_manager.execute_query(query, (site_name, description, site_id), fetch=True)
                if result:
                    self.add_log(f"✅ Updated survey site '{site_name}' (ID: {site_id})")
                    return {'success': True, 'site_id': site_id, 'action': 'updated'}
                else:
                    return {'success': False, 'error': 'Site not found'}
            else:  # insert
                # Check if name already exists
                check = "SELECT site_id FROM survey_sites WHERE site_name = %s"
                existing = self.db_manager.execute_query(check, (site_name,), fetch=True)
                if existing:
                    return {'success': False, 'error': f'A site named "{site_name}" already exists. Use update instead.'}

                insert = """
                INSERT INTO survey_sites (site_name, description)
                VALUES (%s, %s)
                RETURNING site_id
                """
                result = self.db_manager.execute_query(insert, (site_name, description), fetch=True)
                if result:
                    new_id = result[0][0] if isinstance(result[0], tuple) else result[0]['site_id']
                    self.add_log(f"✅ Added new survey site '{site_name}' (ID: {new_id})")
                    return {'success': True, 'site_id': new_id, 'action': 'inserted'}
                else:
                    return {'success': False, 'error': 'Insert failed'}
        except Exception as e:
            error_msg = f"Failed to save survey site: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}

    def _delete_survey_site(self, site_id: int) -> Dict[str, Any]:
        """Delete a survey site."""
        try:
            # Check if any account URLs reference this site
            check_query = "SELECT COUNT(*) FROM account_urls WHERE site_id = %s"
            result = self.db_manager.execute_query(check_query, (site_id,), fetch=True)
            count = result[0][0] if result else 0
            
            if count > 0:
                return {'success': False, 'error': f'Cannot delete site: {count} account URLs are using it'}
            
            query = "DELETE FROM survey_sites WHERE site_id = %s"
            self.db_manager.execute_query(query, (site_id,))
            self.add_log(f"✅ Deleted survey site ID: {site_id}")
            return {'success': True}
        except Exception as e:
            error_msg = f"Failed to delete survey site: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}

    def _get_survey_url_for_account(self, account_id: int, site_name: str) -> str:
        """Retrieve a URL for the account from the given site name."""
        try:
            # First get site_id from site_name
            site_query = "SELECT site_id FROM survey_sites WHERE site_name = %s"
            site_result = self.db_manager.execute_query(site_query, (site_name,), fetch=True)
            if not site_result:
                return "https://example-survey.com"  # fallback
            
            site_id = site_result[0][0] if isinstance(site_result[0], tuple) else site_result[0]['site_id']
            
            # Then get URL for this account
            url = self._get_default_url_for_account(account_id, site_id)
            if url:
                self.add_log(f"Found URL for account {account_id} on site {site_name}: {url}")
                return url
            else:
                self.add_log(f"No URL configured for account {account_id} on site '{site_name}'. Using default.", "WARNING")
                return "https://example-survey.com"  # fallback
        except Exception as e:
            self.add_log(f"Error fetching survey URL: {e}", "ERROR")
            return "https://example-survey.com"

    # --------------------------------------------------------------------------
    # Data loading
    # --------------------------------------------------------------------------
    @st.cache_data(ttl=300)
    def load_accounts_data(_self) -> pd.DataFrame:
        """Load accounts data from PostgreSQL with cookie info and surveys."""
        try:
            accounts_query = """
            SELECT
                a.account_id,
                a.username,
                a.country,
                a.profile_id,
                a.profile_type,
                a.created_time,
                a.updated_time,
                a.mongo_object_id,
                a.total_surveys_processed,
                COALESCE(a.has_cookies, FALSE) as has_cookies,
                a.cookies_last_updated,
                a.is_active,
                a.age,
                a.gender,
                a.city,
                a.education_level,
                a.job_status,
                a.industry,
                a.income_range,
                a.marital_status,
                a.household_size,
                a.has_children
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
                'country': row[2],
                'profile_id': row[3],
                'profile_type': row[4],
                'created_time': row[5],
                'updated_time': row[6],
                'mongo_object_id': row[7],
                'total_surveys_processed': row[8] if row[8] is not None else 0,
                'has_cookies': bool(row[9]),
                'cookies_last_updated': row[10],
                'is_active': bool(row[11]) if len(row) > 11 else True,
                'age': row[12] if len(row) > 12 else None,
                'gender': row[13] if len(row) > 13 else None,
                'city': row[14] if len(row) > 14 else None,
                'education_level': row[15] if len(row) > 15 else None,
                'job_status': row[16] if len(row) > 16 else None,
                'industry': row[17] if len(row) > 17 else None,
                'income_range': row[18] if len(row) > 18 else None,
                'marital_status': row[19] if len(row) > 19 else None,
                'household_size': row[20] if len(row) > 20 else None,
                'has_children': row[21] if len(row) > 21 else None
            } for row in accounts_data])

            # Convert datetime columns
            datetime_cols = ['created_time', 'updated_time', 'cookies_last_updated']
            for col in datetime_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])

            # Add active sessions count from local sessions
            df['active_sessions'] = 0
            local_sessions = st.session_state.get('local_chrome_sessions', {})
            for session_info in local_sessions.values():
                account_id = session_info.get('account_id')
                if account_id:
                    df.loc[df['account_id'] == account_id, 'active_sessions'] += 1

            return df
        except Exception as e:
            st.error(f"Error loading accounts data: {str(e)}")
            return pd.DataFrame()

    # --------------------------------------------------------------------------
    # Rendering methods
    # --------------------------------------------------------------------------
    def _render_quick_stats(self):
        """Render quick stats at top of page."""
        try:
            accounts_stats_query = """
            SELECT
                COUNT(*) as total_accounts,
                COUNT(CASE WHEN profile_type = 'local_chrome' THEN 1 END) as local_accounts,
                SUM(total_surveys_processed) as total_surveys
            FROM accounts
            """
            stats_result = self.db_manager.execute_query(accounts_stats_query, fetch=True)

            if stats_result:
                stats = stats_result[0]
                stats_dict = dict(stats) if hasattr(stats, 'keys') else {
                    'total_accounts': stats[0],
                    'local_accounts': stats[1],
                    'total_surveys': stats[2] or 0
                }

                local_sessions = len(st.session_state.get('local_chrome_sessions', {}))

                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Accounts", stats_dict['total_accounts'])
                with col2:
                    st.metric("Local Chrome", stats_dict['local_accounts'])
                with col3:
                    st.metric("Active Sessions", local_sessions)
                with col4:
                    st.metric("Total Surveys", stats_dict['total_surveys'])
        except Exception as e:
            st.info("Load data to see stats")

    def _render_accounts_overview(self, df):
        """Render standard accounts overview with country and surveys."""
        display_df = df.copy()

        display_columns = {
            'account_id': 'ID',
            'username': 'Username',
            'country': 'Country',
            'profile_type': 'Type',
            'profile_id': 'Profile ID',
            'active_sessions': 'Active Sessions',
            'total_surveys_processed': 'Surveys Processed',
            'has_cookies': 'Has Cookies',
            'created_time': 'Created'
        }

        formatted_df = display_df[list(display_columns.keys())].copy()
        formatted_df = formatted_df.rename(columns=display_columns)

        formatted_df['Type'] = formatted_df['Type'].apply(
            lambda x: '🖥️ Local' if x == 'local_chrome' else 'Unknown'
        )
        formatted_df['Profile ID'] = formatted_df['Profile ID'].apply(
            lambda x: f"{x[:12]}..." if x and len(str(x)) > 12 else (x if x else "None")
        )
        formatted_df['Country'] = formatted_df['Country'].apply(
            lambda x: str(x) if x and str(x).strip() else "—"
        )
        formatted_df['Has Cookies'] = formatted_df['Has Cookies'].apply(
            lambda x: '✓' if x else '✗'
        )
        formatted_df['Created'] = formatted_df['Created'].dt.strftime('%Y-%m-%d %H:%M')

        st.dataframe(
            formatted_df,
            use_container_width=True,
            column_config={
                "ID": st.column_config.NumberColumn("ID", help="Account ID", width="small"),
                "Username": st.column_config.TextColumn("Username", help="Account username", width="medium"),
                "Country": st.column_config.TextColumn("Country", help="Account country", width="medium"),
                "Type": st.column_config.TextColumn("Type", help="Profile type", width="small"),
                "Profile ID": st.column_config.TextColumn("Profile ID", help="Profile identifier", width="medium"),
                "Active Sessions": st.column_config.NumberColumn("Active Sessions", help="Currently active browser sessions", width="small"),
                "Surveys Processed": st.column_config.NumberColumn("Surveys Processed", help="Total surveys processed", width="small"),
                "Has Cookies": st.column_config.TextColumn("Has Cookies", help="Cookie status", width="small"),
                "Created": st.column_config.TextColumn("Created", help="Account creation date", width="medium")
            }
        )

    def _render_account_details_view(self, df):
        """Render detailed account view with cookie upload per account."""
        for _, row in df.iterrows():
            with st.expander(f"🏷️ {row['username']} (ID: {row['account_id']})", expanded=False):
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.write("**Account Info**")
                    st.write(f"Account ID: `{row['account_id']}`")
                    st.write(f"Username: `{row['username']}`")
                    st.write(f"Country: `{row.get('country', 'Not set')}`")
                    st.write(f"Profile ID: `{row['profile_id'][:12] if row['profile_id'] else 'None'}...`")
                    st.write(f"Type: `{row['profile_type']}`")
                    st.write(f"Created: {row['created_time'].strftime('%Y-%m-%d %H:%M')}")
                    st.write(f"Active Sessions: {row['active_sessions']}")

                with col2:
                    st.write("**Survey Stats**")
                    st.write(f"Total Surveys: {row['total_surveys_processed']}")

                    url_count = self._get_account_urls(row['account_id'], show_used=True)
                    st.write(f"Configured URLs: {len(url_count)}")

                    st.write("**Basic Info**")
                    if row.get('age'):
                        st.write(f"Age: {row['age']}")
                    if row.get('gender'):
                        st.write(f"Gender: {row['gender']}")
                    if row.get('city'):
                        st.write(f"City: {row['city']}")
                    if row.get('education_level'):
                        st.write(f"Education: {row['education_level']}")

                with col3:
                    st.write("**Employment**")
                    if row.get('job_status'):
                        st.write(f"Status: {row['job_status']}")
                    if row.get('industry'):
                        st.write(f"Industry: {row['industry']}")
                    if row.get('income_range'):
                        st.write(f"Income: {row['income_range']}")

                    st.write("**Household**")
                    if row.get('marital_status'):
                        st.write(f"Marital: {row['marital_status']}")
                    if row.get('household_size'):
                        st.write(f"Household Size: {row['household_size']}")
                    if row.get('has_children') is not None:
                        st.write(f"Has Children: {'Yes' if row['has_children'] else 'No'}")

                with col4:
                    st.write("**Cookie Status**")
                    st.write(f"Has Cookies: {'✓' if row.get('has_cookies') else '✗'}")
                    if row.get('cookies_last_updated'):
                        st.write(f"Last Updated: {row['cookies_last_updated'].strftime('%Y-%m-%d %H:%M')}")

                    if row.get('has_cookies'):
                        validity = self._check_cookie_validity(row['account_id'])
                        if validity.get('valid'):
                            st.success("✓ Cookies valid")
                            if validity.get('warning'):
                                st.caption(f"⚠️ {validity['warning']}")
                        else:
                            st.error(f"❌ Cookies invalid: {validity.get('reason', 'Unknown')}")

                    st.write("**Actions**")
                    if st.button("▶️ Start Session", key=f"start_session_{row['account_id']}",
                                use_container_width=True, type="primary"):
                        survey_url = self._get_survey_url_for_account(
                            row['account_id'], row.get('country', 'United States')
                        )
                        result = self._start_local_chrome_session(
                            row['profile_id'],
                            row['account_id'],
                            row['username'],
                            survey_url
                        )
                        if result.get('success'):
                            st.success("✓ Session started!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"Failed: {result.get('error')}")

                # Cookie upload section
                st.markdown("---")
                st.write("**🍪 Upload / Update Cookies**")

                cookie_key = f"cookie_upload_{row['account_id']}"
                cookies_json_input = st.text_area(
                    "Paste EditThisCookie JSON",
                    placeholder='[\n  {\n    "domain": ".x.com",\n    "name": "auth_token",\n    "value": "...",\n    ...\n  }\n]',
                    height=120,
                    key=cookie_key,
                    help="Paste the JSON exported from EditThisCookie extension"
                )

                col_cookie1, col_cookie2 = st.columns([2, 1])
                with col_cookie1:
                    if cookies_json_input:
                        try:
                            parsed = json.loads(cookies_json_input)
                            if isinstance(parsed, list) and len(parsed) > 0:
                                st.success(f"✓ {len(parsed)} cookies detected")
                            else:
                                st.warning("⚠️ Invalid cookie format — expected a JSON array")
                        except json.JSONDecodeError:
                            st.error("❌ Invalid JSON")

                with col_cookie2:
                    if st.button(
                        "💾 Save Cookies",
                        key=f"save_cookies_{row['account_id']}",
                        use_container_width=True,
                        disabled=not bool(cookies_json_input and cookies_json_input.strip())
                    ):
                        result = self._store_account_cookies(
                            row['account_id'], cookies_json_input, row['username']
                        )
                        if result['success']:
                            st.success(f"✓ {result['cookie_count']} cookies saved!")
                            st.cache_data.clear()
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"❌ {result['error']}")

                # View cookies modal
                if st.session_state.get(f'modal_view_{row["account_id"]}'):
                    st.markdown("---")
                    st.subheader(f"🍪 Cookies for {row['username']}")
                    cookie_info = self._get_account_cookies(row['account_id'])
                    if cookie_info['has_cookies']:
                        cookies = cookie_info['cookie_data']
                        if isinstance(cookies, str):
                            cookies = json.loads(cookies)

                        critical_names = ['auth_token', 'ct0', 'kdt']
                        st.write("**Critical Authentication Cookies:**")
                        for name in critical_names:
                            cookie = next((c for c in cookies if c['name'] == name), None)
                            if cookie:
                                with st.expander(f"🔑 {name}", expanded=False):
                                    st.json({
                                        'name': cookie.get('name'),
                                        'domain': cookie.get('domain'),
                                        'value': cookie.get('value')[:20] + '...',
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

    def _render_performance_analytics_view(self, df):
        """Render performance analytics focused on surveys."""
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
                st.subheader("Survey Processing")
                top_accounts = df.nlargest(10, 'total_surveys_processed')[['username', 'total_surveys_processed']]
                fig2 = px.bar(
                    top_accounts,
                    x='username',
                    y='total_surveys_processed',
                    title="Top 10 Accounts by Surveys Processed"
                )
                st.plotly_chart(fig2, use_container_width=True)

    # --------------------------------------------------------------------------
    # Add Account Modal
    # --------------------------------------------------------------------------
    def render_add_account_modal(self):
        """Render add account form with country field and optional demographic info."""
        if st.session_state.get('show_add_account', False):
            with st.expander("➕ Add New Account", expanded=True):
                self.render_creation_logs()

                if st.session_state.get('creating_account', False):
                    st.info("⏳ Creating account... Please wait.")
                    if (not st.session_state.get('creation_completed', False) and
                        not st.session_state.get('creation_in_progress', False) and
                        st.session_state.get('username_to_create')):
                        username = st.session_state.get('username_to_create', '')
                        country = st.session_state.get('country_to_create')
                        demographic_data = st.session_state.get('demographic_data', {})
                        if username:
                            self.add_log(f"Triggering account creation for: {username}")
                            self._handle_account_creation(username, country, None, demographic_data)
                    return

                with st.form("add_account_form"):
                    st.write("**Account Configuration:**")

                    col1, col2 = st.columns(2)

                    with col1:
                        username = st.text_input(
                            "Username *",
                            placeholder="Enter username",
                            help="Username for this account (required)"
                        )

                    with col2:
                        country = st.selectbox(
                            "Country *",
                            options=[
                                "United States", "Canada", "United Kingdom", "Australia",
                                "Germany", "France", "Japan", "Brazil", "India",
                                "Mexico", "Spain", "Italy", "Netherlands", "Sweden",
                                "Norway", "Denmark", "Finland", "New Zealand",
                                "Singapore", "South Africa", "Other"
                            ],
                            help="Select the country associated with this account (for survey targeting)"
                        )

                    st.markdown("---")
                    st.markdown("### 📋 Optional Demographic Information")
                    st.caption("This information helps match you with relevant surveys. All fields are optional.")

                    # 1. Basic Personal Info
                    with st.expander("1️⃣ Basic Personal Info", expanded=False):
                        col1, col2 = st.columns(2)
                        with col1:
                            age = st.number_input("Age", min_value=18, max_value=120, value=None, step=1)
                            gender = st.selectbox("Gender", ["", "Male", "Female", "Non-binary", "Prefer not to say"])
                        with col2:
                            date_of_birth = st.date_input("Date of Birth", value=None)
                            city = st.text_input("City", placeholder="e.g., Nairobi")

                        education_level = st.selectbox(
                            "Education Level",
                            ["", "High School", "Some College", "Associate Degree",
                            "Bachelor's Degree", "Master's Degree", "Doctorate", "Trade School", "Other"]
                        )

                    # 2. Contact & Account Info
                    with st.expander("2️⃣ Contact & Account Info", expanded=False):
                        col1, col2 = st.columns(2)
                        with col1:
                            email = st.text_input("Email Address", placeholder="user@example.com")
                        with col2:
                            phone = st.text_input("Phone Number", placeholder="+1 234 567 8900")

                    # 3. Employment & Income
                    with st.expander("3️⃣ Employment & Income", expanded=False):
                        col1, col2 = st.columns(2)
                        with col1:
                            job_status = st.selectbox(
                                "Job Status",
                                ["", "Student", "Employed Full-time", "Employed Part-time",
                                "Self-employed", "Unemployed", "Retired", "Homemaker"]
                            )
                            industry = st.text_input("Industry", placeholder="e.g., Technology, Healthcare, Education")
                        with col2:
                            income_range = st.selectbox(
                                "Income Range",
                                ["", "Under $25,000", "$25,000 - $50,000", "$50,000 - $75,000",
                                "$75,000 - $100,000", "$100,000 - $150,000", "Over $150,000", "Prefer not to say"]
                            )

                    # 4. Household Information
                    with st.expander("4️⃣ Household Information", expanded=False):
                        col1, col2 = st.columns(2)
                        with col1:
                            marital_status = st.selectbox(
                                "Marital Status",
                                ["", "Single", "Married", "Divorced", "Widowed", "Domestic Partnership"]
                            )
                            household_size = st.number_input("Household Size", min_value=1, max_value=20, value=None, step=1)
                        with col2:
                            has_children = st.selectbox("Have Children?", ["", "Yes", "No"])

                    # 5. Lifestyle & Habits
                    with st.expander("5️⃣ Lifestyle & Habits", expanded=False):
                        shopping_habits = st.text_area(
                            "Shopping Habits",
                            placeholder="e.g., Online shopping, in-store, frequency..."
                        )
                        brands_used = st.text_area(
                            "Brands You Use",
                            placeholder="e.g., Nike, Apple, Samsung, local brands..."
                        )
                        hobbies = st.text_area(
                            "Hobbies & Interests",
                            placeholder="e.g., Gaming, Sports, Reading, Cooking..."
                        )
                        internet_usage = st.selectbox(
                            "Internet Usage",
                            ["", "Light (1-2 hours/day)", "Moderate (3-5 hours/day)",
                            "Heavy (6+ hours/day)", "Constant"]
                        )

                    # 6. Device & Tech Usage
                    with st.expander("6️⃣ Device & Tech Usage", expanded=False):
                        col1, col2 = st.columns(2)
                        with col1:
                            device_type = st.selectbox(
                                "Primary Device",
                                ["", "Smartphone", "Tablet", "Laptop", "Desktop Computer", "Multiple Devices"]
                            )
                            owns_laptop = st.selectbox("Owns Laptop?", ["", "Yes", "No"])
                        with col2:
                            owns_tv = st.selectbox("Owns TV?", ["", "Yes", "No"])
                            internet_provider = st.text_input("Internet Provider", placeholder="e.g., Comcast, Spectrum")

                    st.caption("Profile will be created at: `/workspace/chrome_profiles/account_{username}`")

                    if st.session_state.get('account_creation_message'):
                        if st.session_state.get('account_creation_error'):
                            st.error(st.session_state.account_creation_message)
                        else:
                            st.success(st.session_state.account_creation_message)

                    col_submit, col_cancel = st.columns([1, 1])

                    with col_submit:
                        submitted = st.form_submit_button(
                            "Create Local Chrome Account",
                            use_container_width=True
                        )

                    with col_cancel:
                        cancel = st.form_submit_button("Cancel", use_container_width=True)

                    if submitted:
                        if not username.strip():
                            st.warning("Please enter a username")
                        else:
                            demographic_data = {
                                'age': age if age else None,
                                'date_of_birth': date_of_birth if date_of_birth else None,
                                'gender': gender if gender else None,
                                'city': city if city else None,
                                'education_level': education_level if education_level else None,
                                'email': email if email else None,
                                'phone': phone if phone else None,
                                'job_status': job_status if job_status else None,
                                'industry': industry if industry else None,
                                'income_range': income_range if income_range else None,
                                'marital_status': marital_status if marital_status else None,
                                'household_size': household_size if household_size else None,
                                'has_children': has_children == "Yes" if has_children else None,
                                'shopping_habits': shopping_habits if shopping_habits else None,
                                'brands_used': brands_used if brands_used else None,
                                'hobbies': hobbies if hobbies else None,
                                'internet_usage': internet_usage if internet_usage else None,
                                'device_type': device_type if device_type else None,
                                'owns_laptop': owns_laptop == "Yes" if owns_laptop else None,
                                'owns_tv': owns_tv == "Yes" if owns_tv else None,
                                'internet_provider': internet_provider if internet_provider else None
                            }

                            # Remove None values
                            demographic_data = {k: v for k, v in demographic_data.items() if v is not None}

                            self.add_log(f"Form submitted - preparing creation for: {username.strip()}")
                            st.session_state.username_to_create = username.strip()
                            st.session_state.country_to_create = country
                            st.session_state.cookies_to_upload = None
                            st.session_state.demographic_data = demographic_data
                            st.session_state.creating_account = True
                            st.session_state.account_creation_message = None
                            st.session_state.account_creation_error = False
                            st.session_state.creation_completed = False
                            st.session_state.creation_in_progress = False
                            st.rerun()

                    if cancel:
                        self._clear_account_creation_state()
                        st.rerun()

    # --------------------------------------------------------------------------
    # Delete Account Modal
    # --------------------------------------------------------------------------
    def render_delete_account_modal(self, accounts_df):
        """Render delete account interface"""
        if st.session_state.get('show_delete_account', False):
            with st.expander("🗑️ Delete Account", expanded=True):
                self.render_creation_logs()

                if st.session_state.get('deleting_account', False):
                    st.info("⏳ Deleting account... Please wait.")
                    if (not st.session_state.get('deletion_completed', False) and
                        not st.session_state.get('deletion_in_progress', False) and
                        st.session_state.get('account_id_to_delete')):
                        account_id = st.session_state.get('account_id_to_delete')
                        if account_id:
                            self.add_log(f"Triggering account deletion for ID: {account_id}")
                            self._handle_account_deletion(account_id)
                    return

                st.warning("⚠️ **Account Deletion Warning**")
                st.write("This action will permanently delete the account from:")
                st.write("- ✅ PostgreSQL database (all related data)")
                st.write("- ✅ MongoDB collections (profiles, sessions, etc.)")
                st.write("")
                st.error("**This action cannot be undone!**")

                with st.form("delete_account_form"):
                    if not accounts_df.empty:
                        account_options = [(f"{row['username']} (ID: {row['account_id']}) - Profile: {row['profile_id'][:8]}...", idx)
                                          for idx, row in accounts_df.iterrows()]

                        selected_account = st.selectbox(
                            "Select Account to Delete",
                            options=[("Choose an account...", None)] + account_options,
                            format_func=lambda x: x[0]
                        )

                        if selected_account[1] is not None:
                            account_id = accounts_df.iloc[selected_account[1]]['account_id']
                            account_row = accounts_df.iloc[selected_account[1]]

                            st.info("**Account Details:**")
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write(f"**Username:** {account_row['username']}")
                                st.write(f"**Account ID:** {account_row['account_id']}")
                                st.write(f"**Country:** {account_row['country']}")
                            with col2:
                                st.write(f"**Profile ID:** {account_row['profile_id']}")
                                st.write(f"**Surveys Processed:** {account_row['total_surveys_processed']}")

                    else:
                        st.info("No accounts available to delete.")
                        selected_account = (None, None)

                    if selected_account[1] is not None:
                        account_row = accounts_df.iloc[selected_account[1]]
                        confirm_deletion = st.checkbox(
                            f"I understand this will permanently delete '{account_row['username']}' and all related data"
                        )
                    else:
                        confirm_deletion = False

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

    # --------------------------------------------------------------------------
    # Cookie management methods
    # --------------------------------------------------------------------------
    def _store_account_cookies(self, account_id, cookies_json, username):
        """Store cookies for an account in PostgreSQL."""
        try:
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            self.add_log(f"Storing cookies for account {account_id} ({username})")

            try:
                cookies = json.loads(cookies_json)
            except json.JSONDecodeError as e:
                return {'success': False, 'error': f"Invalid JSON format: {str(e)}"}

            if not isinstance(cookies, list):
                return {'success': False, 'error': "Cookies must be a JSON array"}

            required_fields = ['name', 'value', 'domain']
            for i, cookie in enumerate(cookies):
                missing = [f for f in required_fields if f not in cookie]
                if missing:
                    return {'success': False, 'error': f"Cookie #{i} missing fields: {missing}"}

            cookie_count = len(cookies)
            self.add_log(f"Validated {cookie_count} cookies")

            # Deactivate previous cookies
            deactivate_query = """
            UPDATE account_cookies
            SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
            WHERE account_id = %s AND is_active = TRUE
            """
            self.db_manager.execute_query(deactivate_query, (account_id,))
            self.add_log("Deactivated previous cookies")

            cookie_json_string = json.dumps(cookies)

            insert_query = """
            INSERT INTO account_cookies (
                account_id, cookie_data, cookie_count,
                uploaded_at, updated_at, is_active, cookie_source
            )
            VALUES (%s, %s::jsonb, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE, 'editthiscookie')
            RETURNING cookie_id
            """
            result = self.db_manager.execute_query(insert_query, (account_id, cookie_json_string, cookie_count), fetch=True)

            if not result:
                raise Exception("INSERT returned no rows")
            first_row = result[0]
            cookie_id = first_row[0] if isinstance(first_row, tuple) else first_row.get('cookie_id')
            if not cookie_id:
                raise Exception("Could not extract cookie_id")

            self.add_log(f"✓ Stored cookies with ID: {cookie_id}")

            update_account_query = """
            UPDATE accounts
            SET has_cookies = TRUE, cookies_last_updated = CURRENT_TIMESTAMP
            WHERE account_id = %s
            """
            self.db_manager.execute_query(update_account_query, (account_id,))
            self.add_log(f"✓ Updated account {account_id} metadata")

            return {
                'success': True,
                'cookie_id': cookie_id,
                'cookie_count': cookie_count,
                'account_id': account_id
            }
        except Exception as e:
            error_msg = f"Failed to store cookies: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}

    def _get_account_cookies(self, account_id):
        """Get active cookies for an account from PostgreSQL."""
        try:
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
                return {'has_cookies': False, 'cookies': None}

            row = result[0]
            if isinstance(row, tuple):
                cookie_id = row[0]
                cookie_data_raw = row[1]
                cookie_count = row[2]
                uploaded_at = row[3]
                updated_at = row[4]
                cookie_source = row[5]
                notes = row[6]
            else:
                cookie_id = row.get('cookie_id')
                cookie_data_raw = row.get('cookie_data')
                cookie_count = row.get('cookie_count')
                uploaded_at = row.get('uploaded_at')
                updated_at = row.get('updated_at')
                cookie_source = row.get('cookie_source')
                notes = row.get('notes')

            cookie_data = None
            if cookie_data_raw:
                if isinstance(cookie_data_raw, str):
                    cookie_data = json.loads(cookie_data_raw)
                elif isinstance(cookie_data_raw, (list, dict)):
                    cookie_data = cookie_data_raw

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
            return {'has_cookies': False, 'error': str(e)}

    def _check_cookie_validity(self, account_id):
        """Check if stored cookies are still valid."""
        try:
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            cookie_info = self._get_account_cookies(account_id)
            if not cookie_info['has_cookies']:
                return {'valid': False, 'reason': 'No cookies stored', 'needs_capture': True}

            cookies = cookie_info['cookie_data']
            if isinstance(cookies, str):
                cookies = json.loads(cookies)

            auth_token = next((c for c in cookies if c['name'] == 'auth_token'), None)
            ct0 = next((c for c in cookies if c['name'] == 'ct0'), None)

            if not auth_token:
                return {'valid': False, 'reason': 'No auth_token found', 'needs_capture': True}
            if not ct0:
                return {'valid': False, 'reason': 'No ct0 found', 'needs_capture': True}

            current_time = time.time()
            if auth_token.get('expirationDate') and auth_token['expirationDate'] < current_time:
                return {'valid': False, 'reason': 'auth_token expired', 'needs_capture': True}

            uploaded_at = cookie_info.get('uploaded_at')
            if uploaded_at and (datetime.now() - uploaded_at).days > 30:
                return {'valid': True, 'warning': 'Cookies are over 30 days old', 'age_days': (datetime.now() - uploaded_at).days}

            return {'valid': True, 'has_auth_token': True, 'has_ct0': True, 'cookie_count': len(cookies)}
        except Exception as e:
            return {'valid': False, 'reason': f'Validation error: {str(e)}', 'needs_capture': True}

    # --------------------------------------------------------------------------
    # Log rendering
    # --------------------------------------------------------------------------
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

    def _render_local_chrome_tab(self, accounts_df):
        """Render Local Chrome session management with URL selection."""
        st.subheader("🖥️ Local Chrome Session Management")
        st.info("✓ Free • ✓ Persistent profiles • ✓ Account-based cookies • ✓ Custom start URLs")
        
        # Add info about the default URL
        st.info("ℹ️ **Default URL:** All sessions will start at `https://mylocation.org/` if no custom URL is selected.")

        if accounts_df.empty:
            st.warning("No accounts available. Create an account first.")
            return

        local_accounts = accounts_df[accounts_df['profile_type'] == 'local_chrome']
        if local_accounts.empty:
            st.warning("No Local Chrome accounts found. Create one in the Overview tab.")
            return

        col1, col2 = st.columns([2, 1])

        with col1:
            st.subheader("Active Sessions")
            local_sessions = st.session_state.get('local_chrome_sessions', {})

            if local_sessions:
                accounts_with_sessions = {}
                for session_id, session_info in local_sessions.items():
                    acc_user = session_info.get('account_username', 'Unknown')
                    accounts_with_sessions.setdefault(acc_user, []).append((session_id, session_info))

                for acc_user, sessions in accounts_with_sessions.items():
                    with st.container():
                        st.markdown(f"**👤 Account: {acc_user}**")
                        st.caption(f"{len(sessions)} session(s)")

                        for session_id, session_info in sessions:
                            s1, s2, s3 = st.columns([3, 2, 1])
                            with s1:
                                st.write(f"**Session:** `{session_id[:12]}...`")
                                if session_info.get('profile_path'):
                                    st.caption(f"📁 {session_info['profile_path']}")
                                if session_info.get('start_url'):
                                    st.caption(f"🌐 URL: {session_info['start_url'][:50]}...")
                                if session_info.get('debug_port'):
                                    st.caption(f"🔌 Debug: port {session_info['debug_port']}")
                            with s2:
                                started_at = session_info.get('started_at')
                                if started_at:
                                    duration = datetime.now() - started_at
                                    hours, remainder = divmod(duration.total_seconds(), 3600)
                                    minutes, _ = divmod(remainder, 60)
                                    st.write(f"**Duration:** {int(hours):02d}:{int(minutes):02d}")
                                if session_info.get('browser_url'):
                                    st.link_button("🖥️ VNC", session_info['browser_url'], use_container_width=True)
                            with s3:
                                if st.button("🛑", key=f"stop_local_{session_id[:8]}"):
                                    result = self._stop_local_chrome_session(session_id)
                                    if result.get('success'):
                                        st.success("Stopped")
                                        st.rerun()
                                    else:
                                        st.error(result.get('error'))
                            st.divider()
            else:
                st.info("No active sessions")

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
                key="local_tab_select"
            )

            if selected[1] is not None:
                row = local_accounts.iloc[selected[1]]
                account_id = row['account_id']
                
                st.info(f"**👤 Account: {row['username']}**")
                
                # URL selection - clearly show default option
                url_options = ["Default: https://mylocation.org/"]
                url_mapping = {"Default: https://mylocation.org/": "https://mylocation.org/"}
                
                # Add saved URLs for this account
                account_urls = self._get_account_urls(account_id, show_used=False)
                
                # Group URLs by site
                urls_by_site = {}
                for url_info in account_urls:
                    site_name = url_info.get('site_name', 'Other')
                    if site_name not in urls_by_site:
                        urls_by_site[site_name] = []
                    urls_by_site[site_name].append(url_info)
                
                # Add site URLs to options
                for site_name, urls in urls_by_site.items():
                    for url_info in urls:
                        default_marker = "⭐ " if url_info.get('is_default') else ""
                        label = f"{default_marker}{site_name}: {url_info['url'][:50]}..."
                        url_mapping[label] = url_info['url']
                        url_options.append(label)
                
                # Allow custom URL entry
                url_options.append("Custom URL...")
                
                selected_url_option = st.selectbox(
                    "Select Start URL:",
                    options=url_options,
                    key="start_url_select",
                    help="Choose a URL or enter a custom one. If nothing is selected, the default https://mylocation.org/ will be used."
                )
                
                start_url = None
                if selected_url_option == "Custom URL...":
                    start_url = st.text_input(
                        "Enter Custom URL:",
                        placeholder="https://example.com",
                        key="custom_url_input",
                        help="Enter any URL. If left blank, the default URL will be used."
                    )
                elif selected_url_option != "Choose account..." and selected_url_option in url_mapping:
                    start_url = url_mapping.get(selected_url_option)
                
                # Show profile status
                profile_path = self.chrome_manager.get_profile_path(row['username'])
                default_dir = os.path.join(profile_path, 'Default')
                if os.path.exists(default_dir):
                    st.success("✓ Existing profile - state preserved")
                else:
                    st.info("📁 New profile will be created")

                cookie_info = self._get_account_cookies(account_id)
                if cookie_info['has_cookies']:
                    st.success(f"✓ {cookie_info['cookie_count']} cookies stored")
                else:
                    st.warning("⚠️ No cookies stored")
                    
                # Display what URL will be used
                if selected_url_option == "Custom URL..." and start_url and start_url.strip():
                    st.info(f"🔗 Will open: {start_url}")
                else:
                    st.info("🔗 Will open default URL: https://mylocation.org/")

                if st.button("▶️ Start Chrome Session", type="primary", use_container_width=True,
                            key=f"start_local_{account_id}"):
                    with st.spinner("Starting Chrome with 3 tabs..."):
                        result = self._start_local_chrome_session(
                            row['profile_id'],
                            account_id,
                            row['username'],
                            start_url
                        )
                        if result.get('success'):
                            st.success("✓ Chrome started!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"Failed: {result.get('error')}")

    def _start_local_chrome_session(self, profile_id, account_id, account_username, start_url=None):
        """Start local Chrome session with 3 tabs: Automa, EditThisCookie, and custom start URL."""
        try:
            self.add_log(f"Starting LOCAL Chrome session for: {account_username}")
            
            # Set default URL if none provided
            DEFAULT_SURVEY_URL = "https://mylocation.org/"
            if start_url is None or start_url.strip() == "":
                start_url = DEFAULT_SURVEY_URL
                self.add_log(f"No URL provided, using default: {DEFAULT_SURVEY_URL}")
            else:
                self.add_log(f"Start URL: {start_url}")

            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            profile_path = self.chrome_manager.get_profile_path(account_username)
            session_id = f"local_{account_username}_{int(time.time())}"
            self.add_log(f"Session ID: {session_id}")

            # Ensure account cookie script exists
            self._ensure_account_cookie_script(account_id, account_username)

            # Start Chrome with custom start URL (or default)
            result = self.chrome_manager.run_persistent_chrome(
                session_id=session_id,
                profile_path=profile_path,
                username=account_username,
                survey_url=start_url,
                show_terminal=True
            )

            if not result.get("success"):
                raise Exception(result.get("error", "Unknown error"))

            debug_port = result.get("debug_port", 9222)
            self.add_log(f"✓ Chrome process started with debug port {debug_port}")
            self.add_log(f"  Tab 1: Automa Extension")
            self.add_log(f"  Tab 2: EditThisCookie Extension")
            self.add_log(f"  Tab 3: {start_url}")
            self.add_log(f"✓ Chrome visible at: {result.get('vnc_url')}")

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
                        "debug_port": debug_port,
                        "startup_urls": result.get("startup_urls", []),
                        "start_url": start_url,
                        "created_at": datetime.now(),
                        "is_active": True,
                        "session_status": "active",
                        "is_persistent": True,
                        "profile_preserved": True,
                        "chrome_auto_opened": True
                    }
                    db.browser_sessions.insert_one(session_doc)
                    self.add_log("✓ Stored in MongoDB")
                except Exception as e:
                    self.add_log(f"⚠ MongoDB storage failed: {e}", "WARNING")

            # Store in local session state
            if "local_chrome_sessions" not in st.session_state:
                st.session_state.local_chrome_sessions = {}

            st.session_state.local_chrome_sessions[session_id] = {
                "session_id": session_id,
                "session_type": "local_chrome",
                "profile_id": profile_id,
                "account_id": int(account_id),
                "account_username": account_username,
                "started_at": datetime.now(),
                "debug_port": debug_port,
                "browser_url": result.get("vnc_url"),
                "profile_path": profile_path,
                "is_persistent": True,
                "chrome_visible": True,
                "startup_urls": result.get("startup_urls", []),
                "start_url": start_url
            }

            self.add_log("✓ Session stored in local_chrome_sessions")

            return {
                "success": True,
                "session_id": session_id,
                "vnc_url": result.get("vnc_url"),
                "debug_port": debug_port,
                "message": f"Chrome started with 3 tabs: Automa, EditThisCookie, and {start_url}",
                "profile_path": profile_path,
                "startup_urls": result.get("startup_urls", []),
                "start_url": start_url
            }
        except Exception as e:
            error_msg = f"Failed to start local session: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {"success": False, "error": error_msg}

    # --------------------------------------------------------------------------
    # Local Chrome session management with custom URL selection
    # --------------------------------------------------------------------------
    def _render_local_chrome_tab(self, accounts_df):
        """Render Local Chrome session management with URL selection."""
        st.subheader("🖥️ Local Chrome Session Management")
        st.info("✓ Free • ✓ Persistent profiles • ✓ Account-based cookies • ✓ Custom start URLs")
        
        # Add info about the default URL
        st.info("ℹ️ **Default URL:** All sessions will start at `https://mylocation.org/` if no custom URL is selected.")

        if accounts_df.empty:
            st.warning("No accounts available. Create an account first.")
            return

        local_accounts = accounts_df[accounts_df['profile_type'] == 'local_chrome']
        if local_accounts.empty:
            st.warning("No Local Chrome accounts found. Create one in the Overview tab.")
            return

        col1, col2 = st.columns([2, 1])

        with col1:
            st.subheader("Active Sessions")
            local_sessions = st.session_state.get('local_chrome_sessions', {})

            if local_sessions:
                accounts_with_sessions = {}
                for session_id, session_info in local_sessions.items():
                    acc_user = session_info.get('account_username', 'Unknown')
                    accounts_with_sessions.setdefault(acc_user, []).append((session_id, session_info))

                for acc_user, sessions in accounts_with_sessions.items():
                    with st.container():
                        st.markdown(f"**👤 Account: {acc_user}**")
                        st.caption(f"{len(sessions)} session(s)")

                        for session_id, session_info in sessions:
                            s1, s2, s3 = st.columns([3, 2, 1])
                            with s1:
                                st.write(f"**Session:** `{session_id[:12]}...`")
                                if session_info.get('profile_path'):
                                    st.caption(f"📁 {session_info['profile_path']}")
                                if session_info.get('start_url'):
                                    st.caption(f"🌐 URL: {session_info['start_url'][:50]}...")
                                if session_info.get('debug_port'):
                                    st.caption(f"🔌 Debug: port {session_info['debug_port']}")
                            with s2:
                                started_at = session_info.get('started_at')
                                if started_at:
                                    duration = datetime.now() - started_at
                                    hours, remainder = divmod(duration.total_seconds(), 3600)
                                    minutes, _ = divmod(remainder, 60)
                                    st.write(f"**Duration:** {int(hours):02d}:{int(minutes):02d}")
                                if session_info.get('browser_url'):
                                    st.link_button("🖥️ VNC", session_info['browser_url'], use_container_width=True)
                            with s3:
                                if st.button("🛑", key=f"stop_local_{session_id[:8]}"):
                                    result = self._stop_local_chrome_session(session_id)
                                    if result.get('success'):
                                        st.success("Stopped")
                                        st.rerun()
                                    else:
                                        st.error(result.get('error'))
                            st.divider()
            else:
                st.info("No active sessions")

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
                key="local_tab_select"
            )

            if selected[1] is not None:
                row = local_accounts.iloc[selected[1]]
                account_id = row['account_id']
                
                st.info(f"**👤 Account: {row['username']}**")
                
                # URL selection - clearly show default option
                url_options = ["Default: https://mylocation.org/"]
                url_mapping = {"Default: https://mylocation.org/": "https://mylocation.org/"}
                
                # Add saved URLs for this account
                account_urls = self._get_account_urls(account_id, show_used=False)
                
                # Group URLs by site
                urls_by_site = {}
                for url_info in account_urls:
                    site_name = url_info.get('site_name', 'Other')
                    if site_name not in urls_by_site:
                        urls_by_site[site_name] = []
                    urls_by_site[site_name].append(url_info)
                
                # Add site URLs to options
                for site_name, urls in urls_by_site.items():
                    for url_info in urls:
                        default_marker = "⭐ " if url_info.get('is_default') else ""
                        label = f"{default_marker}{site_name}: {url_info['url'][:50]}..."
                        url_mapping[label] = url_info['url']
                        url_options.append(label)
                
                # Allow custom URL entry
                url_options.append("Custom URL...")
                
                selected_url_option = st.selectbox(
                    "Select Start URL:",
                    options=url_options,
                    key="start_url_select",
                    help="Choose a URL or enter a custom one. If nothing is selected, the default https://mylocation.org/ will be used."
                )
                
                start_url = None
                if selected_url_option == "Custom URL...":
                    start_url = st.text_input(
                        "Enter Custom URL:",
                        placeholder="https://example.com",
                        key="custom_url_input",
                        help="Enter any URL. If left blank, the default URL will be used."
                    )
                elif selected_url_option != "Choose account..." and selected_url_option in url_mapping:
                    start_url = url_mapping.get(selected_url_option)
                
                # Show profile status
                profile_path = self.chrome_manager.get_profile_path(row['username'])
                default_dir = os.path.join(profile_path, 'Default')
                if os.path.exists(default_dir):
                    st.success("✓ Existing profile - state preserved")
                else:
                    st.info("📁 New profile will be created")

                cookie_info = self._get_account_cookies(account_id)
                if cookie_info['has_cookies']:
                    st.success(f"✓ {cookie_info['cookie_count']} cookies stored")
                else:
                    st.warning("⚠️ No cookies stored")
                    
                # Display what URL will be used
                if selected_url_option == "Custom URL..." and start_url and start_url.strip():
                    st.info(f"🔗 Will open: {start_url}")
                else:
                    st.info("🔗 Will open default URL: https://mylocation.org/")

                if st.button("▶️ Start Chrome Session", type="primary", use_container_width=True,
                            key=f"start_local_{account_id}"):
                    with st.spinner("Starting Chrome with 3 tabs..."):
                        result = self._start_local_chrome_session(
                            row['profile_id'],
                            account_id,
                            row['username'],
                            start_url
                        )
                        if result.get('success'):
                            st.success("✓ Chrome started!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"Failed: {result.get('error')}")

    def _stop_local_chrome_session(self, session_id):
        """Stop local Chrome session with proper cleanup."""
        try:
            self.add_log(f"=== STOPPING LOCAL SESSION ===")
            self.add_log(f"Session ID: {session_id}")

            local_sessions = st.session_state.get('local_chrome_sessions', {})
            if session_id not in local_sessions:
                error_msg = f"Session {session_id} not found"
                self.add_log(error_msg, "ERROR")
                return {'success': False, 'error': 'Session not found'}

            session_info = local_sessions[session_id]
            profile_path = session_info.get('profile_path', 'Unknown')

            result = self.chrome_manager.stop_session(session_id)
            if not result['success']:
                self.add_log(f"Chrome manager stop failed: {result.get('error')}", "WARNING")
                self.chrome_manager._force_kill_all_chrome_processes()

            # Remove from session state
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
                            'session_status': 'stopped'
                        }}
                    )
                    self.add_log(f"✓ Updated MongoDB")
                except Exception as e:
                    self.add_log(f"⚠ MongoDB update failed: {e}", "WARNING")

            success_msg = f"Session stopped. Profile saved at: {profile_path}"
            self.add_log(f"✓ {success_msg}")

            return {'success': True, 'message': success_msg, 'profile_path': profile_path}
        except Exception as e:
            error_msg = f"Failed to stop session: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}

    def _ensure_account_cookie_script(self, account_id, username):
        """Ensure account cookie script exists and is up-to-date."""
        try:
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            cookie_info = self._get_account_cookies(account_id)
            if not cookie_info['has_cookies']:
                self.add_log(f"⚠️ Account {username} has no cookies stored yet", "WARNING")
                return {'success': False, 'reason': 'no_cookies'}

            script_result = self._generate_cookie_copy_script(account_id, username)
            if script_result['success']:
                self.add_log(f"✓ Account cookie script ready: {script_result['script_filename']}")
                return {'success': True, 'script_path': script_result['script_path']}
            else:
                self.add_log(f"⚠️ Could not generate cookie script: {script_result['error']}", "WARNING")
                return {'success': False, 'error': script_result['error']}
        except Exception as e:
            self.add_log(f"⚠️ Error ensuring cookie script: {e}", "WARNING")
            return {'success': False, 'error': str(e)}

    def _generate_cookie_copy_script(self, account_id, username):
        """Generate a shell script to copy cookies to clipboard."""
        try:
            cookie_info = self._get_account_cookies(account_id)
            if not cookie_info['has_cookies']:
                return {'success': False, 'error': 'No cookies found for this account'}

            cookies = cookie_info['cookie_data']
            if isinstance(cookies, str):
                cookies = json.loads(cookies)

            cookie_json = json.dumps(cookies, indent=2)
            escaped_cookie_json = cookie_json.replace("'", "'\"'\"'")

            scripts_dir = Path("/app/cookie_scripts")
            scripts_dir.mkdir(exist_ok=True, mode=0o777)

            safe_username = "".join(c for c in username if c.isalnum() or c in "-_")
            script_filename = f"copy_cookies_{safe_username}.sh"
            script_path = scripts_dir / script_filename

            profile_path = self.chrome_manager.get_profile_path(username)

            script_content = f"""#!/bin/bash
# Account: {username}
# Account ID: {account_id}
# Profile: {profile_path}
# Generated: {cookie_info['updated_at']}
# Cookie Count: {cookie_info['cookie_count']}

set -e

export DISPLAY=:99

if ! command -v xclip &> /dev/null; then
    echo "❌ xclip is not installed"
    exit 1
fi

COOKIE_DATA='{escaped_cookie_json}'

echo "$COOKIE_DATA" | xclip -selection clipboard

if [ $? -eq 0 ]; then
    echo "✅ Cookies copied to clipboard"
    echo "Paste them in EditThisCookie in VNC"
else
    echo "❌ Failed to copy to clipboard"
    exit 1
fi
"""
            with open(script_path, 'w') as f:
                f.write(script_content)
            os.chmod(script_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)

            self.add_log(f"✓ Generated account cookie script: {script_path}")
            return {
                'success': True,
                'script_path': str(script_path),
                'script_filename': script_filename,
                'cookie_count': cookie_info['cookie_count'],
                'command': f"cd /app/cookie_scripts && ./{script_filename}"
            }
        except Exception as e:
            error_msg = f"Failed to generate script: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}

    def _sync_cookies_to_session_file(self, account_id, username):
        """Sync cookies from PostgreSQL to session_data.json for workflows."""
        try:
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            self.add_log(f"Syncing cookies from PostgreSQL to session_data.json for {username}...")

            cookie_info = self._get_account_cookies(account_id)
            if not cookie_info['has_cookies']:
                return {'success': False, 'reason': 'no_cookies'}

            cookies = cookie_info['cookie_data']
            if isinstance(cookies, str):
                cookies = json.loads(cookies)

            profile_path = self.chrome_manager.get_profile_path(username)
            session_file = os.path.join(profile_path, 'session_data.json')

            critical_cookies = ['auth_token', 'ct0', 'kdt']
            cookie_names = [c['name'] for c in cookies]
            missing_critical = [name for name in critical_cookies if name not in cookie_names]
            has_auth_token = 'auth_token' in cookie_names

            session_data = {
                'timestamp': datetime.now().isoformat(),
                'accountId': account_id,
                'profileDir': profile_path,
                'cookies': cookies,
                'localStorage': {},
                'metadata': {
                    'cookieCount': len(cookies),
                    'localStorageCount': 0,
                    'savedBy': 'streamlit_cookie_sync',
                    'syncedFrom': 'postgresql',
                    'hasCriticalCookies': has_auth_token,
                    'capturedAt': datetime.now().isoformat()
                }
            }

            os.makedirs(profile_path, exist_ok=True)
            if os.path.exists(session_file):
                backup_file = f"{session_file}.backup.{int(time.time())}"
                import shutil
                shutil.copy2(session_file, backup_file)
                self.add_log(f"✓ Backed up existing session to: {backup_file}")

            temp_file = f"{session_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(session_data, f, indent=2)
            os.rename(temp_file, session_file)
            os.chmod(session_file, 0o644)

            file_size = os.path.getsize(session_file)
            self.add_log(f"✓ Synced {len(cookies)} cookies to session_data.json")
            self.add_log(f"  File: {session_file}, Size: {file_size} bytes, Has auth_token: {has_auth_token}")

            return {
                'success': True,
                'session_file': session_file,
                'cookie_count': len(cookies),
                'has_auth_token': has_auth_token,
                'missing_critical': missing_critical,
                'file_size': file_size
            }
        except Exception as e:
            error_msg = f"Failed to sync cookies: {str(e)}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}

    # --------------------------------------------------------------------------
    # Main render method
    # --------------------------------------------------------------------------
    def render(self):
        """Main render method."""
        st.title("👥 Accounts Management")

        self._render_quick_stats()
        st.markdown("---")

        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Overview",
            "🖥️ Local Chrome",
            "📈 Analytics",
            "🌐 Survey Sites"
        ])

        with st.spinner("Loading accounts data..."):
            accounts_df = self.load_accounts_data()
            survey_sites_df = self.load_survey_sites_data()

        with tab1:
            self._render_overview_tab(accounts_df)

        with tab2:
            self._render_local_chrome_tab(accounts_df)

        with tab3:
            self._render_analytics_tab(accounts_df)

        with tab4:
            self._render_survey_sites_tab(survey_sites_df)

    def _render_overview_tab(self, accounts_df):
        """Render accounts overview tab."""
        st.subheader("Accounts Overview")

        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🔄 Refresh", use_container_width=True, key="overview_refresh"):
                st.cache_data.clear()
                st.rerun()
        with col2:
            if st.button("➕ Add Account", use_container_width=True, key="overview_add"):
                st.session_state.show_add_account = True
                st.rerun()
        with col3:
            if st.button("🗑️ Delete Account", use_container_width=True, key="overview_delete"):
                st.session_state.show_delete_account = True
                st.rerun()

        st.markdown("---")
        self.render_add_account_modal()
        self.render_delete_account_modal(accounts_df)
        st.markdown("---")

        col1, col2, col3 = st.columns(3)
        with col1:
            status_filter = st.selectbox(
                "Account Type",
                ["All", "Local Chrome", "With Active Sessions"],
                key="overview_status_filter"
            )
        with col2:
            time_filter = st.selectbox(
                "Time Range",
                ["All Time", "Last 7 Days", "Last 30 Days", "Last 90 Days"],
                key="overview_time_filter"
            )
        with col3:
            view_option = st.selectbox(
                "View Mode",
                ["Accounts Overview", "Account Details", "Performance Analytics"],
                key="overview_view_option"
            )

        st.markdown("---")

        if accounts_df.empty:
            st.info("No accounts found. Use the 'Add Account' button.")
        else:
            filtered_df = self._apply_filters(accounts_df, status_filter, time_filter)
            st.write(f"Showing {len(filtered_df)} of {len(accounts_df)} accounts")

            if view_option == "Account Details":
                self._render_account_details_view(filtered_df)
            elif view_option == "Performance Analytics":
                self._render_performance_analytics_view(filtered_df)
            else:
                self._render_accounts_overview(filtered_df)

    def _apply_filters(self, df, status_filter, time_filter):
        """Apply filters to the accounts DataFrame."""
        filtered_df = df.copy()

        if status_filter == "Local Chrome":
            filtered_df = filtered_df[filtered_df['profile_type'] == 'local_chrome']
        elif status_filter == "With Active Sessions":
            filtered_df = filtered_df[filtered_df['active_sessions'] > 0]

        if time_filter != "All Time":
            days_map = {"Last 7 Days": 7, "Last 30 Days": 30, "Last 90 Days": 90}
            days = days_map[time_filter]
            cutoff_date = datetime.now() - timedelta(days=days)
            filtered_df = filtered_df[filtered_df['created_time'] >= cutoff_date]

        return filtered_df

    def _render_analytics_tab(self, accounts_df):
        """Render analytics tab focused on surveys."""
        st.subheader("📈 Account Analytics")

        if accounts_df.empty:
            st.info("No accounts to analyze")
            return

        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🔄 Refresh", use_container_width=True, key="analytics_refresh"):
                st.cache_data.clear()
                st.rerun()
        with col2:
            st.metric("Total Accounts", len(accounts_df))
        with col3:
            active_sessions = len(st.session_state.get('local_chrome_sessions', {}))
            st.metric("Active Sessions", active_sessions)

        st.markdown("---")

        st.subheader("Per-Account Performance")
        display_columns = ['username', 'country', 'profile_type', 'active_sessions',
                           'total_surveys_processed', 'has_cookies', 'created_time']
        display_df = accounts_df[display_columns].copy()
        display_df = display_df.rename(columns={
            'username': 'Username',
            'country': 'Country',
            'profile_type': 'Type',
            'active_sessions': 'Sessions',
            'total_surveys_processed': 'Surveys',
            'has_cookies': 'Cookies',
            'created_time': 'Created'
        })
        display_df['Type'] = display_df['Type'].apply(lambda x: '🖥️ Local' if x == 'local_chrome' else x)
        display_df['Cookies'] = display_df['Cookies'].apply(lambda x: '✓' if x else '✗')
        display_df['Country'] = display_df['Country'].apply(lambda x: str(x) if x and str(x).strip() else "—")
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Account Type Distribution")
            type_counts = accounts_df['profile_type'].value_counts()
            fig = px.pie(values=type_counts.values, names=type_counts.index)
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.subheader("Survey Processing")
            top_accounts = accounts_df.nlargest(10, 'total_surveys_processed')
            fig2 = px.bar(top_accounts, x='username', y='total_surveys_processed')
            st.plotly_chart(fig2, use_container_width=True)

    

    def _render_survey_sites_tab(self, survey_sites_df):
        """Render survey sites management tab with site names and account URLs."""
        st.subheader("🌐 Survey Sites Management")

        st.info("""
        **Survey Sites** are websites where surveys are found. 
        Each site has a name (e.g., "Top Surveys", "Quick Rewards") and accounts can have multiple URLs for each site.
        URLs are tracked for usage to prevent reusing the same URL for extraction.
        """)

        # Add new survey site form (by name)
        with st.expander("➕ Add New Survey Site", expanded=False):
            with st.form("add_survey_site_form"):
                col1, col2 = st.columns([2, 1])
                with col1:
                    site_name = st.text_input("Site Name *", placeholder="e.g., Top Surveys, Quick Rewards")
                with col2:
                    pass
                
                description = st.text_area("Description", placeholder="Optional description of this survey site")

                if st.form_submit_button("✅ Add Survey Site", type="primary"):
                    if not site_name.strip():
                        st.error("Site name is required!")
                    else:
                        result = self._add_or_update_survey_site(site_name.strip(), description.strip())
                        if result['success']:
                            st.success(f"✅ Added survey site '{site_name}'")
                            st.cache_data.clear()
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"❌ Failed: {result.get('error')}")

        # Account and Site selector for URL management
        if not survey_sites_df.empty:
            st.markdown("---")
            st.subheader("🔗 Manage Account URLs")
            
            # Load accounts
            accounts_df = self.load_accounts_data()
            if not accounts_df.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    account_options = {f"{row['username']} (ID: {row['account_id']})": row['account_id'] 
                                      for _, row in accounts_df.iterrows()}
                    selected_account = st.selectbox(
                        "Select Account:",
                        options=list(account_options.keys()),
                        key="url_account_select"
                    )
                    account_id = account_options[selected_account]
                
                with col2:
                    site_options = {f"{row['site_name']}": row['site_id'] 
                                   for _, row in survey_sites_df.iterrows()}
                    selected_site = st.selectbox(
                        "Select Survey Site:",
                        options=list(site_options.keys()),
                        key="url_site_select"
                    )
                    site_id = site_options[selected_site]
                
                # Show existing URLs for this account/site
                account_urls = self._get_account_urls(account_id, site_id, show_used=True)
                
                # Separate used and unused URLs
                unused_urls = [u for u in account_urls if not u.get('is_used', False)]
                used_urls = [u for u in account_urls if u.get('is_used', False)]
                
                # Display unused URLs first
                if unused_urls:
                    st.markdown(f"**✅ Available URLs ({len(unused_urls)}):**")
                    for url_info in unused_urls:
                        with st.container():
                            col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
                            
                            with col1:
                                default_marker = "⭐ " if url_info.get('is_default') else ""
                                st.write(f"{default_marker}{url_info['url']}")
                                if url_info.get('notes'):
                                    st.caption(url_info['notes'])
                            
                            with col2:
                                if st.button("✏️ Edit", key=f"edit_url_{url_info['url_id']}", use_container_width=True):
                                    st.session_state[f'editing_url_{url_info["url_id"]}'] = True
                            
                            with col3:
                                if not url_info.get('is_default'):
                                    if st.button("⭐ Set Default", key=f"default_url_{url_info['url_id']}", use_container_width=True):
                                        self._update_account_url(url_info['url_id'], url_info['url'], True, url_info.get('notes', ''))
                                        st.rerun()
                            
                            with col4:
                                if st.button("✅ Mark Used", key=f"mark_used_{url_info['url_id']}", use_container_width=True):
                                    self._mark_url_used(url_info['url_id'])
                                    st.rerun()
                            
                            with col5:
                                if st.button("🗑️ Delete", key=f"delete_url_{url_info['url_id']}", use_container_width=True):
                                    st.session_state[f'confirm_delete_url_{url_info["url_id"]}'] = True
                            
                            # Edit form
                            if st.session_state.get(f'editing_url_{url_info["url_id"]}', False):
                                st.divider()
                                with st.form(key=f"edit_url_form_{url_info['url_id']}"):
                                    new_url = st.text_input("URL", value=url_info['url'])
                                    is_default = st.checkbox("Set as Default", value=url_info.get('is_default', False))
                                    notes = st.text_area("Notes", value=url_info.get('notes', ''))
                                    
                                    col_save, col_cancel = st.columns(2)
                                    with col_save:
                                        if st.form_submit_button("💾 Save", use_container_width=True):
                                            result = self._update_account_url(url_info['url_id'], new_url, is_default, notes)
                                            if result['success']:
                                                del st.session_state[f'editing_url_{url_info["url_id"]}']
                                                st.rerun()
                                            else:
                                                st.error(result.get('error'))
                                    with col_cancel:
                                        if st.form_submit_button("Cancel", use_container_width=True):
                                            del st.session_state[f'editing_url_{url_info["url_id"]}']
                                            st.rerun()
                            
                            # Delete confirmation
                            if st.session_state.get(f'confirm_delete_url_{url_info["url_id"]}', False):
                                st.warning(f"Delete this URL?")
                                col_yes, col_no = st.columns(2)
                                with col_yes:
                                    if st.button("✅ Yes", key=f"confirm_yes_url_{url_info['url_id']}"):
                                        result = self._delete_account_url(url_info['url_id'])
                                        if result['success']:
                                            del st.session_state[f'confirm_delete_url_{url_info["url_id"]}']
                                            st.rerun()
                                with col_no:
                                    if st.button("❌ No", key=f"confirm_no_url_{url_info['url_id']}"):
                                        del st.session_state[f'confirm_delete_url_{url_info["url_id"]}']
                                        st.rerun()
                            
                            st.divider()
                
                # Display used URLs
                if used_urls:
                    with st.expander(f"📋 Used URLs ({len(used_urls)})", expanded=False):
                        for url_info in used_urls:
                            col1, col2, col3 = st.columns([3, 1, 1])
                            with col1:
                                st.write(f"~~{url_info['url']}~~")
                                if url_info.get('used_at'):
                                    st.caption(f"Used: {url_info['used_at'].strftime('%Y-%m-%d %H:%M')}")
                            with col2:
                                if st.button("↩️ Reset", key=f"reset_url_{url_info['url_id']}", use_container_width=True):
                                    self._mark_url_unused(url_info['url_id'])
                                    st.rerun()
                            with col3:
                                if st.button("🗑️ Delete", key=f"delete_used_{url_info['url_id']}", use_container_width=True):
                                    st.session_state[f'confirm_delete_url_{url_info["url_id"]}'] = True
                            
                            if st.session_state.get(f'confirm_delete_url_{url_info["url_id"]}', False):
                                col_yes, col_no = st.columns(2)
                                with col_yes:
                                    if st.button("✅ Yes", key=f"confirm_yes_used_{url_info['url_id']}"):
                                        result = self._delete_account_url(url_info['url_id'])
                                        if result['success']:
                                            del st.session_state[f'confirm_delete_url_{url_info["url_id"]}']
                                            st.rerun()
                                with col_no:
                                    if st.button("❌ No", key=f"confirm_no_used_{url_info['url_id']}"):
                                        del st.session_state[f'confirm_delete_url_{url_info["url_id"]}']
                                        st.rerun()
                
                # Add new URL form
                with st.expander("➕ Add New URL", expanded=not bool(account_urls)):
                    with st.form("add_account_url_form"):
                        new_url = st.text_input("URL *", placeholder="https://example.com/survey-page")
                        is_default = st.checkbox("Set as Default URL for this site")
                        notes = st.text_area("Notes (optional)", placeholder="Any notes about this URL")
                        
                        if st.form_submit_button("✅ Add URL", type="primary"):
                            if not new_url.strip():
                                st.error("URL is required!")
                            else:
                                result = self._add_account_url(account_id, site_id, new_url.strip(), is_default, notes)
                                if result['success']:
                                    st.success("✅ URL added successfully!")
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.error(f"❌ Failed: {result.get('error')}")

        # Display existing survey sites
        if survey_sites_df.empty:
            st.info("No survey sites added yet. Add one above.")
        else:
            st.markdown("---")
            st.subheader(f"Existing Survey Sites ({len(survey_sites_df)})")

            for _, row in survey_sites_df.iterrows():
                with st.expander(f"🌐 {row['site_name']}", expanded=False):
                    col1, col2, col3 = st.columns([3, 1, 1])

                    with col1:
                        st.write(f"**Site Name:** {row['site_name']}")
                        if row.get('description'):
                            st.write(f"**Description:** {row['description']}")
                        st.caption(f"Added: {row['created_at'].strftime('%Y-%m-%d %H:%M') if row.get('created_at') else 'Unknown'}")

                    with col2:
                        if st.button("✏️ Edit", key=f"edit_site_{row['site_id']}", use_container_width=True):
                            st.session_state[f'editing_site_{row["site_id"]}'] = True
                            st.rerun()

                    with col3:
                        if st.button("🗑️ Delete", key=f"delete_site_{row['site_id']}", use_container_width=True):
                            st.session_state[f'confirm_delete_site_{row["site_id"]}'] = True

                    # Edit form
                    if st.session_state.get(f'editing_site_{row["site_id"]}', False):
                        st.divider()
                        with st.form(key=f"edit_site_form_{row['site_id']}"):
                            new_name = st.text_input("Site Name", value=row['site_name'])
                            new_description = st.text_area("Description", value=row.get('description', ''))

                            col_save, col_cancel = st.columns(2)
                            with col_save:
                                if st.form_submit_button("💾 Save Changes", use_container_width=True):
                                    result = self._add_or_update_survey_site(
                                        new_name.strip(),
                                        new_description.strip(),
                                        site_id=row['site_id']
                                    )
                                    if result['success']:
                                        st.success("✅ Updated!")
                                        del st.session_state[f'editing_site_{row["site_id"]}']
                                        st.cache_data.clear()
                                        st.rerun()
                                    else:
                                        st.error(f"❌ Failed: {result.get('error')}")
                            with col_cancel:
                                if st.form_submit_button("Cancel", use_container_width=True):
                                    del st.session_state[f'editing_site_{row["site_id"]}']
                                    st.rerun()

                    # Delete confirmation
                    if st.session_state.get(f'confirm_delete_site_{row["site_id"]}', False):
                        st.warning(f"Delete survey site '{row['site_name']}'? This will also delete all associated account URLs.")
                        col_yes, col_no = st.columns(2)
                        with col_yes:
                            if st.button("✅ Yes, Delete", key=f"confirm_yes_{row['site_id']}"):
                                result = self._delete_survey_site(row['site_id'])
                                if result['success']:
                                    st.success("✅ Deleted!")
                                    del st.session_state[f'confirm_delete_site_{row["site_id"]}']
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.error(f"❌ Failed: {result.get('error')}")
                        with col_no:
                            if st.button("❌ No", key=f"confirm_no_{row['site_id']}"):
                                del st.session_state[f'confirm_delete_site_{row["site_id"]}']
                                st.rerun()