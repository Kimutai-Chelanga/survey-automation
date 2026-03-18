"""
Simplified Prompts Page - One prompt per user for answering questions
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import time
import logging
from typing import Optional

from src.core.database.postgres import prompts as pg_prompts
from src.core.database.postgres import accounts as pg_utils
from src.core.database.postgres.connection import get_postgres_connection
from psycopg2.extras import RealDictCursor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PromptsPage:
    """Simplified Prompts page - One prompt per user for answering questions."""

    def __init__(self, db_manager):
        self.db_manager = db_manager

        # Initialize session state
        if 'prompt_operation_logs' not in st.session_state:
            st.session_state.prompt_operation_logs = []

        if 'editing_prompt_id' not in st.session_state:
            st.session_state.editing_prompt_id = None

    def add_log(self, message, level="INFO"):
        """Add a log message."""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] {level}: {message}"

        if 'prompt_operation_logs' not in st.session_state:
            st.session_state.prompt_operation_logs = []
        st.session_state.prompt_operation_logs.append(log_entry)

        if len(st.session_state.prompt_operation_logs) > 50:
            st.session_state.prompt_operation_logs = st.session_state.prompt_operation_logs[-50:]

        if level == "ERROR":
            logger.error(message)
        elif level == "WARNING":
            logger.warning(message)
        else:
            logger.info(message)

    def clear_logs(self):
        """Clear logs."""
        st.session_state.prompt_operation_logs = []

    def render_operation_logs(self):
        """Render logs."""
        if st.session_state.get('prompt_operation_logs'):
            with st.expander("🔍 Operation Logs", expanded=True):
                log_text = "\n".join(st.session_state.prompt_operation_logs)
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
                        file_name=f"prompt_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                        mime="text/plain"
                    )

    @st.cache_data(ttl=60)
    def load_accounts(_self) -> pd.DataFrame:
        """Load accounts for selection."""
        try:
            accounts_data = pg_utils.get_all_accounts()
            if not accounts_data:
                return pd.DataFrame(columns=['account_id', 'username', 'country'])

            df = pd.DataFrame(accounts_data)
            required_cols = ['account_id', 'username']
            if 'country' not in df.columns:
                df['country'] = 'Unknown'

            return df[required_cols + ['country']].drop_duplicates()

        except Exception as e:
            logger.error(f"Error loading accounts: {e}")
            return pd.DataFrame(columns=['account_id', 'username', 'country'])

    @st.cache_data(ttl=60)
    def load_prompts_data(_self) -> pd.DataFrame:
        """Load all prompts (one per user)."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT 
                            p.prompt_id,
                            p.account_id,
                            a.username,
                            a.country,
                            p.name as prompt_name,
                            p.content,
                            p.prompt_type,
                            p.created_time,
                            p.updated_time,
                            p.is_active,
                            COUNT(DISTINCT ans.answer_id) as answer_count
                        FROM prompts p
                        LEFT JOIN accounts a ON p.account_id = a.account_id
                        LEFT JOIN answers ans ON a.account_id = ans.account_id
                        GROUP BY p.prompt_id, p.account_id, a.username, a.country, 
                                 p.name, p.content, p.prompt_type, p.created_time, 
                                 p.updated_time, p.is_active
                        ORDER BY a.username
                    """)
                    return pd.DataFrame([dict(row) for row in cursor.fetchall()])

        except Exception as e:
            logger.error(f"Error loading prompts: {e}")
            return pd.DataFrame()

    def render(self):
        """Main render method."""
        st.title("📝 User Prompts")

        st.info("""
        **Each user has one prompt** that defines their persona for answering survey questions.
        This prompt tells the AI how to answer questions as this specific user.
        """)

        # Load data
        with st.spinner("Loading data..."):
            accounts_df = self.load_accounts()
            prompts_df = self.load_prompts_data()

        # Show stats
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Users", len(accounts_df))
        with col2:
            users_with_prompts = len(prompts_df['account_id'].unique()) if not prompts_df.empty else 0
            st.metric("Users with Prompts", users_with_prompts)
        with col3:
            total_answers = prompts_df['answer_count'].sum() if not prompts_df.empty else 0
            st.metric("Total Answers Generated", int(total_answers))

        st.markdown("---")

        # Split view: Users without prompts (left) and Users with prompts (right)
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("🆕 Users Needing Prompts")

            # Find users without prompts
            if not accounts_df.empty:
                users_with_prompts_ids = prompts_df['account_id'].unique() if not prompts_df.empty else []
                users_needing_prompts = accounts_df[~accounts_df['account_id'].isin(users_with_prompts_ids)]

                if users_needing_prompts.empty:
                    st.success("✅ All users have prompts!")
                else:
                    st.warning(f"{len(users_needing_prompts)} users need prompts")

                    for _, user in users_needing_prompts.iterrows():
                        with st.container():
                            st.markdown(f"**👤 {user['username']}** (Country: {user.get('country', 'Unknown')})")

                            # Create prompt form for this user
                            with st.form(key=f"create_prompt_{user['account_id']}"):
                                prompt_name = st.text_input(
                                    "Prompt Name",
                                    value=f"{user['username']}_persona",
                                    key=f"name_{user['account_id']}"
                                )

                                prompt_content = st.text_area(
                                    "User Persona Prompt",
                                    height=200,
                                    placeholder=f"Example: You are {user['username']}, a {user.get('country', '')} resident who...",
                                    key=f"content_{user['account_id']}"
                                )

                                col_submit, _ = st.columns([1, 1])
                                with col_submit:
                                    submitted = st.form_submit_button(
                                        "✅ Create Prompt",
                                        use_container_width=True,
                                        type="primary"
                                    )

                                if submitted:
                                    if not prompt_content.strip():
                                        st.error("Prompt content is required!")
                                    else:
                                        try:
                                            prompt_id = pg_prompts.create_prompt(
                                                account_id=int(user['account_id']),
                                                name=prompt_name.strip(),
                                                content=prompt_content.strip(),
                                                prompt_type='user_persona',
                                                is_active=True
                                            )

                                            if prompt_id:
                                                self.add_log(f"✅ Created prompt for {user['username']}")
                                                st.success("Prompt created!")
                                                st.cache_data.clear()
                                                time.sleep(1)
                                                st.rerun()
                                            else:
                                                st.error("Failed to create prompt")
                                        except Exception as e:
                                            st.error(f"Error: {e}")
                                            self.add_log(f"Error: {e}", "ERROR")

                            st.markdown("---")
            else:
                st.info("No accounts found")

        with col_right:
            st.subheader("✅ Users with Prompts")

            if prompts_df.empty:
                st.info("No prompts created yet")
            else:
                for _, prompt in prompts_df.iterrows():
                    with st.expander(
                        f"👤 **{prompt['username']}** - {prompt.get('country', 'Unknown')} "
                        f"({prompt.get('answer_count', 0)} answers)",
                        expanded=(st.session_state.editing_prompt_id == prompt['prompt_id'])
                    ):
                        col1, col2 = st.columns([3, 1])

                        with col1:
                            if st.session_state.editing_prompt_id == prompt['prompt_id']:
                                # Edit mode
                                with st.form(key=f"edit_form_{prompt['prompt_id']}"):
                                    new_name = st.text_input(
                                        "Prompt Name",
                                        value=prompt['prompt_name'],
                                        key=f"edit_name_{prompt['prompt_id']}"
                                    )

                                    new_content = st.text_area(
                                        "Prompt Content",
                                        value=prompt['content'],
                                        height=200,
                                        key=f"edit_content_{prompt['prompt_id']}"
                                    )

                                    is_active = st.checkbox(
                                        "Active",
                                        value=prompt['is_active'],
                                        key=f"edit_active_{prompt['prompt_id']}"
                                    )

                                    col_save, col_cancel = st.columns(2)

                                    with col_save:
                                        if st.form_submit_button("💾 Save", use_container_width=True, type="primary"):
                                            success = pg_prompts.update_prompt(
                                                prompt_id=int(prompt['prompt_id']),
                                                name=new_name.strip(),
                                                content=new_content.strip(),
                                                is_active=is_active
                                            )

                                            if success:
                                                self.add_log(f"✅ Updated prompt for {prompt['username']}")
                                                st.success("Prompt updated!")
                                                st.session_state.editing_prompt_id = None
                                                st.cache_data.clear()
                                                time.sleep(1)
                                                st.rerun()
                                            else:
                                                st.error("Failed to update prompt")

                                    with col_cancel:
                                        if st.form_submit_button("Cancel", use_container_width=True):
                                            st.session_state.editing_prompt_id = None
                                            st.rerun()
                            else:
                                # View mode
                                st.markdown(f"**Name:** {prompt['prompt_name']}")
                                st.markdown(f"**Type:** `{prompt['prompt_type']}`")
                                st.markdown(f"**Created:** {prompt['created_time'].strftime('%Y-%m-%d %H:%M')}")
                                st.markdown(f"**Status:** {'✅ Active' if prompt['is_active'] else '❌ Inactive'}")

                                with st.container():
                                    st.markdown("**Prompt Content:**")
                                    st.info(prompt['content'])

                        with col2:
                            st.markdown("**Actions:**")

                            if st.button("✏️ Edit", key=f"edit_{prompt['prompt_id']}", use_container_width=True):
                                st.session_state.editing_prompt_id = prompt['prompt_id']
                                st.rerun()

                            # Preview button to see how this prompt would answer
                            if st.button("🔍 Preview", key=f"preview_{prompt['prompt_id']}", use_container_width=True):
                                with st.popover("Answer Preview"):
                                    st.markdown("**Sample Question:** What is your favorite feature of this survey site?")
                                    st.markdown("**Sample Answer:**")
                                    st.info("Based on your persona, I would generate an answer that matches your profile...")

                            if not prompt['is_active']:
                                if st.button("✅ Activate", key=f"activate_{prompt['prompt_id']}", use_container_width=True):
                                    pg_prompts.update_prompt(
                                        prompt_id=int(prompt['prompt_id']),
                                        is_active=True
                                    )
                                    self.add_log(f"✅ Activated prompt for {prompt['username']}")
                                    st.cache_data.clear()
                                    st.rerun()

                        st.markdown("---")

        # Show logs
        st.markdown("---")
        self.render_operation_logs()