"""
Enhanced Prompts Page - One prompt per user using all available demographic information
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import time
import logging
import json
from typing import Optional, Dict, Any

from src.core.database.postgres import prompts as pg_prompts
from src.core.database.postgres import accounts as pg_utils
from src.core.database.postgres.connection import get_postgres_connection
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# All demographic fields that may or may not exist in a given row
DEMOGRAPHIC_FIELDS = [
    'age', 'gender', 'city', 'country', 'education_level', 'job_status',
    'industry', 'income_range', 'marital_status', 'household_size', 'has_children',
    'shopping_habits', 'brands_used', 'hobbies', 'internet_usage', 'device_type',
    'owns_laptop', 'owns_tv', 'internet_provider'
]


class PromptsPage:
    """Enhanced Prompts page - One prompt per user using all available demographic info."""

    def __init__(self, db_manager):
        self.db_manager = db_manager

        if 'prompt_operation_logs' not in st.session_state:
            st.session_state.prompt_operation_logs = []
        if 'editing_prompt_id' not in st.session_state:
            st.session_state.editing_prompt_id = None
        if 'generated_prompt' not in st.session_state:
            st.session_state.generated_prompt = None

    def add_log(self, message, level="INFO"):
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
        st.session_state.prompt_operation_logs = []

    def render_operation_logs(self):
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
    def load_accounts_with_demographics(_self) -> pd.DataFrame:
        """Load accounts with all demographic information."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            a.account_id,
                            a.username,
                            a.country,
                            a.age,
                            a.gender,
                            a.city,
                            a.education_level,
                            a.email,
                            a.phone,
                            a.job_status,
                            a.industry,
                            a.income_range,
                            a.marital_status,
                            a.household_size,
                            a.has_children,
                            a.shopping_habits,
                            a.brands_used,
                            a.hobbies,
                            a.internet_usage,
                            a.device_type,
                            a.owns_laptop,
                            a.owns_tv,
                            a.internet_provider,
                            a.created_time,
                            a.is_active
                        FROM accounts a
                        ORDER BY a.username
                    """)
                    rows = [dict(row) for row in cursor.fetchall()]
                    if not rows:
                        return pd.DataFrame()
                    return pd.DataFrame(rows)
        except Exception as e:
            logger.error(f"Error loading accounts with demographics: {e}")
            return pd.DataFrame()

    @st.cache_data(ttl=60)
    def load_prompts_data(_self) -> pd.DataFrame:
        """Load all prompts with answer counts."""
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
                    rows = [dict(row) for row in cursor.fetchall()]
                    if not rows:
                        return pd.DataFrame()
                    return pd.DataFrame(rows)
        except Exception as e:
            logger.error(f"Error loading prompts: {e}")
            return pd.DataFrame()

    def _count_users_with_demographics(self, accounts_df: pd.DataFrame) -> int:
        """Safely count users that have at least one demographic field filled."""
        if accounts_df.empty:
            return 0
        # Only check columns that actually exist in the dataframe
        check_cols = [c for c in ['age', 'gender', 'city'] if c in accounts_df.columns]
        if not check_cols:
            # Fall back: count all accounts if demographic columns aren't present
            return len(accounts_df)
        return accounts_df.dropna(subset=check_cols, how='all').shape[0]

    def _generate_prompt_from_demographics(self, user_data: Dict[str, Any]) -> str:
        """Generate a rich prompt using all available demographic information."""
        prompt_parts = []

        prompt_parts.append(f"You are {user_data.get('username', 'a user')}.")

        location_parts = []
        if user_data.get('city'):
            location_parts.append(f"from {user_data['city']}")
        if user_data.get('country'):
            location_parts.append(user_data['country'])
        if location_parts:
            prompt_parts.append(f"You are {', '.join(location_parts)}.")

        if user_data.get('age'):
            prompt_parts.append(f"You are {user_data['age']} years old.")
        if user_data.get('gender'):
            prompt_parts.append(f"Your gender is {user_data['gender']}.")
        if user_data.get('education_level'):
            prompt_parts.append(f"Your education level is {user_data['education_level']}.")

        employment_parts = []
        if user_data.get('job_status'):
            employment_parts.append(user_data['job_status'])
        if user_data.get('industry'):
            employment_parts.append(f"working in {user_data['industry']}")
        if employment_parts:
            prompt_parts.append(f"You are {' '.join(employment_parts)}.")

        if user_data.get('income_range'):
            prompt_parts.append(f"Your income range is {user_data['income_range']}.")

        household_parts = []
        if user_data.get('marital_status'):
            household_parts.append(f"you are {user_data['marital_status'].lower()}")
        if user_data.get('household_size'):
            household_parts.append(f"living in a household of {user_data['household_size']} people")
        if user_data.get('has_children') is not None:
            household_parts.append(f"{'have' if user_data['has_children'] else 'do not have'} children")
        if household_parts:
            prompt_parts.append(f"In your household, {' and '.join(household_parts)}.")

        if user_data.get('shopping_habits'):
            prompt_parts.append(f"Your shopping habits: {user_data['shopping_habits']}")
        if user_data.get('brands_used'):
            prompt_parts.append(f"Brands you use: {user_data['brands_used']}")
        if user_data.get('hobbies'):
            prompt_parts.append(f"Your hobbies include: {user_data['hobbies']}")
        if user_data.get('internet_usage'):
            prompt_parts.append(f"Your internet usage is {user_data['internet_usage']}.")

        device_parts = []
        if user_data.get('device_type'):
            device_parts.append(f"primarily use a {user_data['device_type']}")
        if user_data.get('owns_laptop') is not None:
            device_parts.append(f"{'own' if user_data['owns_laptop'] else 'do not own'} a laptop")
        if user_data.get('owns_tv') is not None:
            device_parts.append(f"{'own' if user_data['owns_tv'] else 'do not own'} a TV")
        if device_parts:
            prompt_parts.append(f"You {' and '.join(device_parts)}.")

        if user_data.get('internet_provider'):
            prompt_parts.append(f"Your internet provider is {user_data['internet_provider']}.")

        prompt_parts.append("")
        prompt_parts.append("When answering survey questions, always respond as this persona.")
        prompt_parts.append("Base your answers on your demographics, lifestyle, and preferences.")
        prompt_parts.append("Be consistent with your profile when answering questions.")
        prompt_parts.append("Answer naturally as a real person would, with appropriate detail.")

        return "\n".join(prompt_parts)

    def _format_demographic_summary(self, user_data: Dict[str, Any]) -> str:
        """Create a readable summary of available demographic information."""
        summary = []
        demographic_fields = [
            ('age', 'Age'), ('gender', 'Gender'), ('city', 'City'), ('country', 'Country'),
            ('education_level', 'Education'), ('job_status', 'Job Status'), ('industry', 'Industry'),
            ('income_range', 'Income Range'), ('marital_status', 'Marital Status'),
            ('household_size', 'Household Size'), ('has_children', 'Has Children'),
            ('shopping_habits', 'Shopping Habits'), ('brands_used', 'Brands Used'),
            ('hobbies', 'Hobbies'), ('internet_usage', 'Internet Usage'),
            ('device_type', 'Primary Device'), ('owns_laptop', 'Owns Laptop'),
            ('owns_tv', 'Owns TV'), ('internet_provider', 'Internet Provider')
        ]
        for field, label in demographic_fields:
            value = user_data.get(field)
            if value is not None and value != '':
                if field in ['has_children', 'owns_laptop', 'owns_tv']:
                    value = 'Yes' if value else 'No'
                summary.append(f"  • {label}: {value}")

        if summary:
            return "Available demographic information:\n" + "\n".join(summary)
        return "No demographic information available yet."

    def _count_filled_demo_fields(self, user_data: Dict[str, Any]) -> int:
        """Count how many demographic fields are filled for a user."""
        return sum(
            1 for field in DEMOGRAPHIC_FIELDS
            if user_data.get(field) not in [None, '']
        )

    def render(self):
        """Main render method."""
        st.title("📝 User Persona Prompts")

        st.info("""
        **Each user has one prompt** that defines their persona for answering survey questions.
        The prompt is automatically generated using all available demographic information.
        You can edit the generated prompt to customize it further.
        """)

        with st.spinner("Loading user data..."):
            accounts_df = self.load_accounts_with_demographics()
            prompts_df = self.load_prompts_data()

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Users", len(accounts_df))
        with col2:
            users_with_prompts = len(prompts_df['account_id'].unique()) if not prompts_df.empty else 0
            st.metric("Users with Prompts", users_with_prompts)
        with col3:
            total_answers = int(prompts_df['answer_count'].sum()) if not prompts_df.empty else 0
            st.metric("Total Answers Generated", total_answers)
        with col4:
            # Safe count - only use columns that exist
            st.metric("Users with Demographics", self._count_users_with_demographics(accounts_df))

        st.markdown("---")

        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("🆕 Users Needing Prompts")

            if not accounts_df.empty:
                users_with_prompts_ids = prompts_df['account_id'].unique() if not prompts_df.empty else []
                users_needing_prompts = accounts_df[~accounts_df['account_id'].isin(users_with_prompts_ids)]

                if users_needing_prompts.empty:
                    st.success("✅ All users have prompts!")
                else:
                    st.warning(f"{len(users_needing_prompts)} users need prompts")

                    for _, user in users_needing_prompts.iterrows():
                        user_dict = user.to_dict()

                        with st.container():
                            st.markdown(f"**👤 {user_dict['username']}**")

                            with st.expander("📊 View Demographic Info", expanded=False):
                                st.markdown(self._format_demographic_summary(user_dict))

                            generated_prompt = self._generate_prompt_from_demographics(user_dict)
                            demo_count = self._count_filled_demo_fields(user_dict)

                            with st.form(key=f"create_prompt_{user_dict['account_id']}"):
                                prompt_name = st.text_input(
                                    "Prompt Name",
                                    value=f"{user_dict['username']}_persona",
                                    key=f"name_{user_dict['account_id']}"
                                )
                                prompt_content = st.text_area(
                                    "User Persona Prompt",
                                    value=generated_prompt,
                                    height=300,
                                    key=f"content_{user_dict['account_id']}"
                                )

                                st.caption(f"✨ Using {demo_count} demographic fields in this prompt")

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
                                                account_id=int(user_dict['account_id']),
                                                name=prompt_name.strip(),
                                                content=prompt_content.strip(),
                                                prompt_type='user_persona',
                                                is_active=True
                                            )
                                            if prompt_id:
                                                self.add_log(f"✅ Created prompt for {user_dict['username']} using {demo_count} demographic fields")
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
                    prompt_dict = prompt.to_dict()

                    # Get full user data for this account (safe lookup)
                    user_rows = accounts_df[accounts_df['account_id'] == prompt_dict['account_id']] if not accounts_df.empty else pd.DataFrame()
                    user_data = user_rows.iloc[0].to_dict() if not user_rows.empty else {}

                    with st.expander(
                        f"👤 **{prompt_dict['username']}** - {prompt_dict.get('answer_count', 0)} answers",
                        expanded=(st.session_state.editing_prompt_id == prompt_dict['prompt_id'])
                    ):
                        col1, col2 = st.columns([3, 1])

                        with col1:
                            if st.session_state.editing_prompt_id == prompt_dict['prompt_id']:
                                with st.form(key=f"edit_form_{prompt_dict['prompt_id']}"):
                                    new_name = st.text_input("Prompt Name", value=prompt_dict['prompt_name'])
                                    new_content = st.text_area("Prompt Content", value=prompt_dict['content'], height=300)
                                    is_active = st.checkbox("Active", value=prompt_dict['is_active'])

                                    if st.form_submit_button("🔄 Regenerate from Demographics", use_container_width=True):
                                        if user_data:
                                            regenerated = self._generate_prompt_from_demographics(user_data)
                                            st.session_state.generated_prompt = regenerated
                                            st.rerun()

                                    if st.session_state.get('generated_prompt'):
                                        st.info("Preview of regenerated prompt:")
                                        st.text_area("Regenerated Content", value=st.session_state.generated_prompt, height=200, disabled=True)
                                        if st.form_submit_button("📝 Use Regenerated", use_container_width=True):
                                            new_content = st.session_state.generated_prompt
                                            st.session_state.generated_prompt = None

                                    col_save, col_cancel = st.columns(2)
                                    with col_save:
                                        if st.form_submit_button("💾 Save", use_container_width=True, type="primary"):
                                            success = pg_prompts.update_prompt(
                                                prompt_id=int(prompt_dict['prompt_id']),
                                                name=new_name.strip(),
                                                content=new_content.strip(),
                                                is_active=is_active
                                            )
                                            if success:
                                                self.add_log(f"✅ Updated prompt for {prompt_dict['username']}")
                                                st.success("Prompt updated!")
                                                st.session_state.editing_prompt_id = None
                                                st.session_state.generated_prompt = None
                                                st.cache_data.clear()
                                                time.sleep(1)
                                                st.rerun()
                                            else:
                                                st.error("Failed to update prompt")
                                    with col_cancel:
                                        if st.form_submit_button("Cancel", use_container_width=True):
                                            st.session_state.editing_prompt_id = None
                                            st.session_state.generated_prompt = None
                                            st.rerun()
                            else:
                                st.markdown(f"**Name:** {prompt_dict['prompt_name']}")
                                st.markdown(f"**Type:** `{prompt_dict['prompt_type']}`")
                                st.markdown(f"**Created:** {prompt_dict['created_time'].strftime('%Y-%m-%d %H:%M')}")
                                st.markdown(f"**Status:** {'✅ Active' if prompt_dict['is_active'] else '❌ Inactive'}")

                                if user_data:
                                    with st.expander("📊 View Source Demographics", expanded=False):
                                        st.markdown(self._format_demographic_summary(user_data))

                                st.markdown("**Prompt Content:**")
                                st.info(prompt_dict['content'])

                        with col2:
                            st.markdown("**Actions:**")

                            if st.button("✏️ Edit", key=f"edit_{prompt_dict['prompt_id']}", use_container_width=True):
                                st.session_state.editing_prompt_id = prompt_dict['prompt_id']
                                st.rerun()

                            if not prompt_dict['is_active']:
                                if st.button("✅ Activate", key=f"activate_{prompt_dict['prompt_id']}", use_container_width=True):
                                    pg_prompts.update_prompt(prompt_id=int(prompt_dict['prompt_id']), is_active=True)
                                    self.add_log(f"✅ Activated prompt for {prompt_dict['username']}")
                                    st.cache_data.clear()
                                    st.rerun()

                        st.markdown("---")

        st.markdown("---")
        self.render_operation_logs()