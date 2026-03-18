import streamlit as st
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import logging

from src.core.database.postgres import accounts as pg_accounts
from src.core.database.postgres.connection import get_postgres_connection
from psycopg2.extras import RealDictCursor
from ..base_page import BasePage

logger = logging.getLogger(__name__)


class DashboardPage(BasePage):
    """Clean dashboard focused on survey statistics."""

    def __init__(self, db_manager):
        super().__init__(db_manager)

    def render(self):
        """Main render method."""
        try:
            st.set_page_config(
                page_title="Survey Dashboard",
                layout="wide",
                initial_sidebar_state="expanded"
            )
        except:
            pass

        st.title("📊 Survey Dashboard")
        st.markdown("---")

        # Load data
        with st.spinner("Loading dashboard data..."):
            accounts_df = self._load_accounts_data()
            questions_df = self._load_questions_data()
            answers_df = self._load_answers_data()

        # Top metrics
        self._render_top_metrics(accounts_df, questions_df, answers_df)
        st.markdown("---")

        # Main dashboard sections
        col1, col2 = st.columns(2)

        with col1:
            self._render_accounts_overview(accounts_df)
            self._render_questions_overview(questions_df)

        with col2:
            self._render_answers_overview(answers_df)
            self._render_recent_activity(answers_df)

        st.markdown("---")

        # Bottom charts
        col3, col4 = st.columns(2)

        with col3:
            self._render_questions_by_type_chart(questions_df)

        with col4:
            self._render_answers_timeline_chart(answers_df)

    def _load_accounts_data(self) -> pd.DataFrame:
        """Load accounts data."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT 
                            a.account_id,
                            a.username,
                            a.country,
                            a.created_time,
                            a.is_active,
                            a.total_surveys_processed,
                            a.age,
                            a.gender,
                            a.city,
                            a.education_level,
                            a.job_status,
                            a.industry,
                            a.income_range,
                            a.marital_status,
                            a.household_size,
                            a.has_children,
                            COUNT(DISTINCT q.question_id) as question_count,
                            COUNT(DISTINCT ans.answer_id) as answer_count
                        FROM accounts a
                        LEFT JOIN questions q ON a.account_id = q.account_id
                        LEFT JOIN answers ans ON a.account_id = ans.account_id
                        GROUP BY a.account_id, a.username, a.country, a.created_time,
                                 a.is_active, a.total_surveys_processed,
                                 a.age, a.gender, a.city, a.education_level,
                                 a.job_status, a.industry, a.income_range,
                                 a.marital_status, a.household_size, a.has_children
                        ORDER BY a.created_time DESC
                    """)
                    return pd.DataFrame([dict(row) for row in cursor.fetchall()])
        except Exception as e:
            logger.error(f"Error loading accounts: {e}")
            st.error(f"Database error: {e}")
            return pd.DataFrame()

    def _load_questions_data(self) -> pd.DataFrame:
        """Load questions data - FIXED: removed reference to ss.country."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT 
                            q.question_id,
                            q.survey_site_id,
                            q.account_id,
                            q.question_text,
                            q.question_type,
                            q.question_category,
                            q.click_element,
                            q.input_element,
                            q.submit_element,
                            q.extracted_at,
                            q.is_active,
                            q.used_in_workflow,
                            q.extraction_batch_id,
                            ss.site_name as survey_site_name,
                            ss.description as survey_site_description,
                            COUNT(ans.answer_id) as answer_count
                        FROM questions q
                        LEFT JOIN survey_sites ss ON q.survey_site_id = ss.site_id
                        LEFT JOIN answers ans ON q.question_id = ans.question_id
                        GROUP BY q.question_id, q.survey_site_id, q.account_id, q.question_text, 
                                 q.question_type, q.question_category, q.click_element,
                                 q.input_element, q.submit_element, q.extracted_at, q.is_active,
                                 q.used_in_workflow, q.extraction_batch_id, ss.site_name, ss.description
                        ORDER BY q.extracted_at DESC
                    """)
                    return pd.DataFrame([dict(row) for row in cursor.fetchall()])
        except Exception as e:
            logger.error(f"Error loading questions: {e}")
            st.error(f"Database error: {e}")
            return pd.DataFrame()

    def _load_answers_data(self) -> pd.DataFrame:
        """Load answers data - FIXED: removed reference to ss.country."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT 
                            a.answer_id,
                            a.question_id,
                            a.account_id,
                            a.answer_text,
                            a.answer_value_numeric,
                            a.answer_value_boolean,
                            a.submitted_at,
                            a.submission_batch_id,
                            a.workflow_id,
                            q.question_text,
                            q.question_type,
                            q.question_category,
                            ss.site_name as survey_site_name,
                            acc.username as account_username,
                            w.workflow_name
                        FROM answers a
                        LEFT JOIN questions q ON a.question_id = q.question_id
                        LEFT JOIN survey_sites ss ON q.survey_site_id = ss.site_id
                        LEFT JOIN accounts acc ON a.account_id = acc.account_id
                        LEFT JOIN workflows w ON a.workflow_id = w.workflow_id
                        ORDER BY a.submitted_at DESC
                        LIMIT 1000
                    """)
                    return pd.DataFrame([dict(row) for row in cursor.fetchall()])
        except Exception as e:
            logger.error(f"Error loading answers: {e}")
            st.error(f"Database error: {e}")
            return pd.DataFrame()

    def _render_top_metrics(self, accounts_df: pd.DataFrame, questions_df: pd.DataFrame, answers_df: pd.DataFrame):
        """Render top metrics row."""
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric(
                "Total Accounts",
                len(accounts_df) if not accounts_df.empty else 0
            )

        with col2:
            if not accounts_df.empty and 'is_active' in accounts_df.columns:
                active_accounts = int(accounts_df['is_active'].sum())
            else:
                active_accounts = 0
            st.metric("Active Accounts", active_accounts)

        with col3:
            st.metric(
                "Total Questions",
                len(questions_df) if not questions_df.empty else 0
            )

        with col4:
            st.metric(
                "Total Answers",
                len(answers_df) if not answers_df.empty else 0
            )

        with col5:
            response_rate = 0
            if not questions_df.empty and len(answers_df) > 0:
                response_rate = round((len(answers_df) / len(questions_df)) * 100, 1)
            st.metric("Response Rate", f"{response_rate}%")

    def _render_accounts_overview(self, accounts_df: pd.DataFrame):
        """Render accounts overview section."""
        st.subheader("👥 Accounts Overview")

        if accounts_df.empty:
            st.info("No accounts found")
            return

        total_surveys = accounts_df['total_surveys_processed'].sum() if 'total_surveys_processed' in accounts_df.columns else 0
        countries = accounts_df['country'].nunique() if 'country' in accounts_df.columns else 0

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Surveys Processed", int(total_surveys))
        with col2:
            st.metric("Countries", countries)

        # Show demographic stats
        if 'age' in accounts_df.columns:
            avg_age = accounts_df['age'].mean()
            if pd.notna(avg_age):
                st.metric("Average Age", f"{avg_age:.0f}")

        st.markdown("**Top Accounts by Answers**")
        if 'answer_count' in accounts_df.columns:
            top_accounts = accounts_df.nlargest(5, 'answer_count')[['username', 'answer_count', 'country']]
            if not top_accounts.empty:
                st.dataframe(top_accounts, use_container_width=True, hide_index=True)

    def _render_questions_overview(self, questions_df: pd.DataFrame):
        """Render questions overview section."""
        st.subheader("❓ Questions Overview")

        if questions_df.empty:
            st.info("No questions found")
            return

        if 'question_type' in questions_df.columns:
            type_counts = questions_df['question_type'].value_counts()
            st.markdown("**Questions by Type**")
            for q_type, count in type_counts.items():
                st.markdown(f"- **{q_type}:** {count}")

        if 'question_category' in questions_df.columns:
            category_counts = questions_df['question_category'].value_counts().head(5)
            st.markdown("**Top Categories**")
            for cat, count in category_counts.items():
                if pd.notna(cat):
                    st.markdown(f"- **{cat}:** {count}")

        st.markdown("**Most Answered Questions**")
        if 'answer_count' in questions_df.columns:
            top_questions = questions_df.nlargest(5, 'answer_count')[['question_text', 'answer_count', 'survey_site_name']]
            if not top_questions.empty:
                for _, row in top_questions.iterrows():
                    st.markdown(f"- {row['question_text'][:50]}... ({row['answer_count']} answers)")

    def _render_answers_overview(self, answers_df: pd.DataFrame):
        """Render answers overview section."""
        st.subheader("📝 Answers Overview")

        if answers_df.empty:
            st.info("No answers found")
            return

        today = datetime.now().date()
        today_answers = 0
        if 'submitted_at' in answers_df.columns:
            today_answers = len([
                d for d in answers_df['submitted_at']
                if d and hasattr(d, 'date') and d.date() == today
            ])

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Today's Answers", today_answers)
        with col2:
            st.metric("Avg per Day", round(len(answers_df) / 30, 1) if len(answers_df) > 0 else 0)

        st.markdown("**Recent Answers**")
        recent = answers_df.head(5)
        for _, row in recent.iterrows():
            answer_preview = str(row.get('answer_text') or '')[:100]
            site_name = row.get('survey_site_name', 'Unknown')
            st.caption(
                f"**{row.get('account_username', 'Unknown')}** on "
                f"{site_name}: {answer_preview}..."
            )

    def _render_recent_activity(self, answers_df: pd.DataFrame):
        """Render recent activity section."""
        st.subheader("🕒 Recent Activity")

        if answers_df.empty:
            st.info("No recent activity")
            return

        last_7_days = datetime.now() - timedelta(days=7)
        recent_answers = answers_df[
            pd.to_datetime(answers_df['submitted_at']) >= last_7_days
        ] if 'submitted_at' in answers_df.columns else pd.DataFrame()

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Last 7 Days", len(recent_answers))
        with col2:
            unique_accounts = recent_answers['account_username'].nunique() if 'account_username' in recent_answers.columns else 0
            st.metric("Active Accounts", unique_accounts)

        if 'submission_batch_id' in answers_df.columns:
            batches = answers_df['submission_batch_id'].dropna().unique()[:3]
            if len(batches):
                st.markdown("**Latest Submission Batches**")
                for batch in batches:
                    st.code(str(batch)[:30] + "...", language=None)

        # Show workflow usage
        if 'workflow_name' in answers_df.columns:
            workflow_counts = answers_df['workflow_name'].value_counts().head(3)
            if not workflow_counts.empty:
                st.markdown("**Top Workflows**")
                for wf_name, count in workflow_counts.items():
                    if wf_name:  # Skip None values
                        st.caption(f"- {wf_name}: {count} answers")

    def _render_questions_by_type_chart(self, questions_df: pd.DataFrame):
        """Render questions by type chart."""
        st.subheader("📊 Questions by Type")

        if questions_df.empty or 'question_type' not in questions_df.columns:
            st.info("No data for chart")
            return

        type_counts = questions_df['question_type'].value_counts().reset_index()
        type_counts.columns = ['Type', 'Count']

        fig = px.pie(
            type_counts,
            values='Count',
            names='Type',
            title="Question Type Distribution"
        )
        st.plotly_chart(fig, use_container_width=True)

    def _render_answers_timeline_chart(self, answers_df: pd.DataFrame):
        """Render answers timeline chart."""
        st.subheader("📈 Answers Timeline")

        if answers_df.empty or 'submitted_at' not in answers_df.columns:
            st.info("No timeline data")
            return

        # Convert to datetime and group by date
        answers_df['date'] = pd.to_datetime(answers_df['submitted_at']).dt.date
        timeline = answers_df.groupby('date').size().reset_index()
        timeline.columns = ['Date', 'Count']

        fig = px.line(
            timeline,
            x='Date',
            y='Count',
            title="Answers Over Time"
        )
        st.plotly_chart(fig, use_container_width=True)