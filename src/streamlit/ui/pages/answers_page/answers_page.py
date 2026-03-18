# File: src/streamlit/ui/pages/accounts/answers_page.py
# Answers management page for survey responses

import streamlit as st
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
import plotly.express as px
import plotly.graph_objects as go
from src.core.database.postgres.connection import get_postgres_connection
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class AnswersPage:
    """Answers management page for survey responses."""

    def __init__(self, db_manager):
        self.db_manager = db_manager

    # =========================================================================
    # ANSWER QUERY METHODS
    # =========================================================================

    def get_answers(
        self,
        question_id: Optional[int] = None,
        account_id: Optional[int] = None,
        survey_site_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        batch_id: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get answers with optional filters."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT 
                            a.answer_id,
                            a.question_id,
                            a.account_id,
                            a.answer_text,
                            a.answer_value_numeric,
                            a.answer_value_boolean,
                            a.submitted_at,
                            a.submission_batch_id,
                            a.metadata,
                            q.question_text,
                            q.question_type,
                            q.options,
                            q.survey_site_id,
                            ss.country as survey_site_country,
                            ss.url as survey_site_url,
                            acc.username as account_username
                        FROM answers a
                        LEFT JOIN questions q ON a.question_id = q.question_id
                        LEFT JOIN survey_sites ss ON q.survey_site_id = ss.site_id
                        LEFT JOIN accounts acc ON a.account_id = acc.account_id
                        WHERE 1=1
                    """
                    params = []
                    
                    if question_id:
                        query += " AND a.question_id = %s"
                        params.append(question_id)
                    
                    if account_id:
                        query += " AND a.account_id = %s"
                        params.append(account_id)
                    
                    if survey_site_id:
                        query += " AND q.survey_site_id = %s"
                        params.append(survey_site_id)
                    
                    if start_date:
                        query += " AND a.submitted_at >= %s"
                        params.append(start_date)
                    
                    if end_date:
                        query += " AND a.submitted_at <= %s"
                        params.append(end_date)
                    
                    if batch_id:
                        query += " AND a.submission_batch_id = %s"
                        params.append(batch_id)
                    
                    query += " ORDER BY a.submitted_at DESC LIMIT %s"
                    params.append(limit)
                    
                    cursor.execute(query, params)
                    results = cursor.fetchall()
                    
                    # Parse metadata JSON
                    for row in results:
                        if row.get('metadata'):
                            row['metadata'] = json.loads(row['metadata'])
                        if row.get('options'):
                            row['options'] = json.loads(row['options'])
                    
                    return [dict(row) for row in results]

        except Exception as e:
            logger.error(f"Error getting answers: {e}")
            return []

    def get_answer_statistics(self, question_id: int) -> Dict[str, Any]:
        """Get detailed statistics for answers to a specific question."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Get question info
                    cursor.execute("""
                        SELECT question_text, question_type, options
                        FROM questions
                        WHERE question_id = %s
                    """, (question_id,))
                    
                    question = cursor.fetchone()
                    if not question:
                        return {}
                    
                    stats = {
                        'question_id': question_id,
                        'question_text': question['question_text'],
                        'question_type': question['question_type'],
                        'total_answers': 0,
                        'breakdown': {},
                        'time_series': []
                    }
                    
                    if question['question_type'] == 'multiple_choice':
                        # Count by option
                        cursor.execute("""
                            SELECT answer_text, COUNT(*) as count
                            FROM answers
                            WHERE question_id = %s
                            GROUP BY answer_text
                            ORDER BY count DESC
                        """, (question_id,))
                        
                        for row in cursor.fetchall():
                            stats['breakdown'][row['answer_text']] = row['count']
                            stats['total_answers'] += row['count']
                    
                    elif question['question_type'] == 'rating':
                        # Rating statistics
                        cursor.execute("""
                            SELECT 
                                COUNT(*) as total,
                                AVG(answer_value_numeric) as avg,
                                STDDEV(answer_value_numeric) as stddev,
                                MIN(answer_value_numeric) as min_val,
                                MAX(answer_value_numeric) as max_val,
                                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY answer_value_numeric) as median,
                                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY answer_value_numeric) as q1,
                                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY answer_value_numeric) as q3
                            FROM answers
                            WHERE question_id = %s AND answer_value_numeric IS NOT NULL
                        """, (question_id,))
                        
                        agg = cursor.fetchone()
                        if agg and agg['total'] > 0:
                            stats['total_answers'] = agg['total']
                            stats['average'] = float(agg['avg']) if agg['avg'] else None
                            stats['stddev'] = float(agg['stddev']) if agg['stddev'] else None
                            stats['min'] = float(agg['min_val']) if agg['min_val'] else None
                            stats['max'] = float(agg['max_val']) if agg['max_val'] else None
                            stats['median'] = float(agg['median']) if agg['median'] else None
                            stats['q1'] = float(agg['q1']) if agg['q1'] else None
                            stats['q3'] = float(agg['q3']) if agg['q3'] else None
                    
                    elif question['question_type'] == 'yes_no':
                        # Yes/No counts
                        cursor.execute("""
                            SELECT 
                                answer_value_boolean,
                                COUNT(*) as count
                            FROM answers
                            WHERE question_id = %s
                            GROUP BY answer_value_boolean
                        """, (question_id,))
                        
                        for row in cursor.fetchall():
                            key = 'Yes' if row['answer_value_boolean'] else 'No'
                            stats['breakdown'][key] = row['count']
                            stats['total_answers'] += row['count']
                    
                    else:  # text
                        # Text response statistics
                        cursor.execute("""
                            SELECT 
                                COUNT(*) as total,
                                COUNT(DISTINCT answer_text) as unique_responses,
                                AVG(LENGTH(answer_text)) as avg_length
                            FROM answers
                            WHERE question_id = %s
                        """, (question_id,))
                        
                        agg = cursor.fetchone()
                        stats['total_answers'] = agg['total'] if agg else 0
                        stats['unique_responses'] = agg['unique_responses'] if agg else 0
                        stats['avg_length'] = float(agg['avg_length']) if agg and agg['avg_length'] else 0
                    
                    # Get time series data
                    cursor.execute("""
                        SELECT 
                            DATE(submitted_at) as date,
                            COUNT(*) as count
                        FROM answers
                        WHERE question_id = %s
                        GROUP BY DATE(submitted_at)
                        ORDER BY date
                    """, (question_id,))
                    
                    stats['time_series'] = [dict(row) for row in cursor.fetchall()]
                    
                    return stats

        except Exception as e:
            logger.error(f"Error getting answer statistics: {e}")
            return {}

    def get_survey_summary(self, survey_site_id: Optional[int] = None) -> pd.DataFrame:
        """Get summary statistics for all surveys."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT 
                            ss.site_id,
                            ss.country as survey_site,
                            ss.url,
                            COUNT(DISTINCT q.question_id) as total_questions,
                            COUNT(DISTINCT a.answer_id) as total_answers,
                            COUNT(DISTINCT a.account_id) as unique_respondents,
                            MIN(a.submitted_at) as first_response,
                            MAX(a.submitted_at) as latest_response,
                            COUNT(DISTINCT a.submission_batch_id) as submission_batches
                        FROM survey_sites ss
                        LEFT JOIN questions q ON ss.site_id = q.survey_site_id
                        LEFT JOIN answers a ON q.question_id = a.question_id
                    """
                    params = []
                    
                    if survey_site_id:
                        query += " WHERE ss.site_id = %s"
                        params.append(survey_site_id)
                    
                    query += """
                        GROUP BY ss.site_id, ss.country, ss.url
                        ORDER BY ss.country
                    """
                    
                    cursor.execute(query, params)
                    results = cursor.fetchall()
                    
                    return pd.DataFrame([dict(row) for row in results])

        except Exception as e:
            logger.error(f"Error getting survey summary: {e}")
            return pd.DataFrame()

    def get_account_response_summary(self, account_id: int) -> Dict[str, Any]:
        """Get response summary for a specific account."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT 
                            COUNT(DISTINCT a.question_id) as questions_answered,
                            COUNT(a.answer_id) as total_responses,
                            COUNT(DISTINCT q.survey_site_id) as sites_participated,
                            MIN(a.submitted_at) as first_response,
                            MAX(a.submitted_at) as latest_response,
                            COUNT(DISTINCT a.submission_batch_id) as batches
                        FROM answers a
                        LEFT JOIN questions q ON a.question_id = q.question_id
                        WHERE a.account_id = %s
                    """, (account_id,))
                    
                    return dict(cursor.fetchone() or {})

        except Exception as e:
            logger.error(f"Error getting account response summary: {e}")
            return {}

    # =========================================================================
    # ANSWER DELETION
    # =========================================================================

    def delete_answer(self, answer_id: int) -> Dict[str, Any]:
        """Delete a single answer."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM answers WHERE answer_id = %s",
                        (answer_id,)
                    )
                    conn.commit()
                    
                    if cursor.rowcount == 0:
                        return {'success': False, 'error': 'Answer not found'}
                    
                    logger.info(f"✅ Deleted answer ID: {answer_id}")
                    return {'success': True}

        except Exception as e:
            logger.error(f"Error deleting answer: {e}")
            return {'success': False, 'error': str(e)}

    def delete_answers_by_question(self, question_id: int) -> Dict[str, Any]:
        """Delete all answers for a specific question."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM answers WHERE question_id = %s",
                        (question_id,)
                    )
                    deleted_count = cursor.rowcount
                    conn.commit()
                    
                    logger.info(f"✅ Deleted {deleted_count} answers for question {question_id}")
                    return {'success': True, 'deleted_count': deleted_count}

        except Exception as e:
            logger.error(f"Error deleting answers for question: {e}")
            return {'success': False, 'error': str(e)}

    def delete_answers_by_account(self, account_id: int) -> Dict[str, Any]:
        """Delete all answers for a specific account."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM answers WHERE account_id = %s",
                        (account_id,)
                    )
                    deleted_count = cursor.rowcount
                    conn.commit()
                    
                    logger.info(f"✅ Deleted {deleted_count} answers for account {account_id}")
                    return {'success': True, 'deleted_count': deleted_count}

        except Exception as e:
            logger.error(f"Error deleting answers for account: {e}")
            return {'success': False, 'error': str(e)}

    def delete_answers_by_batch(self, batch_id: str) -> Dict[str, Any]:
        """Delete all answers from a specific submission batch."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM answers WHERE submission_batch_id = %s",
                        (batch_id,)
                    )
                    deleted_count = cursor.rowcount
                    conn.commit()
                    
                    logger.info(f"✅ Deleted {deleted_count} answers from batch {batch_id}")
                    return {'success': True, 'deleted_count': deleted_count}

        except Exception as e:
            logger.error(f"Error deleting answers by batch: {e}")
            return {'success': False, 'error': str(e)}

    # =========================================================================
    # RENDERING METHODS
    # =========================================================================

    def render(self):
        """Main render method for answers page."""
        st.header("📝 Survey Answers")
        
        # Load accounts for filtering
        accounts = self._load_accounts()
        
        # Create tabs
        tab1, tab2, tab3, tab4 = st.tabs([
            "📋 All Answers",
            "📊 Question Analytics",
            "📈 Survey Summary",
            "⚙️ Bulk Actions"
        ])
        
        with tab1:
            self._render_answers_list(accounts)
        
        with tab2:
            self._render_question_analytics()
        
        with tab3:
            self._render_survey_summary()
        
        with tab4:
            self._render_bulk_actions(accounts)

    def _load_accounts(self) -> List[Dict[str, Any]]:
        """Load accounts for filtering."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT account_id, username, country
                        FROM accounts
                        WHERE is_active = TRUE OR is_active IS NULL
                        ORDER BY username
                    """)
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error loading accounts: {e}")
            return []

    def _render_answers_list(self, accounts: List[Dict[str, Any]]):
        """Render the answers list with filters."""
        st.subheader("📋 All Answers")
        
        # Filters
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            account_options = ["All Accounts"] + [f"{a['username']} (ID: {a['account_id']})" for a in accounts]
            selected_account = st.selectbox("Account:", account_options, key="ans_filter_account")
        
        with col2:
            date_range = st.date_input(
                "Date Range:",
                value=(datetime.now() - timedelta(days=30), datetime.now()),
                key="ans_filter_date"
            )
        
        with col3:
            batch_id = st.text_input("Batch ID:", placeholder="Filter by batch...")
        
        with col4:
            limit = st.number_input("Max Results:", min_value=10, max_value=5000, value=500, step=100)
        
        # Apply filters
        account_id = None
        if selected_account != "All Accounts":
            account_id = int(selected_account.split("ID: ")[1].rstrip(")"))
        
        start_date = None
        end_date = None
        if len(date_range) == 2:
            start_date = datetime.combine(date_range[0], datetime.min.time())
            end_date = datetime.combine(date_range[1], datetime.max.time())
        
        # Load answers
        answers = self.get_answers(
            account_id=account_id,
            start_date=start_date,
            end_date=end_date,
            batch_id=batch_id if batch_id else None,
            limit=limit
        )
        
        if not answers:
            st.info("No answers found matching the filters.")
            return
        
        # Display summary stats
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Answers", len(answers))
        
        unique_questions = len(set(a['question_id'] for a in answers if a['question_id']))
        col2.metric("Unique Questions", unique_questions)
        
        unique_batches = len(set(a['submission_batch_id'] for a in answers if a['submission_batch_id']))
        col3.metric("Submission Batches", unique_batches)
        
        if start_date and end_date:
            days = (end_date - start_date).days
            col4.metric("Date Range", f"{days} days")
        
        st.divider()
        
        # Display answers in a table
        df = pd.DataFrame([{
            'Answer ID': a['answer_id'],
            'Question': a.get('question_text', '')[:100] + ('...' if len(a.get('question_text', '')) > 100 else ''),
            'Answer': a.get('answer_text', ''),
            'Type': a.get('question_type', ''),
            'Account': a.get('account_username', 'Unknown'),
            'Site': a.get('survey_site_country', 'Unknown'),
            'Submitted': a['submitted_at'].strftime('%Y-%m-%d %H:%M') if a.get('submitted_at') else 'Unknown',
            'Batch': (a.get('submission_batch_id') or '')[:20] + '...' if a.get('submission_batch_id') and len(a.get('submission_batch_id', '')) > 20 else a.get('submission_batch_id', '')
        } for a in answers])
        
        st.dataframe(df, use_container_width=True, hide_index=True, height=500)
        
        # Export button
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download Answers CSV",
            data=csv,
            file_name=f"answers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
        
        st.divider()
        
        # Show per-answer details in expanders
        st.subheader("Detailed View")
        
        for answer in answers[:20]:  # Limit to first 20 for performance
            with st.expander(f"Answer #{answer['answer_id']} - {answer.get('question_text', '')[:80]}...", expanded=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**Answer Details**")
                    st.write(f"**Answer ID:** {answer['answer_id']}")
                    st.write(f"**Question ID:** {answer['question_id']}")
                    st.write(f"**Question:** {answer.get('question_text', 'N/A')}")
                    st.write(f"**Question Type:** {answer.get('question_type', 'N/A')}")
                    
                    if answer.get('answer_text'):
                        st.write(f"**Answer Text:** {answer['answer_text']}")
                    if answer.get('answer_value_numeric') is not None:
                        st.write(f"**Numeric Value:** {answer['answer_value_numeric']}")
                    if answer.get('answer_value_boolean') is not None:
                        st.write(f"**Boolean Value:** {'Yes' if answer['answer_value_boolean'] else 'No'}")
                
                with col2:
                    st.markdown("**Metadata**")
                    st.write(f"**Account:** {answer.get('account_username', 'Unknown')}")
                    st.write(f"**Survey Site:** {answer.get('survey_site_country', 'Unknown')}")
                    st.write(f"**Submitted:** {answer.get('submitted_at')}")
                    st.write(f"**Batch ID:** {answer.get('submission_batch_id', 'N/A')}")
                    
                    if answer.get('metadata'):
                        st.write("**Additional Metadata:**")
                        st.json(answer['metadata'])
                
                # Delete button with confirmation
                if st.button("🗑️ Delete This Answer", key=f"del_ans_{answer['answer_id']}"):
                    st.session_state[f'confirm_del_ans_{answer["answer_id"]}'] = True
                
                if st.session_state.get(f'confirm_del_ans_{answer["answer_id"]}', False):
                    col_del1, col_del2 = st.columns(2)
                    with col_del1:
                        if st.button("✅ Yes, Delete", key=f"confirm_yes_{answer['answer_id']}"):
                            result = self.delete_answer(answer['answer_id'])
                            if result['success']:
                                st.success("✅ Answer deleted!")
                                del st.session_state[f'confirm_del_ans_{answer["answer_id"]}']
                                st.rerun()
                            else:
                                st.error(f"❌ Failed: {result.get('error')}")
                    with col_del2:
                        if st.button("❌ Cancel", key=f"confirm_no_{answer['answer_id']}"):
                            del st.session_state[f'confirm_del_ans_{answer["answer_id"]}']
                            st.rerun()

    def _render_question_analytics(self):
        """Render analytics for individual questions."""
        st.subheader("📊 Question Analytics")
        
        # Load questions for selection
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT 
                            q.question_id,
                            q.question_text,
                            q.question_type,
                            ss.country as survey_site,
                            COUNT(a.answer_id) as answer_count
                        FROM questions q
                        LEFT JOIN survey_sites ss ON q.survey_site_id = ss.site_id
                        LEFT JOIN answers a ON q.question_id = a.question_id
                        GROUP BY q.question_id, ss.country
                        ORDER BY answer_count DESC
                        LIMIT 100
                    """)
                    questions = cursor.fetchall()
        except Exception as e:
            st.error(f"Error loading questions: {e}")
            return
        
        if not questions:
            st.info("No questions found.")
            return
        
        # Question selector
        question_options = {
            f"{q['question_text'][:100]}... ({q['survey_site']}) - {q['answer_count']} answers": q['question_id']
            for q in questions
        }
        
        selected_question = st.selectbox(
            "Select Question:",
            options=list(question_options.keys())
        )
        
        if not selected_question:
            return
        
        question_id = question_options[selected_question]
        
        # Get statistics
        stats = self.get_answer_statistics(question_id)
        
        if not stats:
            st.warning("No statistics available for this question.")
            return
        
        st.divider()
        
        # Question info
        st.markdown(f"### {stats.get('question_text', 'Unknown Question')}")
        st.markdown(f"**Type:** `{stats.get('question_type')}`")
        st.markdown(f"**Total Answers:** {stats.get('total_answers', 0)}")
        
        st.divider()
        
        # Visualizations based on question type
        col1, col2 = st.columns(2)
        
        with col1:
            if stats['question_type'] == 'multiple_choice':
                # Pie chart for multiple choice
                if stats.get('breakdown'):
                    fig = px.pie(
                        values=list(stats['breakdown'].values()),
                        names=list(stats['breakdown'].keys()),
                        title="Response Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            elif stats['question_type'] == 'rating':
                # Box plot for ratings
                if stats.get('q1') and stats.get('q3'):
                    fig = go.Figure()
                    fig.add_trace(go.Box(
                        q1=[stats['q1']],
                        median=[stats['median']],
                        q3=[stats['q3']],
                        lowerfence=[stats['min']],
                        upperfence=[stats['max']],
                        mean=[stats['average']],
                        name="Rating Distribution"
                    ))
                    fig.update_layout(title="Rating Distribution")
                    st.plotly_chart(fig, use_container_width=True)
            
            elif stats['question_type'] == 'yes_no':
                # Bar chart for Yes/No
                if stats.get('breakdown'):
                    fig = px.bar(
                        x=list(stats['breakdown'].keys()),
                        y=list(stats['breakdown'].values()),
                        title="Yes/No Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Time series
            if stats.get('time_series'):
                ts_df = pd.DataFrame(stats['time_series'])
                fig = px.line(
                    ts_df,
                    x='date',
                    y='count',
                    title="Responses Over Time"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        st.divider()
        
        # Detailed statistics table
        st.subheader("Detailed Statistics")
        
        stats_df = pd.DataFrame([{
            'Metric': 'Total Answers',
            'Value': stats.get('total_answers', 0)
        }])
        
        if stats.get('average') is not None:
            stats_df = pd.concat([stats_df, pd.DataFrame([{
                'Metric': 'Average',
                'Value': f"{stats['average']:.2f}"
            }])])
        
        if stats.get('median') is not None:
            stats_df = pd.concat([stats_df, pd.DataFrame([{
                'Metric': 'Median',
                'Value': f"{stats['median']:.2f}"
            }])])
        
        if stats.get('stddev') is not None:
            stats_df = pd.concat([stats_df, pd.DataFrame([{
                'Metric': 'Std Dev',
                'Value': f"{stats['stddev']:.2f}"
            }])])
        
        if stats.get('min') is not None:
            stats_df = pd.concat([stats_df, pd.DataFrame([{
                'Metric': 'Minimum',
                'Value': stats['min']
            }])])
        
        if stats.get('max') is not None:
            stats_df = pd.concat([stats_df, pd.DataFrame([{
                'Metric': 'Maximum',
                'Value': stats['max']
            }])])
        
        if stats.get('unique_responses') is not None:
            stats_df = pd.concat([stats_df, pd.DataFrame([{
                'Metric': 'Unique Responses',
                'Value': stats['unique_responses']
            }])])
        
        if stats.get('avg_length') is not None:
            stats_df = pd.concat([stats_df, pd.DataFrame([{
                'Metric': 'Avg Response Length',
                'Value': f"{stats['avg_length']:.1f} chars"
            }])])
        
        st.dataframe(stats_df, use_container_width=True, hide_index=True)
        
        # Show raw answers for this question
        st.divider()
        st.subheader("Raw Answers")
        
        answers = self.get_answers(question_id=question_id, limit=100)
        
        if answers:
            ans_df = pd.DataFrame([{
                'Answer ID': a['answer_id'],
                'Account': a.get('account_username', 'Unknown'),
                'Answer': a.get('answer_text', ''),
                'Numeric': a.get('answer_value_numeric', ''),
                'Boolean': a.get('answer_value_boolean', ''),
                'Submitted': a.get('submitted_at'),
                'Batch': (a.get('submission_batch_id') or '')[:20] + '...' if a.get('submission_batch_id') else ''
            } for a in answers])
            
            st.dataframe(ans_df, use_container_width=True, hide_index=True)

    def _render_survey_summary(self):
        """Render summary of all surveys."""
        st.subheader("📈 Survey Summary")
        
        # Load summary data
        summary_df = self.get_survey_summary()
        
        if summary_df.empty:
            st.info("No survey data available yet.")
            return
        
        # Overall metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Surveys", len(summary_df))
        
        with col2:
            st.metric("Total Questions", summary_df['total_questions'].sum())
        
        with col3:
            st.metric("Total Answers", summary_df['total_answers'].sum())
        
        with col4:
            st.metric("Unique Respondents", summary_df['unique_respondents'].sum())
        
        st.divider()
        
        # Per-survey breakdown
        st.subheader("Breakdown by Survey Site")
        
        # Format for display
        display_df = summary_df.copy()
        display_df['first_response'] = pd.to_datetime(display_df['first_response']).dt.strftime('%Y-%m-%d')
        display_df['latest_response'] = pd.to_datetime(display_df['latest_response']).dt.strftime('%Y-%m-%d')
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        st.divider()
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Questions per survey
            fig = px.bar(
                summary_df,
                x='survey_site',
                y='total_questions',
                title="Questions per Survey Site"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Answers per survey
            fig = px.bar(
                summary_df,
                x='survey_site',
                y='total_answers',
                title="Answers per Survey Site"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.divider()
        
        # Select a survey to view details
        st.subheader("Survey Details")
        
        selected_site = st.selectbox(
            "Select Survey Site:",
            options=summary_df['survey_site'].tolist()
        )
        
        if selected_site:
            site_id = summary_df[summary_df['survey_site'] == selected_site]['site_id'].iloc[0]
            
            # Get questions for this survey
            try:
                with get_postgres_connection() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                        cursor.execute("""
                            SELECT 
                                q.question_id,
                                q.question_text,
                                q.question_type,
                                COUNT(a.answer_id) as answer_count
                            FROM questions q
                            LEFT JOIN answers a ON q.question_id = a.question_id
                            WHERE q.survey_site_id = %s
                            GROUP BY q.question_id
                            ORDER BY answer_count DESC
                        """, (site_id,))
                        
                        questions = cursor.fetchall()
                        
                        if questions:
                            q_df = pd.DataFrame([{
                                'Question ID': q['question_id'],
                                'Question': q['question_text'][:100] + '...' if len(q['question_text']) > 100 else q['question_text'],
                                'Type': q['question_type'],
                                'Answers': q['answer_count']
                            } for q in questions])
                            
                            st.dataframe(q_df, use_container_width=True, hide_index=True)
                        else:
                            st.info("No questions found for this survey site.")
                            
            except Exception as e:
                st.error(f"Error loading questions: {e}")

    def _render_bulk_actions(self, accounts: List[Dict[str, Any]]):
        """Render bulk actions for answers."""
        st.subheader("⚙️ Bulk Actions")
        
        st.warning("⚠️ **DANGER ZONE**: These actions affect multiple answers at once.")
        
        # Delete by account
        with st.expander("🗑️ Delete Answers by Account", expanded=False):
            st.write("Delete ALL answers for a specific account.")
            
            account_options = [f"{a['username']} (ID: {a['account_id']})" for a in accounts]
            selected_account = st.selectbox("Select Account:", account_options, key="bulk_del_account")
            
            if selected_account:
                account_id = int(selected_account.split("ID: ")[1].rstrip(")"))
                
                # Show preview
                summary = self.get_account_response_summary(account_id)
                
                if summary:
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Questions Answered", summary.get('questions_answered', 0))
                    col2.metric("Total Responses", summary.get('total_responses', 0))
                    col3.metric("Sites", summary.get('sites_participated', 0))
                
                if st.button("🗑️ Delete ALL Answers", key="confirm_bulk_del_account", type="primary"):
                    result = self.delete_answers_by_account(account_id)
                    if result['success']:
                        st.success(f"✅ Deleted {result['deleted_count']} answers!")
                        st.rerun()
                    else:
                        st.error(f"❌ Failed: {result.get('error')}")
        
        # Delete by batch
        with st.expander("🗑️ Delete Answers by Batch", expanded=False):
            st.write("Delete ALL answers from a specific submission batch.")
            
            # Get recent batches
            try:
                with get_postgres_connection() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                        cursor.execute("""
                            SELECT 
                                submission_batch_id,
                                COUNT(*) as answer_count,
                                MIN(submitted_at) as first_submission,
                                MAX(submitted_at) as last_submission
                            FROM answers
                            WHERE submission_batch_id IS NOT NULL
                            GROUP BY submission_batch_id
                            ORDER BY last_submission DESC
                            LIMIT 50
                        """)
                        batches = cursor.fetchall()
            except Exception as e:
                st.error(f"Error loading batches: {e}")
                batches = []
            
            if batches:
                batch_options = {
                    f"{b['submission_batch_id'][:30]}... ({b['answer_count']} answers, {b['last_submission'].strftime('%Y-%m-%d')})": b['submission_batch_id']
                    for b in batches
                }
                
                selected_batch = st.selectbox("Select Batch:", options=list(batch_options.keys()))
                
                if selected_batch:
                    batch_id = batch_options[selected_batch]
                    
                    if st.button("🗑️ Delete Batch Answers", key="confirm_bulk_del_batch", type="primary"):
                        result = self.delete_answers_by_batch(batch_id)
                        if result['success']:
                            st.success(f"✅ Deleted {result['deleted_count']} answers!")
                            st.rerun()
                        else:
                            st.error(f"❌ Failed: {result.get('error')}")
            else:
                st.info("No submission batches found.")
        
        # Export data
        with st.expander("📥 Export All Answers", expanded=False):
            st.write("Export all answers to CSV.")
            
            col1, col2 = st.columns(2)
            
            with col1:
                export_limit = st.number_input("Max rows to export:", min_value=100, max_value=10000, value=1000, step=100)
            
            with col2:
                if st.button("📥 Generate Export", use_container_width=True):
                    with st.spinner("Loading answers..."):
                        answers = self.get_answers(limit=export_limit)
                        
                        if answers:
                            df = pd.DataFrame([{
                                'answer_id': a['answer_id'],
                                'question_id': a['question_id'],
                                'question_text': a.get('question_text', ''),
                                'question_type': a.get('question_type', ''),
                                'answer_text': a.get('answer_text', ''),
                                'answer_numeric': a.get('answer_value_numeric', ''),
                                'answer_boolean': a.get('answer_value_boolean', ''),
                                'account_id': a.get('account_id', ''),
                                'account_username': a.get('account_username', ''),
                                'survey_site': a.get('survey_site_country', ''),
                                'submitted_at': a.get('submitted_at', ''),
                                'batch_id': a.get('submission_batch_id', '')
                            } for a in answers])
                            
                            csv = df.to_csv(index=False)
                            st.download_button(
                                label="📥 Download Export",
                                data=csv,
                                file_name=f"answers_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                            
                            st.success(f"✅ Loaded {len(answers)} answers ready for export!")
                        else:
                            st.warning("No answers found.")