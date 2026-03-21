# File: src/streamlit/ui/pages/accounts/answers_page.py
# Answers management page — updated for survey_name, workflow_id in schema

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
    """Answers management page — supports survey_name filtering and workflow_id tracking."""

    def __init__(self, db_manager):
        self.db_manager = db_manager

    # =========================================================================
    # QUERY METHODS
    # =========================================================================

    def get_answers(
        self,
        question_id:    Optional[int]      = None,
        account_id:     Optional[int]      = None,
        survey_site_id: Optional[int]      = None,
        survey_name:    Optional[str]      = None,
        start_date:     Optional[datetime] = None,
        end_date:       Optional[datetime] = None,
        batch_id:       Optional[str]      = None,
        workflow_id:    Optional[int]      = None,
        limit:          int                = 1000,
    ) -> List[Dict[str, Any]]:
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
                            a.workflow_id,
                            q.question_text,
                            q.question_type,
                            q.question_category,
                            q.survey_name,
                            q.survey_complete,
                            q.options,
                            q.survey_site_id,
                            ss.site_name  AS survey_site_name,
                            acc.username  AS account_username,
                            w.workflow_name
                        FROM answers a
                        LEFT JOIN questions    q   ON a.question_id = q.question_id
                        LEFT JOIN survey_sites ss  ON q.survey_site_id = ss.site_id
                        LEFT JOIN accounts     acc ON a.account_id     = acc.account_id
                        LEFT JOIN workflows    w   ON a.workflow_id    = w.workflow_id
                        WHERE 1=1
                    """
                    params = []

                    if question_id:    query += " AND a.question_id = %s";           params.append(question_id)
                    if account_id:     query += " AND a.account_id = %s";            params.append(account_id)
                    if survey_site_id: query += " AND q.survey_site_id = %s";        params.append(survey_site_id)
                    if survey_name:    query += " AND q.survey_name = %s";           params.append(survey_name)
                    if start_date:     query += " AND a.submitted_at >= %s";         params.append(start_date)
                    if end_date:       query += " AND a.submitted_at <= %s";         params.append(end_date)
                    if batch_id:       query += " AND a.submission_batch_id = %s";   params.append(batch_id)
                    if workflow_id:    query += " AND a.workflow_id = %s";           params.append(workflow_id)

                    query += " ORDER BY a.submitted_at DESC LIMIT %s"
                    params.append(limit)

                    cursor.execute(query, params)
                    rows = []
                    for row in cursor.fetchall():
                        row = dict(row)
                        if isinstance(row.get('metadata'), str): row['metadata'] = json.loads(row['metadata'])
                        if isinstance(row.get('options'),  str): row['options']  = json.loads(row['options'])
                        rows.append(row)
                    return rows
        except Exception as e:
            logger.error(f"get_answers: {e}")
            st.error(f"Database error: {e}")
            return []

    def get_distinct_survey_names(self) -> List[str]:
        """All distinct survey_name values from questions that have answers."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT DISTINCT q.survey_name
                        FROM answers a
                        JOIN questions q ON a.question_id = q.question_id
                        WHERE q.survey_name IS NOT NULL
                        ORDER BY q.survey_name
                    """)
                    return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"get_distinct_survey_names: {e}")
            return []

    def get_answer_statistics(self, question_id: int) -> Dict[str, Any]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(
                        "SELECT question_text, question_type, options, survey_name FROM questions WHERE question_id = %s",
                        (question_id,),
                    )
                    question = cursor.fetchone()
                    if not question:
                        return {}

                    stats = {
                        'question_id':   question_id,
                        'question_text': question['question_text'],
                        'question_type': question['question_type'],
                        'survey_name':   question.get('survey_name'),
                        'total_answers': 0,
                        'breakdown':     {},
                        'time_series':   [],
                    }

                    if question['question_type'] in ('multiple_choice','dropdown','checkbox','radio'):
                        cursor.execute(
                            "SELECT answer_text, COUNT(*) AS count FROM answers WHERE question_id=%s GROUP BY answer_text ORDER BY count DESC",
                            (question_id,),
                        )
                        for row in cursor.fetchall():
                            stats['breakdown'][row['answer_text']] = row['count']
                            stats['total_answers'] += row['count']

                    elif question['question_type'] == 'rating':
                        cursor.execute("""
                            SELECT COUNT(*) AS total,
                                   AVG(answer_value_numeric)    AS avg,
                                   STDDEV(answer_value_numeric) AS stddev,
                                   MIN(answer_value_numeric)    AS min_val,
                                   MAX(answer_value_numeric)    AS max_val,
                                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY answer_value_numeric) AS median,
                                   PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY answer_value_numeric) AS q1,
                                   PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY answer_value_numeric) AS q3
                            FROM answers WHERE question_id=%s AND answer_value_numeric IS NOT NULL
                        """, (question_id,))
                        agg = cursor.fetchone()
                        if agg and agg['total'] > 0:
                            stats.update({
                                'total_answers': agg['total'],
                                'average': float(agg['avg'])    if agg['avg']    else None,
                                'stddev':  float(agg['stddev']) if agg['stddev'] else None,
                                'min':     float(agg['min_val'])if agg['min_val']else None,
                                'max':     float(agg['max_val'])if agg['max_val']else None,
                                'median':  float(agg['median']) if agg['median'] else None,
                                'q1':      float(agg['q1'])     if agg['q1']     else None,
                                'q3':      float(agg['q3'])     if agg['q3']     else None,
                            })

                    elif question['question_type'] == 'yes_no':
                        cursor.execute(
                            "SELECT answer_value_boolean, COUNT(*) AS count FROM answers WHERE question_id=%s GROUP BY answer_value_boolean",
                            (question_id,),
                        )
                        for row in cursor.fetchall():
                            key = 'Yes' if row['answer_value_boolean'] else 'No'
                            stats['breakdown'][key] = row['count']
                            stats['total_answers'] += row['count']

                    else:
                        cursor.execute("""
                            SELECT COUNT(*) AS total,
                                   COUNT(DISTINCT answer_text) AS unique_responses,
                                   AVG(LENGTH(answer_text))    AS avg_length
                            FROM answers WHERE question_id=%s
                        """, (question_id,))
                        agg = cursor.fetchone()
                        stats['total_answers']    = agg['total']           if agg else 0
                        stats['unique_responses'] = agg['unique_responses']if agg else 0
                        stats['avg_length']       = float(agg['avg_length']) if agg and agg['avg_length'] else 0

                    # Time series
                    cursor.execute("""
                        SELECT DATE(submitted_at) AS date, COUNT(*) AS count
                        FROM answers WHERE question_id=%s
                        GROUP BY DATE(submitted_at) ORDER BY date
                    """, (question_id,))
                    stats['time_series'] = [dict(row) for row in cursor.fetchall()]

                    return stats
        except Exception as e:
            logger.error(f"get_answer_statistics: {e}")
            return {}

    def get_survey_summary(self, survey_site_id: Optional[int] = None) -> pd.DataFrame:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT
                            ss.site_id,
                            ss.site_name             AS survey_site,
                            q.survey_name,
                            COUNT(DISTINCT q.question_id)      AS total_questions,
                            COUNT(DISTINCT a.answer_id)        AS total_answers,
                            COUNT(DISTINCT a.account_id)       AS unique_respondents,
                            MIN(a.submitted_at)                AS first_response,
                            MAX(a.submitted_at)                AS latest_response,
                            COUNT(DISTINCT a.submission_batch_id) AS submission_batches
                        FROM survey_sites ss
                        LEFT JOIN questions q ON ss.site_id = q.survey_site_id
                        LEFT JOIN answers   a ON q.question_id = a.question_id
                    """
                    params = []
                    if survey_site_id:
                        query += " WHERE ss.site_id = %s"; params.append(survey_site_id)
                    query += " GROUP BY ss.site_id, ss.site_name, q.survey_name ORDER BY ss.site_name, q.survey_name"

                    cursor.execute(query, params)
                    return pd.DataFrame([dict(row) for row in cursor.fetchall()])
        except Exception as e:
            logger.error(f"get_survey_summary: {e}")
            return pd.DataFrame()

    def get_account_response_summary(self, account_id: int) -> Dict[str, Any]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            COUNT(DISTINCT a.question_id)          AS questions_answered,
                            COUNT(a.answer_id)                     AS total_responses,
                            COUNT(DISTINCT q.survey_site_id)       AS sites_participated,
                            COUNT(DISTINCT q.survey_name)          AS surveys_answered,
                            MIN(a.submitted_at)                    AS first_response,
                            MAX(a.submitted_at)                    AS latest_response,
                            COUNT(DISTINCT a.submission_batch_id)  AS batches
                        FROM answers a
                        LEFT JOIN questions q ON a.question_id = q.question_id
                        WHERE a.account_id = %s
                    """, (account_id,))
                    return dict(cursor.fetchone() or {})
        except Exception as e:
            logger.error(f"get_account_response_summary: {e}")
            return {}

    # =========================================================================
    # DELETION
    # =========================================================================

    def delete_answer(self, answer_id: int) -> Dict[str, Any]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM answers WHERE answer_id = %s", (answer_id,))
                    conn.commit()
                    return {'success': cursor.rowcount > 0, 'error': 'Not found' if cursor.rowcount == 0 else None}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def delete_answers_by_survey(self, survey_name: str) -> Dict[str, Any]:
        """Delete all answers for a specific survey name."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        DELETE FROM answers
                        WHERE question_id IN (
                            SELECT question_id FROM questions WHERE survey_name = %s
                        )
                    """, (survey_name,))
                    conn.commit()
                    return {'success': True, 'deleted_count': cursor.rowcount}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def delete_answers_by_question(self, question_id: int) -> Dict[str, Any]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM answers WHERE question_id = %s", (question_id,))
                    conn.commit()
                    return {'success': True, 'deleted_count': cursor.rowcount}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def delete_answers_by_account(self, account_id: int) -> Dict[str, Any]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM answers WHERE account_id = %s", (account_id,))
                    conn.commit()
                    return {'success': True, 'deleted_count': cursor.rowcount}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def delete_answers_by_batch(self, batch_id: str) -> Dict[str, Any]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM answers WHERE submission_batch_id = %s", (batch_id,))
                    conn.commit()
                    return {'success': True, 'deleted_count': cursor.rowcount}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    # =========================================================================
    # RENDER
    # =========================================================================

    def render(self):
        st.header("📝 Survey Answers")
        accounts = self._load_accounts()

        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📋 All Answers",
            "📊 Question Analytics",
            "📈 Survey Summary",
            "🗂️ By Survey Name",
            "⚙️ Bulk Actions",
        ])
        with tab1: self._render_answers_list(accounts)
        with tab2: self._render_question_analytics()
        with tab3: self._render_survey_summary()
        with tab4: self._render_by_survey_name()
        with tab5: self._render_bulk_actions(accounts)

    def _load_accounts(self) -> List[Dict[str, Any]]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(
                        "SELECT account_id, username, country FROM accounts WHERE is_active=TRUE OR is_active IS NULL ORDER BY username"
                    )
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"_load_accounts: {e}")
            return []

    # ------------------------------------------------------------------
    # Tab 1 — All answers
    # ------------------------------------------------------------------

    def _render_answers_list(self, accounts):
        st.subheader("📋 All Answers")

        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            account_options = ["All Accounts"] + [f"{a['username']} (ID: {a['account_id']})" for a in accounts]
            selected_account = st.selectbox("Account:", account_options, key="ans_filter_account")

        with col2:
            survey_names = self.get_distinct_survey_names()
            selected_survey = st.selectbox("Survey:", ["All"] + survey_names, key="ans_filter_survey")

        with col3:
            date_range = st.date_input(
                "Date Range:",
                value=(datetime.now() - timedelta(days=30), datetime.now()),
                key="ans_filter_date",
            )

        with col4:
            batch_id = st.text_input("Batch ID:", placeholder="Filter by batch…")

        with col5:
            limit = st.number_input("Max Results:", min_value=10, max_value=5000, value=500, step=100)

        account_id = None
        if selected_account != "All Accounts":
            account_id = int(selected_account.split("ID: ")[1].rstrip(")"))

        start_date = end_date = None
        if len(date_range) == 2:
            start_date = datetime.combine(date_range[0], datetime.min.time())
            end_date   = datetime.combine(date_range[1], datetime.max.time())

        answers = self.get_answers(
            account_id  = account_id,
            survey_name = selected_survey if selected_survey != "All" else None,
            start_date  = start_date,
            end_date    = end_date,
            batch_id    = batch_id if batch_id else None,
            limit       = int(limit),
        )

        if not answers:
            st.info("No answers found matching the filters.")
            return

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Answers",       len(answers))
        c2.metric("Unique Questions",    len(set(a['question_id'] for a in answers if a['question_id'])))
        c3.metric("Unique Surveys",      len(set(a['survey_name'] for a in answers if a.get('survey_name'))))
        c4.metric("Submission Batches",  len(set(a['submission_batch_id'] for a in answers if a['submission_batch_id'])))

        st.divider()

        df = pd.DataFrame([{
            'Answer ID':    a['answer_id'],
            'Survey':       a.get('survey_name', '—'),
            'Question':     (a.get('question_text') or '')[:100],
            'Answer':       a.get('answer_text', ''),
            'Type':         a.get('question_type', ''),
            'Account':      a.get('account_username', 'Unknown'),
            'Site':         a.get('survey_site_name', 'Unknown'),
            'Workflow':     a.get('workflow_name', '—'),
            'Submitted':    a['submitted_at'].strftime('%Y-%m-%d %H:%M') if a.get('submitted_at') else '—',
            'Batch':        (a.get('submission_batch_id') or '')[:20],
        } for a in answers])

        st.dataframe(df, use_container_width=True, hide_index=True, height=400)
        st.download_button(
            "📥 Download Answers CSV", data=df.to_csv(index=False),
            file_name=f"answers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )

        st.divider()
        st.subheader("Detailed View (first 20)")
        for answer in answers[:20]:
            with st.expander(
                f"Answer #{answer['answer_id']} — "
                f"[{answer.get('survey_name','?')}] "
                f"{(answer.get('question_text') or '')[:80]}…",
                expanded=False,
            ):
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Answer ID:** {answer['answer_id']}")
                    st.write(f"**Survey:** {answer.get('survey_name', '—')}")
                    st.write(f"**Question:** {answer.get('question_text','N/A')}")
                    st.write(f"**Type:** {answer.get('question_type','N/A')}")
                    if answer.get('answer_text'):         st.write(f"**Answer:** {answer['answer_text']}")
                    if answer.get('answer_value_numeric') is not None: st.write(f"**Numeric:** {answer['answer_value_numeric']}")
                    if answer.get('answer_value_boolean') is not None: st.write(f"**Boolean:** {'Yes' if answer['answer_value_boolean'] else 'No'}")
                with col2:
                    st.write(f"**Account:** {answer.get('account_username','Unknown')}")
                    st.write(f"**Site:** {answer.get('survey_site_name','Unknown')}")
                    st.write(f"**Workflow:** {answer.get('workflow_name','—')}")
                    st.write(f"**Submitted:** {answer.get('submitted_at')}")
                    st.write(f"**Batch:** {answer.get('submission_batch_id','N/A')}")

                if st.button("🗑️ Delete", key=f"del_ans_{answer['answer_id']}"):
                    st.session_state[f'confirm_del_{answer["answer_id"]}'] = True

                if st.session_state.get(f'confirm_del_{answer["answer_id"]}', False):
                    c_y, c_n = st.columns(2)
                    with c_y:
                        if st.button("✅ Yes", key=f"yes_{answer['answer_id']}"):
                            r = self.delete_answer(answer['answer_id'])
                            if r['success']:
                                st.success("Deleted!"); del st.session_state[f'confirm_del_{answer["answer_id"]}']
                                st.rerun()
                            else:
                                st.error(r.get('error'))
                    with c_n:
                        if st.button("❌ No", key=f"no_{answer['answer_id']}"):
                            del st.session_state[f'confirm_del_{answer["answer_id"]}']; st.rerun()

    # ------------------------------------------------------------------
    # Tab 2 — Question analytics
    # ------------------------------------------------------------------

    def _render_question_analytics(self):
        st.subheader("📊 Question Analytics")

        survey_names = self.get_distinct_survey_names()
        survey_filter = st.selectbox(
            "Filter by survey:", ["All"] + survey_names, key="qa_survey_filter"
        )

        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT
                            q.question_id,
                            q.question_text,
                            q.question_type,
                            q.survey_name,
                            ss.site_name AS survey_site,
                            COUNT(a.answer_id) AS answer_count
                        FROM questions q
                        LEFT JOIN survey_sites ss ON q.survey_site_id = ss.site_id
                        LEFT JOIN answers       a  ON q.question_id   = a.question_id
                        WHERE 1=1
                    """
                    params = []
                    if survey_filter != "All":
                        query += " AND q.survey_name = %s"; params.append(survey_filter)
                    query += " GROUP BY q.question_id, q.question_text, q.question_type, q.survey_name, ss.site_name ORDER BY answer_count DESC LIMIT 100"
                    cursor.execute(query, params)
                    questions = cursor.fetchall()
        except Exception as e:
            st.error(f"Error: {e}"); return

        if not questions:
            st.info("No questions found."); return

        question_options = {
            f"[{q['survey_name'] or '?'}] {q['question_text'][:100]}… ({q['answer_count']} answers)": q['question_id']
            for q in questions
        }
        selected_question = st.selectbox("Select Question:", list(question_options.keys()))
        if not selected_question: return

        question_id = question_options[selected_question]
        stats = self.get_answer_statistics(question_id)
        if not stats:
            st.warning("No statistics available."); return

        st.divider()
        st.markdown(f"### {stats.get('question_text','?')}")
        st.markdown(
            f"**Type:** `{stats.get('question_type')}` | "
            f"**Survey:** `{stats.get('survey_name') or '—'}`  |  "
            f"**Total Answers:** {stats.get('total_answers', 0)}"
        )
        st.divider()

        col1, col2 = st.columns(2)
        with col1:
            if stats['question_type'] in ('multiple_choice','dropdown','checkbox','radio') and stats.get('breakdown'):
                fig = px.pie(values=list(stats['breakdown'].values()), names=list(stats['breakdown'].keys()), title="Response Distribution")
                st.plotly_chart(fig, use_container_width=True)
            elif stats['question_type'] == 'rating' and stats.get('q1'):
                fig = go.Figure()
                fig.add_trace(go.Box(
                    q1=[stats['q1']], median=[stats['median']], q3=[stats['q3']],
                    lowerfence=[stats['min']], upperfence=[stats['max']],
                    mean=[stats['average']], name="Rating Distribution",
                ))
                st.plotly_chart(fig, use_container_width=True)
            elif stats['question_type'] == 'yes_no' and stats.get('breakdown'):
                fig = px.bar(x=list(stats['breakdown'].keys()), y=list(stats['breakdown'].values()), title="Yes/No")
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            if stats.get('time_series'):
                ts_df = pd.DataFrame(stats['time_series'])
                fig = px.line(ts_df, x='date', y='count', title="Responses Over Time")
                st.plotly_chart(fig, use_container_width=True)

        # Raw answers
        st.divider()
        answers = self.get_answers(question_id=question_id, limit=100)
        if answers:
            ans_df = pd.DataFrame([{
                'Answer ID': a['answer_id'],
                'Account':   a.get('account_username','Unknown'),
                'Answer':    a.get('answer_text',''),
                'Numeric':   a.get('answer_value_numeric',''),
                'Submitted': a.get('submitted_at'),
                'Workflow':  a.get('workflow_name','—'),
                'Batch':     (a.get('submission_batch_id') or '')[:20],
            } for a in answers])
            st.dataframe(ans_df, use_container_width=True, hide_index=True)

    # ------------------------------------------------------------------
    # Tab 3 — Survey summary
    # ------------------------------------------------------------------

    def _render_survey_summary(self):
        st.subheader("📈 Survey Summary")

        summary_df = self.get_survey_summary()
        if summary_df.empty:
            st.info("No survey data available yet."); return

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Surveys",      len(summary_df))
        c2.metric("Total Questions",    summary_df['total_questions'].sum())
        c3.metric("Total Answers",      summary_df['total_answers'].sum())
        c4.metric("Unique Respondents", summary_df['unique_respondents'].sum())

        st.divider()
        display_df = summary_df.copy()
        for col in ['first_response','latest_response']:
            if col in display_df.columns:
                display_df[col] = pd.to_datetime(display_df[col]).dt.strftime('%Y-%m-%d').fillna('—')
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(summary_df, x='survey_name', y='total_questions', title="Questions per Survey")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(summary_df, x='survey_name', y='total_answers', title="Answers per Survey")
            st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Tab 4 — By Survey Name
    # ------------------------------------------------------------------

    def _render_by_survey_name(self):
        st.subheader("🗂️ Answers by Survey Name")
        st.caption("Drill into a specific survey to see all answers and manage them.")

        survey_names = self.get_distinct_survey_names()
        if not survey_names:
            st.info("No survey names with answers yet."); return

        selected = st.selectbox("Select Survey:", survey_names, key="by_survey_name_select")
        if not selected: return

        answers = self.get_answers(survey_name=selected, limit=500)
        if not answers:
            st.info(f"No answers found for survey **{selected}**."); return

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Answers",   len(answers))
        c2.metric("Questions",       len(set(a['question_id'] for a in answers)))
        c3.metric("Accounts",        len(set(a['account_id']  for a in answers if a.get('account_id'))))

        df = pd.DataFrame([{
            'Answer ID': a['answer_id'],
            'Question':  (a.get('question_text') or '')[:80],
            'Type':      a.get('question_type', ''),
            'Answer':    a.get('answer_text', ''),
            'Account':   a.get('account_username','Unknown'),
            'Workflow':  a.get('workflow_name','—'),
            'Submitted': a['submitted_at'].strftime('%Y-%m-%d %H:%M') if a.get('submitted_at') else '—',
            'Batch':     (a.get('submission_batch_id') or '')[:20],
        } for a in answers])

        st.dataframe(df, use_container_width=True, hide_index=True, height=400)
        st.download_button(
            f"📥 Download '{selected}' answers CSV",
            data=df.to_csv(index=False),
            file_name=f"answers_{selected.replace(' ','_')}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

        st.divider()
        st.warning(f"⚠️ Delete all answers for **{selected}**?")
        if st.button(f"🗑️ Delete all answers for '{selected}'", key="del_survey_answers"):
            result = self.delete_answers_by_survey(selected)
            if result['success']:
                st.success(f"✅ Deleted {result['deleted_count']} answers."); st.rerun()
            else:
                st.error(result.get('error'))

    # ------------------------------------------------------------------
    # Tab 5 — Bulk actions
    # ------------------------------------------------------------------

    def _render_bulk_actions(self, accounts):
        st.subheader("⚙️ Bulk Actions")
        st.warning("⚠️ **DANGER ZONE** — these actions affect many rows at once.")

        with st.expander("🗑️ Delete by Account", expanded=False):
            account_options = [f"{a['username']} (ID: {a['account_id']})" for a in accounts]
            sel_acc = st.selectbox("Account:", account_options, key="bulk_del_account")
            if sel_acc:
                account_id = int(sel_acc.split("ID: ")[1].rstrip(")"))
                summary = self.get_account_response_summary(account_id)
                if summary:
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Questions Answered", summary.get('questions_answered', 0))
                    c2.metric("Total Responses",    summary.get('total_responses', 0))
                    c3.metric("Surveys",            summary.get('surveys_answered', 0))
                    c4.metric("Sites",              summary.get('sites_participated', 0))
                if st.button("🗑️ Delete ALL Answers for this account", key="bulk_del_acc_btn", type="primary"):
                    r = self.delete_answers_by_account(account_id)
                    if r['success']:
                        st.success(f"✅ Deleted {r['deleted_count']} answers!"); st.rerun()
                    else:
                        st.error(r.get('error'))

        with st.expander("🗑️ Delete by Survey Name", expanded=False):
            survey_names = self.get_distinct_survey_names()
            if survey_names:
                sel_survey = st.selectbox("Survey:", survey_names, key="bulk_del_survey")
                if sel_survey:
                    survey_answers = self.get_answers(survey_name=sel_survey, limit=1)
                    st.info(f"This will delete all answers for survey **{sel_survey}**.")
                    if st.button(f"🗑️ Delete all answers for '{sel_survey}'", key="bulk_del_survey_btn", type="primary"):
                        r = self.delete_answers_by_survey(sel_survey)
                        if r['success']:
                            st.success(f"✅ Deleted {r['deleted_count']} answers!"); st.rerun()
                        else:
                            st.error(r.get('error'))
            else:
                st.info("No surveys with answers yet.")

        with st.expander("🗑️ Delete by Batch", expanded=False):
            try:
                with get_postgres_connection() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                        cursor.execute("""
                            SELECT submission_batch_id,
                                   COUNT(*) AS answer_count,
                                   MAX(submitted_at) AS last_submission
                            FROM answers
                            WHERE submission_batch_id IS NOT NULL
                            GROUP BY submission_batch_id
                            ORDER BY last_submission DESC LIMIT 50
                        """)
                        batches = cursor.fetchall()
            except Exception as e:
                st.error(f"Error: {e}"); batches = []

            if batches:
                batch_options = {
                    f"{b['submission_batch_id'][:30]}… ({b['answer_count']} answers, {b['last_submission'].strftime('%Y-%m-%d')})": b['submission_batch_id']
                    for b in batches
                }
                sel_batch = st.selectbox("Batch:", list(batch_options.keys()), key="bulk_del_batch")
                if sel_batch:
                    batch_id = batch_options[sel_batch]
                    if st.button("🗑️ Delete Batch Answers", key="bulk_del_batch_btn", type="primary"):
                        r = self.delete_answers_by_batch(batch_id)
                        if r['success']:
                            st.success(f"✅ Deleted {r['deleted_count']} answers!"); st.rerun()
                        else:
                            st.error(r.get('error'))
            else:
                st.info("No submission batches found.")

        with st.expander("📥 Export All Answers", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                survey_filter = st.selectbox("Filter by survey:", ["All"] + self.get_distinct_survey_names(), key="export_survey")
                export_limit  = st.number_input("Max rows:", min_value=100, max_value=10000, value=1000, step=100)
            with col2:
                if st.button("📥 Generate Export", use_container_width=True):
                    with st.spinner("Loading…"):
                        answers = self.get_answers(
                            survey_name=survey_filter if survey_filter != "All" else None,
                            limit=int(export_limit),
                        )
                        if answers:
                            df = pd.DataFrame([{
                                'answer_id':        a['answer_id'],
                                'question_id':      a['question_id'],
                                'question_text':    a.get('question_text',''),
                                'question_type':    a.get('question_type',''),
                                'survey_name':      a.get('survey_name',''),
                                'answer_text':      a.get('answer_text',''),
                                'answer_numeric':   a.get('answer_value_numeric',''),
                                'answer_boolean':   a.get('answer_value_boolean',''),
                                'account_id':       a.get('account_id',''),
                                'account_username': a.get('account_username',''),
                                'survey_site':      a.get('survey_site_name',''),
                                'workflow_name':    a.get('workflow_name',''),
                                'submitted_at':     a.get('submitted_at',''),
                                'batch_id':         a.get('submission_batch_id',''),
                            } for a in answers])
                            st.download_button(
                                "📥 Download",
                                data=df.to_csv(index=False),
                                file_name=f"answers_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                            )
                            st.success(f"✅ {len(answers)} answers ready.")
                        else:
                            st.warning("No answers found.")