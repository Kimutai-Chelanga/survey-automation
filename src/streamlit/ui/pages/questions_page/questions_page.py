# File: src/streamlit/ui/pages/accounts/questions_page.py
# Questions management page — updated for survey_name, survey_complete tracking

import streamlit as st
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
from src.core.database.postgres.connection import get_postgres_connection
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class QuestionsPage:
    """Questions management page — supports survey_name and survey_complete tracking."""

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self._ensure_new_columns()

    def _ensure_new_columns(self):
        migrations = [
            "ALTER TABLE questions ADD COLUMN IF NOT EXISTS survey_name VARCHAR(255);",
            "ALTER TABLE questions ADD COLUMN IF NOT EXISTS survey_complete BOOLEAN DEFAULT FALSE;",
            "ALTER TABLE questions ADD COLUMN IF NOT EXISTS survey_completed_at TIMESTAMP;",
            "CREATE INDEX IF NOT EXISTS idx_questions_survey_name ON questions(survey_name) WHERE survey_name IS NOT NULL;",
            "CREATE INDEX IF NOT EXISTS idx_questions_survey_complete ON questions(survey_complete) WHERE survey_complete = FALSE;",
        ]
        for sql in migrations:
            try:
                self.db_manager.execute_query(sql)
            except Exception as e:
                logger.warning(f"Migration skipped (likely already applied): {e}")

    # =========================================================================
    # QUERY METHODS
    # =========================================================================

    def get_question(self, question_id: int) -> Optional[Dict[str, Any]]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            q.*,
                            ss.site_name  AS survey_site_name,
                            a.username    AS account_username,
                            COUNT(ans.answer_id) AS answer_count
                        FROM questions q
                        LEFT JOIN survey_sites ss ON q.survey_site_id = ss.site_id
                        LEFT JOIN accounts      a  ON q.account_id     = a.account_id
                        LEFT JOIN answers       ans ON q.question_id   = ans.question_id
                        WHERE q.question_id = %s
                        GROUP BY q.question_id, ss.site_name, a.username
                    """, (question_id,))
                    row = cursor.fetchone()
                    if row:
                        row = dict(row)
                        if isinstance(row.get('options'),  str): row['options']  = json.loads(row['options'])
                        if isinstance(row.get('metadata'), str): row['metadata'] = json.loads(row['metadata'])
                    return row
        except Exception as e:
            logger.error(f"get_question: {e}")
            return None

    def get_questions(
        self,
        survey_site_id:    Optional[int]  = None,
        account_id:        Optional[int]  = None,
        question_type:     Optional[str]  = None,
        question_category: Optional[str]  = None,
        survey_name:       Optional[str]  = None,
        survey_complete:   Optional[bool] = None,
        is_active:         Optional[bool] = True,
        unused_only:       bool           = False,
        limit:             int            = 1000,
        batch_id:          Optional[str]  = None,
    ) -> List[Dict[str, Any]]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT
                            q.*,
                            ss.site_name  AS survey_site_name,
                            a.username    AS account_username,
                            COUNT(ans.answer_id) AS answer_count
                        FROM questions q
                        LEFT JOIN survey_sites ss ON q.survey_site_id = ss.site_id
                        LEFT JOIN accounts      a  ON q.account_id     = a.account_id
                        LEFT JOIN answers       ans ON q.question_id   = ans.question_id
                        WHERE 1=1
                    """
                    params = []

                    if survey_site_id:
                        query += " AND q.survey_site_id = %s";    params.append(survey_site_id)
                    if account_id:
                        query += " AND q.account_id = %s";        params.append(account_id)
                    if question_type:
                        query += " AND q.question_type = %s";     params.append(question_type)
                    if question_category:
                        query += " AND q.question_category = %s"; params.append(question_category)
                    if survey_name:
                        query += " AND q.survey_name = %s";       params.append(survey_name)
                    if survey_complete is not None:
                        query += " AND q.survey_complete = %s";   params.append(survey_complete)
                    if is_active is not None:
                        query += " AND q.is_active = %s";         params.append(is_active)
                    if unused_only:
                        query += " AND (q.used_in_workflow IS NULL OR q.used_in_workflow = FALSE)"
                    if batch_id:
                        query += " AND q.extraction_batch_id = %s"; params.append(batch_id)

                    query += """
                        GROUP BY q.question_id, ss.site_name, a.username
                        ORDER BY q.survey_site_id, q.survey_name, q.question_category,
                                 q.order_index, q.extracted_at DESC
                        LIMIT %s
                    """
                    params.append(limit)
                    cursor.execute(query, params)

                    rows = []
                    for row in cursor.fetchall():
                        row = dict(row)
                        if isinstance(row.get('options'),  str): row['options']  = json.loads(row['options'])
                        if isinstance(row.get('metadata'), str): row['metadata'] = json.loads(row['metadata'])
                        rows.append(row)
                    return rows
        except Exception as e:
            logger.error(f"get_questions: {e}")
            st.error(f"Database error: {e}")
            return []

    def get_distinct_survey_names(
        self,
        account_id:     Optional[int] = None,
        survey_site_id: Optional[int] = None,
    ) -> List[str]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    query = """
                        SELECT DISTINCT survey_name
                        FROM questions
                        WHERE survey_name IS NOT NULL
                    """
                    params = []
                    if account_id:
                        query += " AND account_id = %s";      params.append(account_id)
                    if survey_site_id:
                        query += " AND survey_site_id = %s";  params.append(survey_site_id)
                    query += " ORDER BY survey_name"
                    cursor.execute(query, params)
                    return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"get_distinct_survey_names: {e}")
            return []

    def get_questions_by_survey_site(self, survey_site_id: int) -> List[Dict[str, Any]]:
        return self.get_questions(survey_site_id=survey_site_id)

    def get_questions_by_account(self, account_id: int) -> List[Dict[str, Any]]:
        return self.get_questions(account_id=account_id)

    def get_unused_questions(
        self,
        account_id: Optional[int] = None,
        site_id:    Optional[int] = None,
        survey_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return self.get_questions(
            account_id=account_id, survey_site_id=site_id,
            survey_name=survey_name, unused_only=True,
        )

    def get_recent_extractions(self, limit: int = 10) -> List[Dict[str, Any]]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            extraction_batch_id,
                            COUNT(*)                        AS question_count,
                            COUNT(DISTINCT survey_name)     AS survey_count,
                            MIN(extracted_at)               AS first_extracted,
                            MAX(extracted_at)               AS last_extracted,
                            COUNT(DISTINCT survey_site_id)  AS site_count,
                            COUNT(DISTINCT account_id)      AS account_count,
                            COUNT(DISTINCT question_type)   AS type_count,
                            COUNT(DISTINCT question_category) AS category_count
                        FROM questions
                        WHERE extraction_batch_id IS NOT NULL
                        GROUP BY extraction_batch_id
                        ORDER BY last_extracted DESC
                        LIMIT %s
                    """, (limit,))
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"get_recent_extractions: {e}")
            return []

    def get_survey_counts(
        self,
        survey_site_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Returns per-survey question counts, answer counts, and completion status.
        Used to populate the survey summary metrics at the top of the questions list.
        """
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT
                            q.survey_name,
                            ss.site_name,
                            COUNT(DISTINCT q.question_id)                               AS total_questions,
                            COUNT(DISTINCT CASE WHEN q.is_active THEN q.question_id END) AS active_questions,
                            COUNT(DISTINCT CASE WHEN q.used_in_workflow THEN q.question_id END) AS used_questions,
                            COUNT(DISTINCT ans.answer_id)                               AS answers_generated,
                            BOOL_AND(q.survey_complete)                                  AS all_complete,
                            MAX(q.survey_completed_at)                                   AS completed_at
                        FROM questions q
                        LEFT JOIN survey_sites ss ON q.survey_site_id = ss.site_id
                        LEFT JOIN answers       ans ON q.question_id  = ans.question_id
                        WHERE q.survey_name IS NOT NULL
                    """
                    params = []
                    if survey_site_id:
                        query += " AND q.survey_site_id = %s"; params.append(survey_site_id)
                    query += " GROUP BY q.survey_name, ss.site_name ORDER BY ss.site_name, q.survey_name"
                    cursor.execute(query, params)
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"get_survey_counts: {e}")
            return []

    # =========================================================================
    # UPDATE METHODS
    # =========================================================================

    def _simple_update(self, question_id: int, column: str, value) -> Dict[str, Any]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"UPDATE questions SET {column} = %s WHERE question_id = %s",
                        (value, question_id),
                    )
                    conn.commit()
                    return {'success': cursor.rowcount > 0, 'error': 'Not found' if cursor.rowcount == 0 else None}
        except Exception as e:
            logger.error(f"_simple_update {column}: {e}")
            return {'success': False, 'error': str(e)}

    def update_question_click_element(self, question_id: int, value: str) -> Dict[str, Any]:
        return self._simple_update(question_id, 'click_element', value)

    def update_question_input_element(self, question_id: int, value: str) -> Dict[str, Any]:
        return self._simple_update(question_id, 'input_element', value)

    def update_question_submit_element(self, question_id: int, value: str) -> Dict[str, Any]:
        return self._simple_update(question_id, 'submit_element', value)

    def update_question_category(self, question_id: int, category: str) -> Dict[str, Any]:
        return self._simple_update(question_id, 'question_category', category)

    def update_question_survey_name(self, question_id: int, survey_name: str) -> Dict[str, Any]:
        return self._simple_update(question_id, 'survey_name', survey_name)

    def mark_survey_complete(self, account_id: int, site_id: int, survey_name: str) -> Dict[str, Any]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE questions
                        SET survey_complete = TRUE,
                            survey_completed_at = CURRENT_TIMESTAMP
                        WHERE account_id = %s
                          AND survey_site_id = %s
                          AND survey_name = %s
                    """, (account_id, site_id, survey_name))
                    conn.commit()
                    return {'success': True, 'updated': cursor.rowcount}
        except Exception as e:
            logger.error(f"mark_survey_complete: {e}")
            return {'success': False, 'error': str(e)}

    def mark_question_used(self, question_id: int) -> Dict[str, Any]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE questions
                        SET used_in_workflow = TRUE, used_at = CURRENT_TIMESTAMP
                        WHERE question_id = %s
                    """, (question_id,))
                    conn.commit()
                    return {'success': cursor.rowcount > 0}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def deactivate_question(self, question_id: int) -> Dict[str, Any]:
        return self._simple_update(question_id, 'is_active', False)

    def reactivate_question(self, question_id: int) -> Dict[str, Any]:
        return self._simple_update(question_id, 'is_active', True)

    # =========================================================================
    # ANSWER HELPERS
    # =========================================================================

    def get_answers(
        self,
        question_id: Optional[int] = None,
        account_id:  Optional[int] = None,
        batch_id:    Optional[str] = None,
        limit:       int           = 1000,
    ) -> List[Dict[str, Any]]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT
                            a.*,
                            q.question_text, q.question_type,
                            q.question_category, q.survey_name,
                            q.survey_site_id,
                            ss.site_name AS survey_site_name,
                            acc.username  AS account_username
                        FROM answers a
                        LEFT JOIN questions    q   ON a.question_id = q.question_id
                        LEFT JOIN survey_sites ss  ON q.survey_site_id = ss.site_id
                        LEFT JOIN accounts     acc ON a.account_id     = acc.account_id
                        WHERE 1=1
                    """
                    params = []
                    if question_id: query += " AND a.question_id = %s"; params.append(question_id)
                    if account_id:  query += " AND a.account_id = %s";  params.append(account_id)
                    if batch_id:    query += " AND a.submission_batch_id = %s"; params.append(batch_id)
                    query += " ORDER BY a.submitted_at DESC LIMIT %s"; params.append(limit)

                    cursor.execute(query, params)
                    rows = []
                    for row in cursor.fetchall():
                        row = dict(row)
                        if isinstance(row.get('metadata'), str): row['metadata'] = json.loads(row['metadata'])
                        rows.append(row)
                    return rows
        except Exception as e:
            logger.error(f"get_answers: {e}")
            return []

    def get_answer_statistics(self, question_id: int) -> Dict[str, Any]:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(
                        "SELECT question_type, options, question_category, survey_name FROM questions WHERE question_id = %s",
                        (question_id,),
                    )
                    question = cursor.fetchone()
                    if not question:
                        return {}

                    stats = {
                        'question_id':      question_id,
                        'question_type':    question['question_type'],
                        'question_category':question['question_category'],
                        'survey_name':      question.get('survey_name'),
                        'total_answers':    0,
                        'breakdown':        {},
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
                                   AVG(answer_value_numeric)  AS average,
                                   MIN(answer_value_numeric)  AS min,
                                   MAX(answer_value_numeric)  AS max,
                                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY answer_value_numeric) AS median
                            FROM answers WHERE question_id=%s AND answer_value_numeric IS NOT NULL
                        """, (question_id,))
                        agg = cursor.fetchone()
                        if agg:
                            stats.update({
                                'total_answers': agg['total'],
                                'average':  float(agg['average'])  if agg['average']  else None,
                                'min':      float(agg['min'])       if agg['min']       else None,
                                'max':      float(agg['max'])       if agg['max']       else None,
                                'median':   float(agg['median'])    if agg['median']    else None,
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
                        if agg:
                            stats.update({
                                'total_answers':    agg['total'],
                                'unique_responses': agg['unique_responses'],
                                'avg_length':       float(agg['avg_length']) if agg['avg_length'] else 0,
                            })

                    return stats
        except Exception as e:
            logger.error(f"get_answer_statistics: {e}")
            return {}

    # =========================================================================
    # SUMMARY STATS
    # =========================================================================

    def get_questions_summary(self) -> pd.DataFrame:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            ss.site_id,
                            ss.site_name                                                     AS survey_site,
                            COUNT(DISTINCT q.question_id)                                    AS total_questions,
                            COUNT(DISTINCT CASE WHEN q.is_active        THEN q.question_id END) AS active_questions,
                            COUNT(DISTINCT CASE WHEN q.survey_complete   THEN q.question_id END) AS completed_questions,
                            COUNT(DISTINCT q.survey_name)                                    AS unique_surveys,
                            COUNT(DISTINCT q.account_id)                                     AS unique_accounts,
                            COUNT(a.answer_id)                                               AS total_answers,
                            MIN(q.extracted_at)                                              AS first_question,
                            MAX(q.extracted_at)                                              AS latest_question,
                            COUNT(DISTINCT q.extraction_batch_id)                            AS extraction_batches,
                            COUNT(CASE WHEN q.click_element IS NOT NULL THEN 1 END)          AS questions_with_click_elements
                        FROM survey_sites ss
                        LEFT JOIN questions q ON ss.site_id = q.survey_site_id
                        LEFT JOIN answers   a ON q.question_id = a.question_id
                        GROUP BY ss.site_id, ss.site_name
                        ORDER BY ss.site_name
                    """)
                    return pd.DataFrame([dict(row) for row in cursor.fetchall()])
        except Exception as e:
            logger.error(f"get_questions_summary: {e}")
            return pd.DataFrame()

    def get_survey_name_summary(self) -> pd.DataFrame:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            ss.site_name,
                            q.survey_name,
                            COUNT(DISTINCT q.question_id)                  AS total_questions,
                            COUNT(DISTINCT a.answer_id)                    AS answers_generated,
                            BOOL_AND(q.survey_complete)                    AS all_complete,
                            MAX(q.survey_completed_at)                     AS completed_at,
                            COUNT(DISTINCT q.account_id)                   AS accounts
                        FROM questions q
                        LEFT JOIN survey_sites ss ON q.survey_site_id = ss.site_id
                        LEFT JOIN answers       a  ON q.question_id   = a.question_id
                        WHERE q.survey_name IS NOT NULL
                        GROUP BY ss.site_name, q.survey_name
                        ORDER BY ss.site_name, q.survey_name
                    """)
                    return pd.DataFrame([dict(row) for row in cursor.fetchall()])
        except Exception as e:
            logger.error(f"get_survey_name_summary: {e}")
            return pd.DataFrame()

    def get_question_type_distribution(self) -> pd.DataFrame:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            question_type,
                            COUNT(*)                                                AS count,
                            COUNT(CASE WHEN is_active          THEN 1 END)         AS active_count,
                            COUNT(CASE WHEN click_element IS NOT NULL THEN 1 END)  AS has_click_element
                        FROM questions
                        GROUP BY question_type
                        ORDER BY count DESC
                    """)
                    return pd.DataFrame([dict(row) for row in cursor.fetchall()])
        except Exception as e:
            logger.error(f"get_question_type_distribution: {e}")
            return pd.DataFrame()

    def get_question_category_distribution(self) -> pd.DataFrame:
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            question_category,
                            COUNT(*)                                       AS count,
                            COUNT(CASE WHEN is_active THEN 1 END)         AS active_count
                        FROM questions
                        WHERE question_category IS NOT NULL
                        GROUP BY question_category
                        ORDER BY count DESC
                    """)
                    return pd.DataFrame([dict(row) for row in cursor.fetchall()])
        except Exception as e:
            logger.error(f"get_question_category_distribution: {e}")
            return pd.DataFrame()

    # =========================================================================
    # RENDER
    # =========================================================================

    def render(self):
        st.header("📋 Questions & Answers")

        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📋 All Questions",
            "📝 Answers",
            "📊 Analytics",
            "🗂️ By Survey",
            "🔄 Extraction History",
        ])
        with tab1: self._render_questions_list()
        with tab2: self._render_answers_view()
        with tab3: self._render_analytics()
        with tab4: self._render_by_survey()
        with tab5: self._render_extraction_history()

    # ------------------------------------------------------------------
    # Tab 1 — Questions list
    # ------------------------------------------------------------------

    def _render_questions_list(self):
        st.subheader("📋 Extracted Questions")

        questions = self.get_questions(limit=1000)
        if not questions:
            st.warning("No questions extracted yet. Run the extraction process first.")
            return

        # ── Survey count summary metrics ──────────────────────────────
        survey_counts = self.get_survey_counts()
        total_surveys   = len(survey_counts)
        complete_surveys = sum(1 for s in survey_counts if s.get('all_complete'))
        total_questions  = sum(s['total_questions'] for s in survey_counts)
        total_answers    = sum(s['answers_generated'] for s in survey_counts)

        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("Total Surveys",    total_surveys)
        mc2.metric("✅ Completed",      complete_surveys)
        mc3.metric("Total Questions",  total_questions)
        mc4.metric("Answers Generated",total_answers)

        st.markdown("---")

        # ── Two filters only: Site and Survey Name ────────────────────
        col1, col2 = st.columns(2)

        with col1:
            sites = sorted(set(q.get('survey_site_name', 'Unknown') for q in questions))
            site_filter = st.selectbox("🌐 Site:", ["All"] + sites, key="q_filter_site")

        with col2:
            # Dynamically filter survey names to those belonging to the selected site
            if site_filter != "All":
                survey_names = sorted(set(
                    q.get('survey_name') for q in questions
                    if q.get('survey_name') and q.get('survey_site_name') == site_filter
                ))
            else:
                survey_names = sorted(set(q.get('survey_name') for q in questions if q.get('survey_name')))
            survey_filter = st.selectbox("📋 Survey Name:", ["All"] + survey_names, key="q_filter_survey")

        # ── Apply filters ─────────────────────────────────────────────
        filtered = questions
        if site_filter   != "All": filtered = [q for q in filtered if q.get('survey_site_name') == site_filter]
        if survey_filter != "All": filtered = [q for q in filtered if q.get('survey_name') == survey_filter]

        # Per-survey summary strip when a specific survey is selected
        if survey_filter != "All":
            match = next((s for s in survey_counts if s['survey_name'] == survey_filter), None)
            if match:
                sc1, sc2, sc3, sc4 = st.columns(4)
                sc1.metric("Questions",        match['total_questions'])
                sc2.metric("Active",           match['active_questions'])
                sc3.metric("Used in Workflow", match['used_questions'])
                sc4.metric("Answers",          match['answers_generated'])
                if match.get('all_complete'):
                    completed_at = match.get('completed_at')
                    label = f" — completed {completed_at.strftime('%Y-%m-%d %H:%M')}" if completed_at else ""
                    st.success(f"🏁 Survey marked complete{label}")

        st.caption(f"Showing {len(filtered)} question(s)")

        # ── Question cards ────────────────────────────────────────────
        for q in filtered:
            status_icon   = "✅" if q.get('is_active') else "⭕"
            used_icon     = "📌" if q.get('used_in_workflow') else "🆕"
            complete_icon = "🏁" if q.get('survey_complete') else ""
            survey_label  = f"[{q.get('survey_name', q.get('survey_site_name','?'))}]"

            with st.expander(
                f"{status_icon}{used_icon}{complete_icon} {survey_label} "
                f"{q.get('question_text','')[:100]}…",
                expanded=False,
            ):
                col1, col2 = st.columns([2, 1])

                with col1:
                    st.markdown(f"**Question:** {q.get('question_text')}")
                    st.markdown(
                        f"**Type:** `{q.get('question_type')}` | "
                        f"**Category:** `{q.get('question_category','Uncategorized')}` | "
                        f"**Survey:** `{q.get('survey_name') or '—'}`"
                    )
                    st.markdown(f"**Required:** {'✅ Yes' if q.get('required') else '❌ No'}")

                    if q.get('survey_complete'):
                        completed_at = q.get('survey_completed_at')
                        st.success(
                            f"🏁 Survey complete"
                            + (f" — {completed_at.strftime('%Y-%m-%d %H:%M')}" if completed_at else "")
                        )

                    if q.get('options'):
                        st.markdown("**Options:**")
                        for i, opt in enumerate(q['options'], 1):
                            st.markdown(f"  {i}. {opt}")

                    st.markdown(f"**🖱️ Click Element:** `{q.get('click_element') or 'Not set'}`")
                    if q.get('input_element'):
                        st.markdown(f"**📝 Input Element:** `{q['input_element']}`")
                    if q.get('submit_element'):
                        st.markdown(f"**✅ Submit Element:** `{q['submit_element']}`")
                    if q.get('page_url'):
                        st.markdown(f"**🔗 Page URL:** [{q['page_url'][:60]}…]({q['page_url']})")

                    st.caption(
                        f"Extracted: {q.get('extracted_at')} | "
                        f"Answers: {q.get('answer_count', 0)} | "
                        f"Batch: {(q.get('extraction_batch_id') or '')[:20]}"
                    )

                with col2:
                    st.markdown("**Actions:**")
                    qid = q['question_id']

                    if st.button("📝 View Answers",    key=f"view_ans_{qid}", use_container_width=True):
                        st.session_state[f'viewing_answers_{qid}'] = True; st.rerun()
                    if st.button("🖱️ Edit Selectors",  key=f"edit_click_{qid}", use_container_width=True):
                        st.session_state[f'editing_click_{qid}'] = True; st.rerun()
                    if st.button("📂 Edit Category",   key=f"edit_cat_{qid}", use_container_width=True):
                        st.session_state[f'editing_category_{qid}'] = True; st.rerun()
                    if st.button("🗂️ Edit Survey Name", key=f"edit_survey_{qid}", use_container_width=True):
                        st.session_state[f'editing_survey_{qid}'] = True; st.rerun()

                    if q.get('is_active'):
                        if st.button("⭕ Deactivate", key=f"deact_{qid}", use_container_width=True):
                            self.deactivate_question(qid); st.rerun()
                    else:
                        if st.button("✅ Activate", key=f"act_{qid}", use_container_width=True):
                            self.reactivate_question(qid); st.rerun()

                    if not q.get('used_in_workflow'):
                        if st.button("📌 Mark Used", key=f"mark_used_{qid}", use_container_width=True):
                            self.mark_question_used(qid); st.rerun()

                # ── Edit selectors form ───────────────────────────────
                if st.session_state.get(f'editing_click_{qid}', False):
                    st.divider()
                    with st.form(key=f"click_form_{qid}"):
                        st.markdown("**Edit Selectors**")
                        new_click  = st.text_input("Click Element",  value=q.get('click_element', ''))
                        new_input  = st.text_input("Input Element",  value=q.get('input_element', ''))
                        new_submit = st.text_input("Submit Element", value=q.get('submit_element', ''))
                        c1, c2 = st.columns(2)
                        with c1:
                            if st.form_submit_button("💾 Save", use_container_width=True, type="primary"):
                                if new_click:  self.update_question_click_element(qid, new_click)
                                if new_input:  self.update_question_input_element(qid, new_input)
                                if new_submit: self.update_question_submit_element(qid, new_submit)
                                del st.session_state[f'editing_click_{qid}']; st.rerun()
                        with c2:
                            if st.form_submit_button("Cancel", use_container_width=True):
                                del st.session_state[f'editing_click_{qid}']; st.rerun()

                # ── Edit category form ────────────────────────────────
                if st.session_state.get(f'editing_category_{qid}', False):
                    st.divider()
                    with st.form(key=f"category_form_{qid}"):
                        st.markdown("**Edit Category**")
                        CATEGORIES = [
                            "demographics","opinion","feedback","product","service",
                            "personal","shopping","technology","entertainment","health",
                            "education","employment","income","household","lifestyle",
                            "brands","hobbies","internet","device","screener","other",
                        ]
                        current_idx = (CATEGORIES.index(q.get('question_category')) + 1
                                       if q.get('question_category') in CATEGORIES else 0)
                        new_cat = st.selectbox("Category", [""] + CATEGORIES, index=current_idx)
                        c1, c2 = st.columns(2)
                        with c1:
                            if st.form_submit_button("💾 Save", use_container_width=True, type="primary"):
                                if new_cat: self.update_question_category(qid, new_cat)
                                del st.session_state[f'editing_category_{qid}']; st.rerun()
                        with c2:
                            if st.form_submit_button("Cancel", use_container_width=True):
                                del st.session_state[f'editing_category_{qid}']; st.rerun()

                # ── Edit survey name form ─────────────────────────────
                if st.session_state.get(f'editing_survey_{qid}', False):
                    st.divider()
                    with st.form(key=f"survey_form_{qid}"):
                        st.markdown("**Edit Survey Name**")
                        new_survey = st.text_input("Survey Name", value=q.get('survey_name', ''))
                        c1, c2 = st.columns(2)
                        with c1:
                            if st.form_submit_button("💾 Save", use_container_width=True, type="primary"):
                                if new_survey: self.update_question_survey_name(qid, new_survey)
                                del st.session_state[f'editing_survey_{qid}']; st.rerun()
                        with c2:
                            if st.form_submit_button("Cancel", use_container_width=True):
                                del st.session_state[f'editing_survey_{qid}']; st.rerun()

                # ── Answers inline view ───────────────────────────────
                if st.session_state.get(f'viewing_answers_{qid}', False):
                    st.divider()
                    self._render_answers_for_question(qid)
                    if st.button("Close", key=f"close_ans_{qid}"):
                        del st.session_state[f'viewing_answers_{qid}']; st.rerun()

    def _render_answers_for_question(self, question_id: int):
        answers = self.get_answers(question_id=question_id, limit=100)
        if not answers:
            st.info("No answers yet for this question.")
            return

        st.markdown(f"#### Answers ({len(answers)})")
        stats = self.get_answer_statistics(question_id)
        if stats and stats.get('average') is not None:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Average", f"{stats['average']:.2f}")
            c2.metric("Min",  stats.get('min', '—'))
            c3.metric("Max",  stats.get('max', '—'))
            c4.metric("Total", stats.get('total_answers', 0))

        df = pd.DataFrame([{
            'Answer ID': a['answer_id'],
            'Account':   a.get('account_username', 'Unknown'),
            'Answer':    a.get('answer_text', ''),
            'Submitted': a.get('submitted_at'),
            'Batch':     a.get('submission_batch_id', ''),
        } for a in answers])
        st.dataframe(df, use_container_width=True, hide_index=True)

    # ------------------------------------------------------------------
    # Tab 2 — Answers view
    # ------------------------------------------------------------------

    def _render_answers_view(self):
        st.subheader("📝 All Answers")

        # ── Two filters only: Site and Survey Name ────────────────────
        col1, col2 = st.columns(2)
        with col1:
            try:
                with get_postgres_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT DISTINCT site_name FROM survey_sites ORDER BY site_name")
                        all_sites = [row[0] for row in cursor.fetchall()]
            except Exception:
                all_sites = []
            site_filter = st.selectbox("🌐 Site:", ["All"] + all_sites, key="ans_site_filter")

        with col2:
            site_id_for_filter = None
            if site_filter != "All":
                try:
                    with get_postgres_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT site_id FROM survey_sites WHERE site_name = %s", (site_filter,))
                            row = cursor.fetchone()
                            site_id_for_filter = row[0] if row else None
                except Exception:
                    pass
            survey_names = self.get_distinct_survey_names(survey_site_id=site_id_for_filter)
            survey_filter = st.selectbox("📋 Survey Name:", ["All"] + survey_names, key="ans_survey_filter")

        # ── Survey count summary ──────────────────────────────────────
        survey_counts = self.get_survey_counts(survey_site_id=site_id_for_filter)
        total_surveys    = len(survey_counts)
        complete_surveys = sum(1 for s in survey_counts if s.get('all_complete'))
        total_answers    = sum(s['answers_generated'] for s in survey_counts)

        mc1, mc2, mc3 = st.columns(3)
        mc1.metric("Total Surveys",     total_surveys)
        mc2.metric("✅ Completed",       complete_surveys)
        mc3.metric("Answers Generated", total_answers)

        st.markdown("---")

        answers = self.get_answers(limit=500)

        # filter in Python after fetch (survey_name filter via question join)
        if site_filter != "All":
            answers = [a for a in answers if a.get('survey_site_name') == site_filter]
        if survey_filter != "All":
            answers = [a for a in answers if a.get('survey_name') == survey_filter]

        if not answers:
            st.info("No answers found.")
            return

        st.caption(f"Showing {len(answers)} answer(s)")

        df = pd.DataFrame([{
            'Answer ID':    a['answer_id'],
            'Survey':       a.get('survey_name', '—'),
            'Question':     (a.get('question_text') or '')[:100],
            'Answer':       a.get('answer_text', ''),
            'Type':         a.get('question_type', ''),
            'Account':      a.get('account_username', 'Unknown'),
            'Site':         a.get('survey_site_name', 'Unknown'),
            'Submitted':    a.get('submitted_at'),
            'Batch':        (a.get('submission_batch_id') or '')[:20],
        } for a in answers])

        st.dataframe(df, use_container_width=True, hide_index=True, height=500)
        st.download_button(
            "📥 Download CSV", data=df.to_csv(index=False),
            file_name=f"answers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )

    # ------------------------------------------------------------------
    # Tab 3 — Analytics
    # ------------------------------------------------------------------

    def _render_analytics(self):
        st.subheader("📊 Questions Analytics")

        summary_df       = self.get_questions_summary()
        type_dist_df     = self.get_question_type_distribution()
        category_dist_df = self.get_question_category_distribution()

        if summary_df.empty:
            st.info("No data available yet.")
            return

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Total Questions",    summary_df['total_questions'].sum())
        col2.metric("Active Questions",   summary_df['active_questions'].sum())
        col3.metric("Total Answers",      summary_df['total_answers'].sum())
        col4.metric("Survey Sites",       len(summary_df))
        col5.metric("With Click Elements",summary_df['questions_with_click_elements'].sum())

        st.divider()
        st.subheader("By Survey Site")
        st.dataframe(summary_df, use_container_width=True, hide_index=True)

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Question Types")
            if not type_dist_df.empty:
                st.dataframe(type_dist_df, use_container_width=True, hide_index=True)
                st.bar_chart(type_dist_df.set_index('question_type')['count'])
        with col2:
            st.subheader("Question Categories")
            if not category_dist_df.empty:
                st.dataframe(category_dist_df, use_container_width=True, hide_index=True)
                st.bar_chart(category_dist_df.set_index('question_category')['count'])

    # ------------------------------------------------------------------
    # Tab 4 — By Survey
    # ------------------------------------------------------------------

    def delete_survey(self, survey_name: str, survey_site_id: Optional[int] = None, account_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Delete all questions belonging to a specific survey.
        This will cascade delete associated answers due to foreign key constraints.
        """
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    query = "DELETE FROM questions WHERE survey_name = %s"
                    params = [survey_name]
                    
                    if survey_site_id:
                        query += " AND survey_site_id = %s"
                        params.append(survey_site_id)
                    if account_id:
                        query += " AND account_id = %s"
                        params.append(account_id)
                    
                    cursor.execute(query, params)
                    conn.commit()
                    deleted_count = cursor.rowcount
                    
                    return {
                        'success': True,
                        'deleted_questions': deleted_count,
                        'error': None
                    }
        except Exception as e:
            logger.error(f"delete_survey: {e}")
            return {'success': False, 'deleted_questions': 0, 'error': str(e)}

    def _render_by_survey(self):
        st.subheader("🗂️ Questions by Survey")
        st.caption("View and manage questions grouped by survey name, including completion status.")

        survey_df = self.get_survey_name_summary()
        if survey_df.empty:
            st.info("No survey names found yet. Extract questions first.")
            return

        # Add action column with delete buttons
        display = survey_df.copy()
        if 'completed_at' in display.columns:
            display['completed_at'] = pd.to_datetime(display['completed_at']).dt.strftime('%Y-%m-%d %H:%M').fillna('—')
        display['all_complete'] = display['all_complete'].apply(lambda x: '🏁 Yes' if x else '⏳ No')

        # Show the dataframe with an extra column for delete actions
        st.dataframe(display, use_container_width=True, hide_index=True)

        st.divider()

        survey_names = survey_df['survey_name'].tolist()
        selected = st.selectbox("Drill into survey:", survey_names, key="by_survey_select")

        if selected:
            qs = self.get_questions(survey_name=selected, limit=200)
            if qs:
                # Add delete button row
                col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
                with col1:
                    st.success(f"**{len(qs)}** questions for *{selected}*")
                with col4:
                    # Delete button with confirmation
                    if st.button("🗑️ Delete Survey", type="secondary", key=f"delete_survey_{selected}"):
                        st.session_state[f'confirm_delete_{selected}'] = True
                
                # Handle delete confirmation
                if st.session_state.get(f'confirm_delete_{selected}', False):
                    st.warning(f"⚠️ Are you sure you want to delete the entire survey **'{selected}'**?")
                    st.warning(f"This will permanently delete all **{len(qs)}** questions and their associated answers.")
                    col_yes, col_no = st.columns(2)
                    with col_yes:
                        if st.button("✅ Yes, Delete Permanently", type="primary", key=f"confirm_yes_{selected}"):
                            # Get survey details from first question
                            first_q = qs[0] if qs else None
                            result = self.delete_survey(
                                survey_name=selected,
                                survey_site_id=first_q.get('survey_site_id') if first_q else None,
                                account_id=first_q.get('account_id') if first_q else None
                            )
                            if result['success']:
                                st.success(f"✅ Deleted {result['deleted_questions']} questions from survey '{selected}'")
                                # Clear session state and rerun
                                del st.session_state[f'confirm_delete_{selected}']
                                st.rerun()
                            else:
                                st.error(f"Failed to delete survey: {result.get('error')}")
                    with col_no:
                        if st.button("❌ No, Cancel", key=f"confirm_no_{selected}"):
                            del st.session_state[f'confirm_delete_{selected}']
                            st.rerun()
                
                answered = sum(1 for q in qs if (q.get('answer_count') or 0) > 0)
                used = sum(1 for q in qs if q.get('used_in_workflow'))
                completed = sum(1 for q in qs if q.get('survey_complete'))
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Answered by Gemini", answered)
                c2.metric("Used in workflow", used)
                c3.metric("Survey complete", f"{'Yes' if completed == len(qs) else 'No'}")
                
                if not all(q.get('survey_complete') for q in qs):
                    first_q = qs[0]
                    if st.button(f"🏁 Mark all '{selected}' questions as complete"):
                        result = self.mark_survey_complete(
                            first_q.get('account_id'), first_q.get('survey_site_id'), selected
                        )
                        if result['success']:
                            st.success(f"✅ Marked {result['updated']} questions complete.")
                            st.rerun()
                        else:
                            st.error(result.get('error'))
                
                for q in qs[:30]:
                    complete_badge = " 🏁" if q.get('survey_complete') else ""
                    answer_badge = f" ({q.get('answer_count',0)} answers)" if q.get('answer_count') else ""
                    st.markdown(
                        f"- **{q['question_type']}**{complete_badge}{answer_badge}: "
                        f"{q['question_text'][:100]}"
                    )
                if len(qs) > 30:
                    st.caption(f"… and {len(qs)-30} more")

    # ------------------------------------------------------------------
    # Tab 5 — Extraction history
    # ------------------------------------------------------------------

    def _render_extraction_history(self):
        st.subheader("🔄 Extraction History")
        st.info("Each batch represents one extraction run. The **Surveys** column shows how many distinct surveys were found in that run.")

        batches = self.get_recent_extractions(limit=50)
        if not batches:
            st.info("No extraction history yet.")
            return

        df = pd.DataFrame([{
            'Batch ID':        b['extraction_batch_id'][:30] + ('…' if len(b['extraction_batch_id']) > 30 else ''),
            'Questions':       b['question_count'],
            'Surveys':         b.get('survey_count', 0),
            'Sites':           b['site_count'],
            'Accounts':        b['account_count'],
            'First Extracted': b['first_extracted'],
            'Last Extracted':  b['last_extracted'],
        } for b in batches])
        st.dataframe(df, use_container_width=True, hide_index=True)

        selected_batch = st.selectbox(
            "View questions from batch:",
            options=[b['extraction_batch_id'] for b in batches if b['extraction_batch_id']],
            format_func=lambda x: (
                f"{x[:30]}… "
                f"({next(b['question_count'] for b in batches if b['extraction_batch_id'] == x)} questions, "
                f"{next(b.get('survey_count',0) for b in batches if b['extraction_batch_id'] == x)} surveys)"
            ),
        )

        if selected_batch:
            batch_qs = self.get_questions(batch_id=selected_batch, limit=100)
            if batch_qs:
                st.write(f"**Questions from batch** `{selected_batch[:30]}…`")
                for q in batch_qs[:25]:
                    survey_label = f"[{q.get('survey_name','?')}] " if q.get('survey_name') else ""
                    st.markdown(
                        f"- {survey_label}`{q.get('question_type')}` "
                        f"`{q.get('question_category','—')}`: "
                        f"{q.get('question_text','')[:100]}"
                    )
                if len(batch_qs) > 25:
                    st.caption(f"… and {len(batch_qs)-25} more")