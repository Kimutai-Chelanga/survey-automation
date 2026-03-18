# File: src/streamlit/ui/pages/accounts/questions_page.py
# Questions management page for surveys - questions are extracted, not created manually

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
    """Questions management page for surveys - questions are extracted, not created manually."""

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self._ensure_tables_exist()

    def _ensure_tables_exist(self):
        """Ensure the questions and answers tables exist with proper structure."""
        try:
            # Questions table - stores extracted questions
            create_questions_table = """
            CREATE TABLE IF NOT EXISTS questions (
                question_id SERIAL PRIMARY KEY,
                survey_site_id INTEGER NOT NULL REFERENCES survey_sites(site_id) ON DELETE CASCADE,
                account_id INTEGER REFERENCES accounts(account_id) ON DELETE CASCADE,
                question_text TEXT NOT NULL,
                question_type VARCHAR(50) NOT NULL CHECK (question_type IN ('multiple_choice', 'text', 'rating', 'yes_no')),
                options JSONB,  -- For multiple choice: array of options
                required BOOLEAN DEFAULT TRUE,
                order_index INTEGER DEFAULT 0,
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                extraction_batch_id VARCHAR(100),
                
                -- Indexes for performance
                UNIQUE(survey_site_id, question_text, account_id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_questions_survey_site ON questions(survey_site_id);
            CREATE INDEX IF NOT EXISTS idx_questions_account ON questions(account_id);
            CREATE INDEX IF NOT EXISTS idx_questions_type ON questions(question_type);
            CREATE INDEX IF NOT EXISTS idx_questions_batch ON questions(extraction_batch_id);
            """
            self.db_manager.execute_query(create_questions_table)
            
            # Answers table - stores responses
            create_answers_table = """
            CREATE TABLE IF NOT EXISTS answers (
                answer_id SERIAL PRIMARY KEY,
                question_id INTEGER NOT NULL REFERENCES questions(question_id) ON DELETE CASCADE,
                account_id INTEGER REFERENCES accounts(account_id) ON DELETE CASCADE,
                answer_text TEXT,
                answer_value_numeric NUMERIC,
                answer_value_boolean BOOLEAN,
                submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                submission_batch_id VARCHAR(100),
                metadata JSONB,
                
                -- Indexes
                INDEX idx_answers_question (question_id),
                INDEX idx_answers_account (account_id),
                INDEX idx_answers_batch (submission_batch_id)
            );
            """
            self.db_manager.execute_query(create_answers_table)
            
            logger.info("✅ Ensured questions and answers tables exist")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")

    # =========================================================================
    # QUESTION QUERY METHODS (READ-ONLY - questions are extracted)
    # =========================================================================

    def get_question(self, question_id: int) -> Optional[Dict[str, Any]]:
        """Get a single question by ID."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT 
                            q.*,
                            ss.country as survey_site_country,
                            ss.url as survey_site_url,
                            a.username as account_username,
                            COUNT(ans.answer_id) as answer_count
                        FROM questions q
                        LEFT JOIN survey_sites ss ON q.survey_site_id = ss.site_id
                        LEFT JOIN accounts a ON q.account_id = a.account_id
                        LEFT JOIN answers ans ON q.question_id = ans.question_id
                        WHERE q.question_id = %s
                        GROUP BY q.question_id, ss.country, ss.url, a.username
                    """, (question_id,))
                    
                    row = cursor.fetchone()
                    if row and row.get('options'):
                        row['options'] = json.loads(row['options'])
                    return dict(row) if row else None

        except Exception as e:
            logger.error(f"Error getting question: {e}")
            return None

    def get_questions(
        self,
        survey_site_id: Optional[int] = None,
        account_id: Optional[int] = None,
        question_type: Optional[str] = None,
        is_active: Optional[bool] = True,
        limit: int = 1000,
        batch_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get questions with optional filters."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT 
                            q.*,
                            ss.country as survey_site_country,
                            ss.url as survey_site_url,
                            a.username as account_username,
                            COUNT(ans.answer_id) as answer_count
                        FROM questions q
                        LEFT JOIN survey_sites ss ON q.survey_site_id = ss.site_id
                        LEFT JOIN accounts a ON q.account_id = a.account_id
                        LEFT JOIN answers ans ON q.question_id = ans.question_id
                        WHERE 1=1
                    """
                    params = []
                    
                    if survey_site_id:
                        query += " AND q.survey_site_id = %s"
                        params.append(survey_site_id)
                    
                    if account_id:
                        query += " AND q.account_id = %s"
                        params.append(account_id)
                    
                    if question_type:
                        query += " AND q.question_type = %s"
                        params.append(question_type)
                    
                    if is_active is not None:
                        query += " AND q.is_active = %s"
                        params.append(is_active)
                    
                    if batch_id:
                        query += " AND q.extraction_batch_id = %s"
                        params.append(batch_id)
                    
                    query += """
                        GROUP BY q.question_id, ss.country, ss.url, a.username
                        ORDER BY q.survey_site_id, q.order_index, q.extracted_at DESC
                        LIMIT %s
                    """
                    params.append(limit)
                    
                    cursor.execute(query, params)
                    results = cursor.fetchall()
                    
                    # Parse options JSON for each question
                    for row in results:
                        if row.get('options'):
                            row['options'] = json.loads(row['options'])
                    
                    return [dict(row) for row in results]

        except Exception as e:
            logger.error(f"Error getting questions: {e}")
            return []

    def get_questions_by_survey_site(self, survey_site_id: int) -> List[Dict[str, Any]]:
        """Get all questions for a specific survey site."""
        return self.get_questions(survey_site_id=survey_site_id)

    def get_questions_by_account(self, account_id: int) -> List[Dict[str, Any]]:
        """Get all questions for a specific account."""
        return self.get_questions(account_id=account_id)

    def get_questions_by_type(self, question_type: str) -> List[Dict[str, Any]]:
        """Get all questions of a specific type."""
        return self.get_questions(question_type=question_type)

    def get_recent_extractions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get most recent extraction batches."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT 
                            extraction_batch_id,
                            COUNT(*) as question_count,
                            MIN(extracted_at) as first_extracted,
                            MAX(extracted_at) as last_extracted,
                            COUNT(DISTINCT survey_site_id) as site_count,
                            COUNT(DISTINCT account_id) as account_count
                        FROM questions
                        WHERE extraction_batch_id IS NOT NULL
                        GROUP BY extraction_batch_id
                        ORDER BY last_extracted DESC
                        LIMIT %s
                    """, (limit,))
                    
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting recent extractions: {e}")
            return []

    # =========================================================================
    # ANSWER MANAGEMENT
    # =========================================================================

    def save_answers(
        self,
        answers: List[Dict[str, Any]],
        account_id: Optional[int] = None,
        batch_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Save answers to questions."""
        try:
            inserted_count = 0
            failed_count = 0
            
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    for answer in answers:
                        try:
                            cursor.execute("""
                                INSERT INTO answers (
                                    question_id, account_id, answer_text,
                                    answer_value_numeric, answer_value_boolean,
                                    submitted_at, submission_batch_id, metadata
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                answer.get('question_id'),
                                answer.get('account_id') or account_id,
                                answer.get('answer_text'),
                                answer.get('answer_value_numeric'),
                                answer.get('answer_value_boolean'),
                                answer.get('submitted_at', datetime.now()),
                                answer.get('submission_batch_id') or batch_id,
                                json.dumps(answer.get('metadata', {})) if answer.get('metadata') else None
                            ))
                            inserted_count += 1
                        except Exception as e:
                            logger.error(f"Failed to save answer: {e}")
                            failed_count += 1
                    
                    conn.commit()
                    
                    logger.info(f"✅ Saved {inserted_count} answers, {failed_count} failed")
                    return {
                        'success': True,
                        'inserted': inserted_count,
                        'failed': failed_count
                    }

        except Exception as e:
            logger.error(f"Error saving answers: {e}")
            return {'success': False, 'error': str(e)}

    def get_answers(
        self,
        question_id: Optional[int] = None,
        account_id: Optional[int] = None,
        batch_id: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get answers with optional filters."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT 
                            a.*,
                            q.question_text,
                            q.question_type,
                            q.survey_site_id,
                            ss.country as survey_site_country,
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
                    
                    return [dict(row) for row in results]

        except Exception as e:
            logger.error(f"Error getting answers: {e}")
            return []

    def get_answer_statistics(self, question_id: int) -> Dict[str, Any]:
        """Get statistics for answers to a specific question."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Get question info first
                    cursor.execute("""
                        SELECT question_type, options
                        FROM questions
                        WHERE question_id = %s
                    """, (question_id,))
                    
                    question = cursor.fetchone()
                    if not question:
                        return {}
                    
                    stats = {
                        'question_id': question_id,
                        'question_type': question['question_type'],
                        'total_answers': 0,
                        'breakdown': {}
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
                                AVG(answer_value_numeric) as average,
                                MIN(answer_value_numeric) as min,
                                MAX(answer_value_numeric) as max,
                                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY answer_value_numeric) as median
                            FROM answers
                            WHERE question_id = %s AND answer_value_numeric IS NOT NULL
                        """, (question_id,))
                        
                        agg = cursor.fetchone()
                        if agg:
                            stats['total_answers'] = agg['total']
                            stats['average'] = float(agg['average']) if agg['average'] else None
                            stats['min'] = float(agg['min']) if agg['min'] else None
                            stats['max'] = float(agg['max']) if agg['max'] else None
                            stats['median'] = float(agg['median']) if agg['median'] else None
                    
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
                        # Just count for text responses
                        cursor.execute("""
                            SELECT COUNT(*) as total
                            FROM answers
                            WHERE question_id = %s
                        """, (question_id,))
                        
                        stats['total_answers'] = cursor.fetchone()['total']
                    
                    return stats

        except Exception as e:
            logger.error(f"Error getting answer statistics: {e}")
            return {}

    # =========================================================================
    # ADMIN/MAINTENANCE FUNCTIONS (for extracted questions)
    # =========================================================================

    def deactivate_question(self, question_id: int) -> Dict[str, Any]:
        """Deactivate a question (soft delete)."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE questions
                        SET is_active = FALSE, last_seen_at = CURRENT_TIMESTAMP
                        WHERE question_id = %s
                    """, (question_id,))
                    conn.commit()
                    
                    if cursor.rowcount == 0:
                        return {'success': False, 'error': 'Question not found'}
                    
                    logger.info(f"✅ Deactivated question ID: {question_id}")
                    return {'success': True}

        except Exception as e:
            logger.error(f"Error deactivating question: {e}")
            return {'success': False, 'error': str(e)}

    def reactivate_question(self, question_id: int) -> Dict[str, Any]:
        """Reactivate a question."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE questions
                        SET is_active = TRUE, last_seen_at = CURRENT_TIMESTAMP
                        WHERE question_id = %s
                    """, (question_id,))
                    conn.commit()
                    
                    if cursor.rowcount == 0:
                        return {'success': False, 'error': 'Question not found'}
                    
                    logger.info(f"✅ Reactivated question ID: {question_id}")
                    return {'success': True}

        except Exception as e:
            logger.error(f"Error reactivating question: {e}")
            return {'success': False, 'error': str(e)}

    def get_questions_summary(self) -> pd.DataFrame:
        """Get summary statistics for questions by survey site."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT 
                            ss.country as survey_site,
                            ss.url,
                            COUNT(DISTINCT q.question_id) as total_questions,
                            COUNT(DISTINCT CASE WHEN q.is_active THEN q.question_id END) as active_questions,
                            COUNT(DISTINCT q.account_id) as unique_accounts,
                            COUNT(a.answer_id) as total_answers,
                            MIN(q.extracted_at) as first_question,
                            MAX(q.extracted_at) as latest_question,
                            COUNT(DISTINCT q.extraction_batch_id) as extraction_batches
                        FROM survey_sites ss
                        LEFT JOIN questions q ON ss.site_id = q.survey_site_id
                        LEFT JOIN answers a ON q.question_id = a.question_id
                        GROUP BY ss.site_id, ss.country, ss.url
                        ORDER BY ss.country
                    """)
                    
                    results = cursor.fetchall()
                    return pd.DataFrame([dict(row) for row in results])

        except Exception as e:
            logger.error(f"Error getting questions summary: {e}")
            return pd.DataFrame()

    # =========================================================================
    # RENDERING METHODS
    # =========================================================================

    def render(self):
        """Main render method for questions page."""
        st.header("📋 Questions & Answers")
        
        # Create tabs
        tab1, tab2, tab3, tab4 = st.tabs([
            "📋 All Questions",
            "📝 Answers",
            "📊 Analytics",
            "🔄 Extraction History"
        ])
        
        with tab1:
            self._render_questions_list()
        
        with tab2:
            self._render_answers_view()
        
        with tab3:
            self._render_analytics()
        
        with tab4:
            self._render_extraction_history()

    def _render_questions_list(self):
        """Render the questions list with filters."""
        st.subheader("📋 Extracted Questions")
        
        st.info("""
        **Questions are automatically extracted from survey sites** - they cannot be created manually.
        Use the filters below to view and manage extracted questions.
        """)
        
        # Load data
        questions = self.get_questions(limit=1000)
        
        if not questions:
            st.warning("No questions have been extracted yet. Run the extraction process first.")
            return
        
        # Filters
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            sites = list(set([q.get('survey_site_country', 'Unknown') for q in questions]))
            site_filter = st.selectbox("Survey Site:", ["All"] + sorted(sites), key="q_filter_site")
        
        with col2:
            types = list(set([q.get('question_type', 'Unknown') for q in questions]))
            type_filter = st.selectbox("Question Type:", ["All"] + sorted(types), key="q_filter_type")
        
        with col3:
            active_filter = st.selectbox("Status:", ["All", "Active", "Inactive"], key="q_filter_active")
        
        with col4:
            search = st.text_input("Search questions:", placeholder="Type to search...")
        
        # Apply filters
        filtered = questions.copy()
        
        if site_filter != "All":
            filtered = [q for q in filtered if q.get('survey_site_country') == site_filter]
        
        if type_filter != "All":
            filtered = [q for q in filtered if q.get('question_type') == type_filter]
        
        if active_filter == "Active":
            filtered = [q for q in filtered if q.get('is_active')]
        elif active_filter == "Inactive":
            filtered = [q for q in filtered if not q.get('is_active')]
        
        if search:
            filtered = [q for q in filtered if search.lower() in q.get('question_text', '').lower()]
        
        # Display stats
        st.info(f"Showing {len(filtered)} of {len(questions)} questions")
        
        # Display questions
        for q in filtered:
            with st.expander(
                f"{'✅' if q.get('is_active') else '⭕'} "
                f"[{q.get('survey_site_country', 'Unknown')}] "
                f"{q.get('question_text', '')[:100]}...",
                expanded=False
            ):
                col1, col2, col3 = st.columns([3, 1, 1])
                
                with col1:
                    st.markdown(f"**Question:** {q.get('question_text')}")
                    st.markdown(f"**Type:** `{q.get('question_type')}`")
                    st.markdown(f"**Required:** {'✅ Yes' if q.get('required') else '❌ No'}")
                    
                    if q.get('options'):
                        st.markdown("**Options:**")
                        for i, opt in enumerate(q.get('options', []), 1):
                            st.markdown(f"  {i}. {opt}")
                    
                    st.caption(f"Extracted: {q.get('extracted_at')}")
                    st.caption(f"Last seen: {q.get('last_seen_at')}")
                    st.caption(f"Answers: {q.get('answer_count', 0)}")
                
                with col2:
                    st.markdown("**Actions:**")
                    
                    # View answers button
                    if st.button("📝 View Answers", key=f"view_ans_{q['question_id']}", use_container_width=True):
                        st.session_state[f'viewing_answers_{q["question_id"]}'] = True
                        st.rerun()
                
                with col3:
                    st.markdown("&nbsp;")
                    
                    # Toggle active status
                    if q.get('is_active'):
                        if st.button("⭕ Deactivate", key=f"deact_{q['question_id']}", use_container_width=True):
                            self.deactivate_question(q['question_id'])
                            st.rerun()
                    else:
                        if st.button("✅ Activate", key=f"act_{q['question_id']}", use_container_width=True):
                            self.reactivate_question(q['question_id'])
                            st.rerun()
                
                # Show answers if requested
                if st.session_state.get(f'viewing_answers_{q["question_id"]}', False):
                    st.divider()
                    self._render_answers_for_question(q['question_id'])
                    
                    if st.button("Close Answers", key=f"close_ans_{q['question_id']}"):
                        del st.session_state[f'viewing_answers_{q["question_id"]}']
                        st.rerun()

    def _render_answers_for_question(self, question_id: int):
        """Render answers for a specific question."""
        answers = self.get_answers(question_id=question_id, limit=100)
        
        if not answers:
            st.info("No answers yet for this question.")
            return
        
        st.markdown(f"#### Answers ({len(answers)})")
        
        # Show statistics if applicable
        stats = self.get_answer_statistics(question_id)
        if stats:
            col1, col2, col3 = st.columns(3)
            
            if stats.get('average') is not None:
                col1.metric("Average", f"{stats['average']:.2f}")
            if stats.get('min') is not None:
                col2.metric("Min", stats['min'])
            if stats.get('max') is not None:
                col3.metric("Max", stats['max'])
        
        # Display answers
        answers_df = pd.DataFrame([{
            'Answer ID': a['answer_id'],
            'Account': a.get('account_username', 'Unknown'),
            'Answer': a.get('answer_text', ''),
            'Submitted': a.get('submitted_at'),
            'Batch': a.get('submission_batch_id', '')
        } for a in answers])
        
        st.dataframe(answers_df, use_container_width=True, hide_index=True)

    def _render_answers_view(self):
        """Render answers view with filters."""
        st.subheader("📝 All Answers")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            site_filter = st.text_input("Filter by survey site:", placeholder="Country name...")
        
        with col2:
            date_range = st.date_input(
                "Date range:",
                value=(datetime.now() - timedelta(days=7), datetime.now()),
                key="ans_date_range"
            )
        
        with col3:
            batch_filter = st.text_input("Batch ID:", placeholder="Filter by batch...")
        
        # Load answers
        answers = self.get_answers(limit=500)
        
        if not answers:
            st.info("No answers found.")
            return
        
        # Apply filters
        filtered = answers.copy()
        
        if site_filter:
            filtered = [a for a in filtered if site_filter.lower() in (a.get('survey_site_country') or '').lower()]
        
        if batch_filter:
            filtered = [a for a in filtered if batch_filter in (a.get('submission_batch_id') or '')]
        
        if len(date_range) == 2:
            start_date, end_date = date_range
            filtered = [
                a for a in filtered
                if a.get('submitted_at') and
                start_date <= a['submitted_at'].date() <= end_date
            ]
        
        # Display
        st.info(f"Showing {len(filtered)} of {len(answers)} answers")
        
        df = pd.DataFrame([{
            'Answer ID': a['answer_id'],
            'Question': a.get('question_text', '')[:100] + '...' if a.get('question_text') else '',
            'Answer': a.get('answer_text', ''),
            'Type': a.get('question_type', ''),
            'Account': a.get('account_username', 'Unknown'),
            'Site': a.get('survey_site_country', 'Unknown'),
            'Submitted': a.get('submitted_at'),
            'Batch': a.get('submission_batch_id', '')
        } for a in filtered])
        
        st.dataframe(df, use_container_width=True, hide_index=True, height=500)
        
        # Export
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download Answers CSV",
            data=csv,
            file_name=f"answers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

    def _render_analytics(self):
        """Render analytics dashboard."""
        st.subheader("📊 Questions Analytics")
        
        # Summary by survey site
        summary_df = self.get_questions_summary()
        
        if summary_df.empty:
            st.info("No data available yet.")
            return
        
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Questions", summary_df['total_questions'].sum())
        
        with col2:
            st.metric("Active Questions", summary_df['active_questions'].sum())
        
        with col3:
            st.metric("Total Answers", summary_df['total_answers'].sum())
        
        with col4:
            st.metric("Survey Sites", len(summary_df))
        
        st.divider()
        
        # Site breakdown
        st.subheader("Breakdown by Survey Site")
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        # Question type distribution
        questions = self.get_questions(limit=1000)
        if questions:
            type_df = pd.DataFrame([
                {'Type': q['question_type'], 'Count': 1}
                for q in questions
            ]).groupby('Type').count().reset_index()
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Question Types")
                st.bar_chart(type_df.set_index('Type'))
            
            with col2:
                st.subheader("Active vs Inactive")
                status_df = pd.DataFrame([
                    {'Status': 'Active', 'Count': len([q for q in questions if q.get('is_active')])},
                    {'Status': 'Inactive', 'Count': len([q for q in questions if not q.get('is_active')])}
                ])
                st.bar_chart(status_df.set_index('Status'))

    def _render_extraction_history(self):
        """Render extraction batch history."""
        st.subheader("🔄 Extraction History")
        
        st.info("""
        This shows when questions were extracted from survey sites.
        Each batch represents one extraction run.
        """)
        
        batches = self.get_recent_extractions(limit=50)
        
        if not batches:
            st.info("No extraction history yet.")
            return
        
        df = pd.DataFrame([{
            'Batch ID': b['extraction_batch_id'],
            'Questions': b['question_count'],
            'Sites': b['site_count'],
            'Accounts': b['account_count'],
            'First Extracted': b['first_extracted'],
            'Last Extracted': b['last_extracted']
        } for b in batches])
        
        st.dataframe(df, use_container_width=True, hide_index=True)
        
        # Allow viewing questions from a specific batch
        selected_batch = st.selectbox(
            "View questions from batch:",
            options=[b['extraction_batch_id'] for b in batches if b['extraction_batch_id']],
            format_func=lambda x: f"{x[:20]}... ({next(b['question_count'] for b in batches if b['extraction_batch_id'] == x)} questions)"
        )
        
        if selected_batch:
            batch_questions = self.get_questions(batch_id=selected_batch, limit=100)
            
            if batch_questions:
                st.write(f"**Questions from batch {selected_batch[:20]}...**")
                
                for q in batch_questions[:20]:
                    st.markdown(f"- {q.get('question_text', '')[:100]}... ({q.get('question_type')})")
                
                if len(batch_questions) > 20:
                    st.caption(f"... and {len(batch_questions) - 20} more")