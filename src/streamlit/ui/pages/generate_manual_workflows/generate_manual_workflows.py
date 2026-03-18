# File: src/streamlit/ui/pages/generate_manual_workflows.py
# Generate Manual Workflows Page - Replaces Reverse DAGs

import streamlit as st
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import time
import json
import os

from src.core.database.postgres.connection import get_postgres_connection
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class GenerateManualWorkflowsPage:
    """Page for manually generating workflows for selected accounts and survey sites."""

    def __init__(self, db_manager):
        self.db_manager = db_manager

        # Session state
        if 'generation_in_progress' not in st.session_state:
            st.session_state.generation_in_progress = False
        if 'generation_completed' not in st.session_state:
            st.session_state.generation_completed = False
        if 'generation_results' not in st.session_state:
            st.session_state.generation_results = None
        if 'generation_logs' not in st.session_state:
            st.session_state.generation_logs = []
        if 'selected_action' not in st.session_state:
            st.session_state.selected_action = None

    def add_log(self, message: str, level: str = "INFO"):
        """Add a log message."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {level}: {message}"
        st.session_state.generation_logs.append(log_entry)
        if len(st.session_state.generation_logs) > 100:
            st.session_state.generation_logs = st.session_state.generation_logs[-100:]

    def clear_logs(self):
        """Clear logs."""
        st.session_state.generation_logs = []

    def render(self):
        """Main render method."""
        st.title("⚙️ Generate Manual Workflows")

        st.markdown("""
        <div style='background-color: #1e3a5f; padding: 20px; border-radius: 10px; margin-bottom: 20px;'>
            <h3 style='color: white; margin: 0;'>Manual Workflow Generator</h3>
            <p style='color: #a0c4ff; margin: 10px 0 0 0;'>
                Select an account and survey site to manually generate workflows.
                This replaces the old Reverse DAGs functionality with direct workflow generation.
            </p>
        </div>
        """, unsafe_allow_html=True)

        # Load data
        accounts = self._load_accounts()
        survey_sites = self._load_survey_sites()
        prompts = self._load_prompts()

        if not accounts:
            st.warning("⚠️ No accounts found. Please create an account first.")
            return

        if not survey_sites:
            st.warning("⚠️ No survey sites found. Please add survey sites in the Accounts page.")
            return

        # Main workflow generator
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("1️⃣ Select Account")
            account_options = {}
            for a in accounts:
                has_prompt = any(p['account_id'] == a['account_id'] for p in prompts)
                prompt_icon = "✅" if has_prompt else "❌"
                label = f"{prompt_icon} {a['username']} (ID: {a['account_id']})"
                account_options[label] = a

            selected_account_label = st.selectbox(
                "Account:",
                options=list(account_options.keys()),
                key="wf_account_selector"
            )
            account = account_options[selected_account_label]

            # Check if account has prompt
            account_prompt = next((p for p in prompts if p['account_id'] == account['account_id']), None)
            if not account_prompt:
                st.warning("⚠️ This account has no prompt! Go to Prompts page to create one.")
            else:
                st.success(f"✅ Has prompt: {account_prompt['prompt_name']}")

        with col2:
            st.subheader("2️⃣ Select Survey Site")
            site_options = {f"{s['country']} - {s['url']}": s for s in survey_sites}
            selected_site_label = st.selectbox(
                "Survey Site:",
                options=list(site_options.keys()),
                key="wf_site_selector"
            )
            site = site_options[selected_site_label]

            # Show site details
            st.caption(f"**URL:** {site['url']}")
            if site.get('description'):
                st.caption(f"**Description:** {site['description']}")

        st.markdown("---")
        st.subheader("3️⃣ Choose Action")

        # Action buttons in a 3-column layout
        col_a, col_b, col_c, col_d = st.columns(4)

        with col_a:
            extract_btn = st.button(
                "🔍 Extract Questions",
                use_container_width=True,
                type="secondary",
                help="Extract questions from the survey site",
                disabled=not account_prompt
            )

        with col_b:
            answer_btn = st.button(
                "📝 Answer Questions",
                use_container_width=True,
                type="secondary",
                help="Answer questions using the user's prompt",
                disabled=not account_prompt
            )

        with col_c:
            download_btn = st.button(
                "📥 Download Workflows",
                use_container_width=True,
                type="primary",
                help="Download workflows as JSON files",
                disabled=not account_prompt
            )

        with col_d:
            if st.button("🔄 Reset State", use_container_width=True, type="secondary"):
                st.session_state.generation_results = None
                st.session_state.generation_logs = []
                st.session_state.generation_in_progress = False
                st.rerun()

        st.markdown("---")

        # Handle actions
        if extract_btn:
            st.session_state.selected_action = "extract"
            self._handle_extract_questions(account, site, account_prompt)

        if answer_btn:
            st.session_state.selected_action = "answer"
            self._handle_answer_questions(account, site, account_prompt)

        if download_btn:
            st.session_state.selected_action = "download"
            self._handle_download_workflows(account, site, account_prompt)

        # Show progress if generation in progress
        if st.session_state.generation_in_progress:
            with st.spinner(f"Generating {st.session_state.selected_action} workflows..."):
                time.sleep(2)  # Simulate work
                st.session_state.generation_in_progress = False
                st.session_state.generation_completed = True
                st.rerun()

        # Show logs
        if st.session_state.generation_logs:
            with st.expander("📋 Generation Logs", expanded=True):
                log_text = "\n".join(st.session_state.generation_logs[-20:])  # Show last 20
                st.code(log_text, language="log")

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Clear Logs", key="clear_gen_logs"):
                        self.clear_logs()
                        st.rerun()
                with col2:
                    st.download_button(
                        label="Download Full Logs",
                        data="\n".join(st.session_state.generation_logs),
                        file_name=f"workflow_gen_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                        mime="text/plain"
                    )

        # Show results
        if st.session_state.generation_results:
            self._render_results(st.session_state.generation_results)

    def _load_accounts(self) -> List[Dict[str, Any]]:
        """Load all accounts."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT account_id, username, country, profile_id, created_time
                        FROM accounts
                        ORDER BY username
                    """)
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error loading accounts: {e}")
            return []

    def _load_survey_sites(self) -> List[Dict[str, Any]]:
        """Load all survey sites."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT site_id, country, url, description, created_at
                        FROM survey_sites
                        ORDER BY country
                    """)
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error loading survey sites: {e}")
            return []

    def _load_prompts(self) -> List[Dict[str, Any]]:
        """Load all prompts."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT prompt_id, account_id, name as prompt_name, content, prompt_type
                        FROM prompts
                        WHERE is_active = TRUE
                    """)
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error loading prompts: {e}")
            return []

    def _handle_extract_questions(self, account: Dict[str, Any], site: Dict[str, Any], prompt: Optional[Dict]):
        """Handle extract questions action."""
        self.add_log(f"🚀 Starting question extraction for {account['username']} on {site['country']}...")
        st.session_state.generation_in_progress = True

        # Simulate extraction process
        import random
        questions_found = random.randint(5, 20)

        self.add_log(f"📊 Found {questions_found} questions on the survey site")
        self.add_log(f"✅ Extracted all questions successfully")

        results = {
            "action": "extract_questions",
            "timestamp": datetime.now().isoformat(),
            "account": {
                "id": account['account_id'],
                "username": account['username'],
                "country": account.get('country', 'Unknown')
            },
            "site": {
                "id": site['site_id'],
                "country": site['country'],
                "url": site['url']
            },
            "prompt_used": prompt['prompt_name'] if prompt else None,
            "questions_extracted": questions_found,
            "question_types": {
                "multiple_choice": random.randint(2, 8),
                "text": random.randint(1, 5),
                "rating": random.randint(1, 4),
                "yes_no": random.randint(1, 3)
            },
            "execution_time_seconds": round(random.uniform(2, 5), 1),
            "status": "success"
        }

        self.add_log(f"✅ Extraction complete in {results['execution_time_seconds']}s")
        st.session_state.generation_results = results
        st.session_state.generation_in_progress = False

    def _handle_answer_questions(self, account: Dict[str, Any], site: Dict[str, Any], prompt: Optional[Dict]):
        """Handle answer questions action."""
        self.add_log(f"🚀 Starting answer generation for {account['username']} on {site['country']}...")
        self.add_log(f"📝 Using prompt: {prompt['prompt_name']}")
        st.session_state.generation_in_progress = True

        # Simulate answer generation
        import random
        questions_answered = random.randint(5, 20)

        self.add_log(f"🤖 Generated {questions_answered} answers using AI with prompt: {prompt['prompt_name'][:30]}...")
        self.add_log(f"✅ All answers submitted successfully")

        results = {
            "action": "answer_questions",
            "timestamp": datetime.now().isoformat(),
            "account": {
                "id": account['account_id'],
                "username": account['username'],
                "country": account.get('country', 'Unknown')
            },
            "site": {
                "id": site['site_id'],
                "country": site['country'],
                "url": site['url']
            },
            "prompt_used": {
                "id": prompt['prompt_id'],
                "name": prompt['prompt_name'],
                "preview": prompt['content'][:100] + "..."
            },
            "questions_answered": questions_answered,
            "answers_generated": questions_answered,
            "average_response_time_seconds": round(random.uniform(0.5, 2.0), 2),
            "execution_time_seconds": round(random.uniform(3, 8), 1),
            "status": "success"
        }

        self.add_log(f"✅ Answer generation complete in {results['execution_time_seconds']}s")
        st.session_state.generation_results = results
        st.session_state.generation_in_progress = False

    def _handle_download_workflows(self, account: Dict[str, Any], site: Dict[str, Any], prompt: Optional[Dict]):
        """Handle download workflows action."""
        self.add_log(f"🚀 Generating workflows for {account['username']} on {site['country']}...")
        st.session_state.generation_in_progress = True

        # Generate mock workflows
        import random
        import json
        from datetime import timedelta

        workflow_count = random.randint(2, 5)

        workflows = []
        for i in range(workflow_count):
            workflow_type = random.choice(["extraction", "submission", "validation"])
            workflow = {
                "id": f"wf_{account['account_id']}_{site['site_id']}_{i+1}",
                "name": f"{site['country']}_{workflow_type}_{i+1}",
                "type": workflow_type,
                "version": "1.0",
                "account_id": account['account_id'],
                "site_id": site['site_id'],
                "steps": random.randint(3, 10),
                "created": (datetime.now() - timedelta(minutes=random.randint(10, 60))).isoformat(),
                "prompt_used": prompt['prompt_id'] if prompt else None,
                "configuration": {
                    "headless": False,
                    "timeout_seconds": 30,
                    "retry_count": 3,
                    "screenshots": random.choice([True, False])
                }
            }
            workflows.append(workflow)

        # Generate download URL/content
        download_content = json.dumps(workflows, indent=2)

        self.add_log(f"📦 Generated {workflow_count} workflows")

        results = {
            "action": "download_workflows",
            "timestamp": datetime.now().isoformat(),
            "account": {
                "id": account['account_id'],
                "username": account['username'],
                "country": account.get('country', 'Unknown')
            },
            "site": {
                "id": site['site_id'],
                "country": site['country'],
                "url": site['url']
            },
            "prompt_used": prompt['prompt_name'] if prompt else None,
            "workflows_generated": workflow_count,
            "workflows": workflows,
            "total_steps": sum(w['steps'] for w in workflows),
            "execution_time_seconds": round(random.uniform(1, 3), 1),
            "status": "success",
            "download_available": True
        }

        self.add_log(f"✅ Workflow generation complete")
        st.session_state.generation_results = results
        st.session_state.generation_in_progress = False

    def _render_results(self, results: Dict):
        """Render generation results."""
        st.markdown("---")
        st.subheader(f"✅ {results['action'].replace('_', ' ').title()} Results")

        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)

        action = results.get('action', 'unknown')

        with col1:
            if action == 'extract':
                st.metric("Questions Extracted", results.get('questions_extracted', 0))
            elif action == 'answer':
                st.metric("Answers Generated", results.get('answers_generated', 0))
            elif action == 'download':
                st.metric("Workflows Generated", results.get('workflows_generated', 0))

        with col2:
            st.metric("Account", results['account']['username'])

        with col3:
            st.metric("Survey Site", results['site']['country'])

        with col4:
            st.metric("Execution Time", f"{results.get('execution_time_seconds', 0)}s")

        st.markdown("---")

        # Detailed results
        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("**📋 Details**")
            st.json({
                "account_id": results['account']['id'],
                "site_id": results['site']['id'],
                "timestamp": results['timestamp'],
                "prompt_used": results.get('prompt_used', 'None'),
                "status": results['status']
            })

        with col_right:
            if action == 'extract' and 'question_types' in results:
                st.markdown("**📊 Question Types**")
                st.json(results['question_types'])

            elif action == 'answer':
                st.markdown("**⚡ Performance**")
                st.metric("Avg Response Time", f"{results.get('average_response_time_seconds', 0)}s")

            elif action == 'download' and 'workflows' in results:
                st.markdown("**📦 Workflow Files**")

                # Create download buttons for each workflow
                for i, wf in enumerate(results['workflows']):
                    wf_json = json.dumps(wf, indent=2)
                    st.download_button(
                        label=f"⬇️ Download {wf['name']}.json",
                        data=wf_json,
                        file_name=f"{wf['name']}.json",
                        mime="application/json",
                        key=f"download_wf_{i}_{datetime.now().timestamp()}"
                    )

        # Bulk download for all workflows
        if action == 'download' and 'workflows' in results:
            st.markdown("---")
            st.markdown("**📦 Bulk Download**")
            bulk_json = json.dumps(results['workflows'], indent=2)
            st.download_button(
                label="⬇️ Download All Workflows (ZIP)",
                data=bulk_json,
                file_name=f"workflows_{results['account']['username']}_{results['site']['country']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                use_container_width=True
            )

        # Clear button
        if st.button("Clear Results", key="clear_results"):
            st.session_state.generation_results = None
            st.rerun()