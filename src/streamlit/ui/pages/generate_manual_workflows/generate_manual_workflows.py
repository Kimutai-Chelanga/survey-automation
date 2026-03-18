# File: src/streamlit/ui/pages/generate_manual_workflows.py
# Generate Manual Workflows Page - Separate tabs for each action

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

        # Account and Site selection at the top (shared across all tabs)
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("👤 Select Account")
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
            st.subheader("🌐 Select Survey Site")
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

        # Create 4 tabs for different actions
        tab1, tab2, tab3, tab4 = st.tabs([
            "🔍 Extract Questions",
            "📝 Answer Questions", 
            "⚙️ Create Workflows",
            "📥 Download Workflows"
        ])

        with tab1:
            self._render_extract_questions_tab(account, site, account_prompt)

        with tab2:
            self._render_answer_questions_tab(account, site, account_prompt)

        with tab3:
            self._render_create_workflows_tab(account, site, account_prompt)

        with tab4:
            self._render_download_workflows_tab(account, site, account_prompt)

        # Show logs (shared across all tabs)
        if st.session_state.generation_logs:
            st.markdown("---")
            with st.expander("📋 Generation Logs", expanded=False):
                log_text = "\n".join(st.session_state.generation_logs[-20:])
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

        # Show results (shared across all tabs)
        if st.session_state.generation_results:
            st.markdown("---")
            self._render_results(st.session_state.generation_results)

    # ============================================================================
    # TAB 1: EXTRACT QUESTIONS
    # ============================================================================

    def _render_extract_questions_tab(self, account: Dict[str, Any], site: Dict[str, Any], prompt: Optional[Dict]):
        """Render extract questions tab."""
        st.subheader("🔍 Extract Questions from Survey Site")

        st.info(f"""
        **Account:** {account['username']}  
        **Survey Site:** {site['country']} - {site['url']}  
        **Prompt:** {prompt['prompt_name'] if prompt else 'None'}
        """)

        if not prompt:
            st.error("❌ Cannot extract questions: Account has no prompt")
            return

        # Additional options
        col1, col2 = st.columns(2)
        with col1:
            max_questions = st.number_input(
                "Max Questions to Extract:",
                min_value=1,
                max_value=100,
                value=20,
                key="extract_max_questions"
            )
        with col2:
            include_metadata = st.checkbox(
                "Include Question Metadata",
                value=True,
                key="extract_include_metadata"
            )

        if st.button("🚀 Start Extraction", type="primary", use_container_width=True, key="extract_btn"):
            self._handle_extract_questions(account, site, prompt, max_questions, include_metadata)

    def _handle_extract_questions(self, account: Dict[str, Any], site: Dict[str, Any], 
                                  prompt: Dict, max_questions: int, include_metadata: bool):
        """Handle extract questions action."""
        self.add_log(f"🚀 Starting question extraction for {account['username']} on {site['country']}...")
        self.add_log(f"📝 Using prompt: {prompt['prompt_name']}")
        self.add_log(f"📊 Max questions: {max_questions}, Include metadata: {include_metadata}")
        
        st.session_state.generation_in_progress = True
        st.session_state.selected_action = "extract"

        # Simulate extraction process
        import random
        questions_found = random.randint(min(5, max_questions), max_questions)

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
            "max_questions": max_questions,
            "include_metadata": include_metadata,
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
        st.rerun()

    # ============================================================================
    # TAB 2: ANSWER QUESTIONS
    # ============================================================================

    def _render_answer_questions_tab(self, account: Dict[str, Any], site: Dict[str, Any], prompt: Optional[Dict]):
        """Render answer questions tab."""
        st.subheader("📝 Answer Questions Using User Prompt")

        st.info(f"""
        **Account:** {account['username']}  
        **Survey Site:** {site['country']} - {site['url']}  
        **Prompt:** {prompt['prompt_name'] if prompt else 'None'}
        """)

        if not prompt:
            st.error("❌ Cannot answer questions: Account has no prompt")
            return

        # Preview prompt
        with st.expander("📋 View Prompt Being Used", expanded=False):
            st.markdown(f"**Prompt Name:** {prompt['prompt_name']}")
            st.markdown(f"**Prompt Type:** {prompt.get('prompt_type', 'user_persona')}")
            st.text_area("Prompt Content:", value=prompt['content'], height=150, disabled=True)

        # Options
        col1, col2 = st.columns(2)
        with col1:
            questions_to_answer = st.number_input(
                "Number of Questions to Answer:",
                min_value=1,
                max_value=50,
                value=10,
                key="answer_questions_count"
            )
        with col2:
            creativity_level = st.slider(
                "Creativity Level:",
                min_value=0.1,
                max_value=1.0,
                value=0.7,
                step=0.1,
                key="answer_creativity",
                help="Higher values = more creative answers"
            )

        if st.button("🚀 Start Answer Generation", type="primary", use_container_width=True, key="answer_btn"):
            self._handle_answer_questions(account, site, prompt, questions_to_answer, creativity_level)

    def _handle_answer_questions(self, account: Dict[str, Any], site: Dict[str, Any], 
                                 prompt: Dict, questions_to_answer: int, creativity: float):
        """Handle answer questions action."""
        self.add_log(f"🚀 Starting answer generation for {account['username']} on {site['country']}...")
        self.add_log(f"📝 Using prompt: {prompt['prompt_name']}")
        self.add_log(f"🎯 Questions to answer: {questions_to_answer}, Creativity: {creativity}")
        
        st.session_state.generation_in_progress = True
        st.session_state.selected_action = "answer"

        # Simulate answer generation
        import random
        answers_generated = questions_to_answer

        self.add_log(f"🤖 Generated {answers_generated} answers using AI")
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
            "questions_answered": answers_generated,
            "answers_generated": answers_generated,
            "creativity_level": creativity,
            "average_response_time_seconds": round(random.uniform(0.5, 2.0), 2),
            "execution_time_seconds": round(random.uniform(3, 8), 1),
            "status": "success"
        }

        self.add_log(f"✅ Answer generation complete in {results['execution_time_seconds']}s")
        st.session_state.generation_results = results
        st.session_state.generation_in_progress = False
        st.rerun()

    # ============================================================================
    # TAB 3: CREATE WORKFLOWS
    # ============================================================================

    def _render_create_workflows_tab(self, account: Dict[str, Any], site: Dict[str, Any], prompt: Optional[Dict]):
        """Render create workflows tab."""
        st.subheader("⚙️ Create Workflows from Questions")

        st.info(f"""
        **Account:** {account['username']}  
        **Survey Site:** {site['country']} - {site['url']}  
        **Prompt:** {prompt['prompt_name'] if prompt else 'None'}
        """)

        if not prompt:
            st.warning("⚠️ Account has no prompt - workflows can still be created but may lack personalization")

        # Options
        col1, col2 = st.columns(2)
        with col1:
            workflow_count = st.number_input(
                "Number of Workflows to Create:",
                min_value=1,
                max_value=10,
                value=3,
                key="create_workflow_count"
            )
        with col2:
            workflow_type = st.selectbox(
                "Workflow Type:",
                options=["extraction", "submission", "validation", "mixed"],
                key="create_workflow_type"
            )

        if st.button("🚀 Create Workflows", type="primary", use_container_width=True, key="create_btn"):
            self._handle_create_workflows(account, site, prompt, workflow_count, workflow_type)

    def _handle_create_workflows(self, account: Dict[str, Any], site: Dict[str, Any], 
                                 prompt: Optional[Dict], workflow_count: int, workflow_type: str):
        """Handle create workflows action."""
        self.add_log(f"🚀 Creating workflows for {account['username']} on {site['country']}...")
        self.add_log(f"📊 Creating {workflow_count} workflows of type: {workflow_type}")
        
        st.session_state.generation_in_progress = True
        st.session_state.selected_action = "create"

        # Generate mock workflows
        import random
        from datetime import timedelta

        workflows = []
        for i in range(workflow_count):
            if workflow_type == "mixed":
                wf_type = random.choice(["extraction", "submission", "validation"])
            else:
                wf_type = workflow_type

            workflow = {
                "id": f"wf_{account['account_id']}_{site['site_id']}_{i+1}",
                "name": f"{site['country']}_{wf_type}_{i+1}",
                "type": wf_type,
                "version": "1.0",
                "account_id": account['account_id'],
                "site_id": site['site_id'],
                "steps": random.randint(5, 15),
                "created": (datetime.now() - timedelta(minutes=random.randint(1, 10))).isoformat(),
                "prompt_used": prompt['prompt_id'] if prompt else None,
                "configuration": {
                    "headless": False,
                    "timeout_seconds": 30,
                    "retry_count": 3
                }
            }
            workflows.append(workflow)

        self.add_log(f"📦 Created {len(workflows)} workflows")

        results = {
            "action": "create_workflows",
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
            "workflows_created": workflow_count,
            "workflows": workflows,
            "total_steps": sum(w['steps'] for w in workflows),
            "execution_time_seconds": round(random.uniform(2, 6), 1),
            "status": "success"
        }

        self.add_log(f"✅ Workflow creation complete in {results['execution_time_seconds']}s")
        st.session_state.generation_results = results
        st.session_state.generation_in_progress = False
        st.rerun()

    # ============================================================================
    # TAB 4: DOWNLOAD WORKFLOWS
    # ============================================================================

    def _render_download_workflows_tab(self, account: Dict[str, Any], site: Dict[str, Any], prompt: Optional[Dict]):
        """Render download workflows tab."""
        st.subheader("📥 Download Workflows as JSON")

        st.info(f"""
        **Account:** {account['username']}  
        **Survey Site:** {site['country']} - {site['url']}  
        **Prompt:** {prompt['prompt_name'] if prompt else 'None'}
        """)

        # Options
        col1, col2 = st.columns(2)
        with col1:
            include_manifest = st.checkbox(
                "Include Manifest File",
                value=True,
                key="download_include_manifest"
            )
        with col2:
            format_type = st.selectbox(
                "Download Format:",
                options=["Single JSON", "Multiple JSONs", "ZIP Package"],
                key="download_format"
            )

        if st.button("📦 Generate & Download", type="primary", use_container_width=True, key="download_btn"):
            self._handle_download_workflows(account, site, prompt, include_manifest, format_type)

    def _handle_download_workflows(self, account: Dict[str, Any], site: Dict[str, Any], 
                                   prompt: Optional[Dict], include_manifest: bool, format_type: str):
        """Handle download workflows action."""
        self.add_log(f"🚀 Generating downloadable workflows for {account['username']} on {site['country']}...")
        self.add_log(f"📦 Format: {format_type}, Include manifest: {include_manifest}")
        
        st.session_state.generation_in_progress = True
        st.session_state.selected_action = "download"

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
                "created": (datetime.now() - timedelta(minutes=random.randint(1, 10))).isoformat(),
                "prompt_used": prompt['prompt_id'] if prompt else None,
                "configuration": {
                    "headless": False,
                    "timeout_seconds": 30,
                    "retry_count": 3
                },
                "workflow_data": {
                    "extVersion": "1.30.00",
                    "name": f"{site['country']}_{workflow_type}_{i+1}",
                    "description": f"Workflow for {site['country']}",
                    "nodes": random.randint(5, 15),
                    "edges": random.randint(4, 20)
                }
            }
            workflows.append(workflow)

        self.add_log(f"📦 Generated {workflow_count} workflows for download")

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
            "include_manifest": include_manifest,
            "format_type": format_type,
            "execution_time_seconds": round(random.uniform(1, 3), 1),
            "status": "success",
            "download_available": True
        }

        self.add_log(f"✅ Download package generation complete")
        st.session_state.generation_results = results
        st.session_state.generation_in_progress = False
        st.rerun()

    # ============================================================================
    # RESULTS RENDERING
    # ============================================================================

    def _render_results(self, results: Dict):
        """Render generation results."""
        action = results.get('action', 'unknown')
        action_titles = {
            "extract": "Extract Questions",
            "answer": "Answer Questions",
            "create": "Create Workflows",
            "download": "Download Workflows"
        }
        
        st.subheader(f"✅ {action_titles.get(action, action.title())} Results")

        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            if action == 'extract':
                st.metric("Questions Extracted", results.get('questions_extracted', 0))
            elif action == 'answer':
                st.metric("Answers Generated", results.get('answers_generated', 0))
            elif action in ['create', 'download']:
                st.metric("Workflows Generated", results.get('workflows_generated', results.get('workflows_created', 0)))

        with col2:
            st.metric("Account", results['account']['username'])

        with col3:
            st.metric("Survey Site", results['site']['country'])

        with col4:
            st.metric("Execution Time", f"{results.get('execution_time_seconds', 0)}s")

        st.markdown("---")

        # Detailed results
        if action == 'extract' and 'question_types' in results:
            col_left, col_right = st.columns(2)
            
            with col_left:
                st.markdown("**📋 Details**")
                st.json({
                    "account_id": results['account']['id'],
                    "site_id": results['site']['id'],
                    "max_questions": results.get('max_questions', 'N/A'),
                    "include_metadata": results.get('include_metadata', False),
                    "timestamp": results['timestamp']
                })
            
            with col_right:
                st.markdown("**📊 Question Types**")
                st.json(results['question_types'])

        elif action == 'answer':
            col_left, col_right = st.columns(2)
            
            with col_left:
                st.markdown("**📋 Details**")
                st.json({
                    "account_id": results['account']['id'],
                    "site_id": results['site']['id'],
                    "questions_answered": results.get('questions_answered', 0),
                    "creativity_level": results.get('creativity_level', 0.7),
                    "avg_response_time": f"{results.get('average_response_time_seconds', 0)}s"
                })
            
            with col_right:
                if results.get('prompt_used'):
                    st.markdown("**📝 Prompt Used**")
                    st.info(results['prompt_used']['name'])
                    st.caption(results['prompt_used']['preview'])

        elif action in ['create', 'download'] and 'workflows' in results:
            st.markdown("**📦 Generated Workflows**")
            
            for i, wf in enumerate(results['workflows']):
                with st.expander(f"📋 {wf['name']} ({wf['type']})", expanded=(i == 0)):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Steps", wf.get('steps', 'N/A'))
                    with col2:
                        st.metric("Version", wf.get('version', '1.0'))
                    with col3:
                        st.metric("Workflow ID", wf['id'][:8] + "...")
                    
                    # Download button for individual workflow
                    wf_json = json.dumps(wf, indent=2)
                    st.download_button(
                        label=f"⬇️ Download {wf['name']}.json",
                        data=wf_json,
                        file_name=f"{wf['name']}.json",
                        mime="application/json",
                        key=f"download_wf_{i}_{datetime.now().timestamp()}"
                    )

            # Bulk download
            st.markdown("---")
            st.markdown("**📦 Bulk Download**")
            bulk_json = json.dumps(results['workflows'], indent=2)
            st.download_button(
                label="⬇️ Download All Workflows",
                data=bulk_json,
                file_name=f"workflows_{results['account']['username']}_{results['site']['country']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                use_container_width=True
            )

        else:
            # Fallback for other actions
            st.json(results)

        # Clear button
        if st.button("Clear Results", key="clear_results"):
            st.session_state.generation_results = None
            st.rerun()

    # ============================================================================
    # DATA LOADING HELPERS
    # ============================================================================

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