# File: src/streamlit/ui/pages/generate_manual_workflows/generate_manual_workflows.py
# Generate Manual Workflows Page - Complete with modular extraction and workflow creation

import streamlit as st
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import time
import json
import os
import random
from datetime import timedelta

from src.core.database.postgres.connection import get_postgres_connection
from psycopg2.extras import RealDictCursor

# Import orchestrator
from .orchestrator import SurveySiteOrchestrator
from .utils.chrome_helpers import ensure_chrome_running

logger = logging.getLogger(__name__)


class GenerateManualWorkflowsPage:
    """Page for manually generating workflows for selected accounts and survey sites."""

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.orchestrator = SurveySiteOrchestrator(db_manager)

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
                <br>🔍 <strong>Extract Questions</strong> & ⚙️ <strong>Create Workflows</strong> are modular - each site has its own logic.
                <br>📝 <strong>Answer Questions</strong> & 📥 <strong>Download Workflows</strong> are simple demo versions.
            </p>
        </div>
        """, unsafe_allow_html=True)

        # Load data
        accounts = self._load_accounts()
        survey_sites = self._load_survey_sites()
        prompts = self._load_prompts()
        
        # Get available sites from orchestrator
        available_sites = self.orchestrator.get_available_sites()
        available_site_names = [s['site_name'] for s in available_sites]

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
            
            # Filter survey sites to those with both extractor and workflow creator
            filtered_sites = [s for s in survey_sites if s['site_name'] in available_site_names]
            
            if not filtered_sites:
                st.warning("⚠️ No survey sites with complete modules found. Available sites:")
                if available_sites:
                    with st.expander("Available Sites"):
                        for site in available_sites:
                            st.write(f"- {site['site_name']}: {site.get('description', 'No description')}")
                return
                
            site_options = {f"{s['site_name']}": s for s in filtered_sites}
            selected_site_label = st.selectbox(
                "Survey Site:",
                options=list(site_options.keys()),
                key="wf_site_selector"
            )
            site = site_options[selected_site_label]

            # Show site details
            site_info = next((s for s in available_sites if s['site_name'] == site['site_name']), {})
            st.caption(f"**Extractor:** v{site_info.get('extractor_version', '1.0.0')} | **Creator:** v{site_info.get('creator_version', '1.0.0')}")
            if site.get('description'):
                st.caption(f"**Description:** {site['description']}")

        st.markdown("---")

        # Create 4 tabs for different actions
        tab1, tab2, tab3, tab4 = st.tabs([
            "🔍 Extract Questions (Modular)",
            "📝 Answer Questions (Demo)", 
            "⚙️ Create Workflows (Modular)",
            "📥 Download Workflows (Demo)"
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
    # TAB 1: EXTRACT QUESTIONS (MODULAR)
    # ============================================================================

    def _render_extract_questions_tab(self, account: Dict[str, Any], site: Dict[str, Any], prompt: Optional[Dict]):
        """Render extract questions tab - modular per site."""
        st.subheader("🔍 Extract Questions from Survey Site")

        # Get extractor info for this site
        extractor_info = None
        if site['site_name'] in self.orchestrator.extractors:
            extractor_info = self.orchestrator.extractors[site['site_name']].get_site_info()

        st.info(f"""
        **Account:** {account['username']}  
        **Survey Site:** {site['site_name']}  
        **Extractor Version:** {extractor_info.get('version', '1.0.0') if extractor_info else 'Standard'}  
        **Prompt:** {prompt['prompt_name'] if prompt else 'None'}
        
        This site has a dedicated extractor with site-specific logic.
        """)

        if not prompt:
            st.error("❌ Cannot extract questions: Account has no prompt")
            return

        # Get account URLs for this site
        account_urls = self._get_account_urls(account['account_id'], site['site_id'])
        
        if not account_urls:
            st.warning(f"⚠️ No URLs configured for {account['username']} on {site['site_name']}. Please add URLs in Accounts page.")
            return

        # URL selection
        url_options = []
        url_mapping = {}
        
        for url_info in account_urls:
            used_status = " (Used)" if url_info.get('is_used', False) else " (Available)"
            default_marker = "⭐ " if url_info.get('is_default', False) else ""
            label = f"{default_marker}{url_info['url']}{used_status}"
            url_mapping[label] = url_info
            url_options.append(label)

        selected_url_label = st.selectbox(
            "Select URL to Extract From:",
            options=url_options,
            key="extract_url_select"
        )
        
        selected_url_info = url_mapping[selected_url_label]
        
        if selected_url_info.get('is_used', False):
            st.warning("⚠️ This URL has already been used for extraction. Are you sure you want to use it again?")

        # Extraction options
        col1, col2 = st.columns(2)
        with col1:
            max_questions = st.number_input("Max Questions", min_value=1, max_value=100, value=50, key="extract_max_q")
        with col2:
            include_details = st.checkbox("Include Question Details", value=True, key="extract_include_details")

        # Site-specific options
        with st.expander("🔧 Site-Specific Options", expanded=False):
            st.info(f"Options for {site['site_name']} will appear here")
            use_chrome_profile = st.checkbox("Use Chrome Profile", value=True, help="Use the account's Chrome profile for authentication")

        if st.button("🚀 Start Extraction", type="primary", use_container_width=True, key="extract_btn"):
            self._handle_extract_questions(account, site, prompt, selected_url_info, max_questions, include_details, use_chrome_profile)

    def _handle_extract_questions(self, account: Dict[str, Any], site: Dict[str, Any], 
                                  prompt: Dict, url_info: Dict, max_questions: int, 
                                  include_details: bool, use_chrome_profile: bool):
        """Handle extract questions action using orchestrator."""
        self.add_log(f"🚀 Starting question extraction for {account['username']} on {site['site_name']}...")
        self.add_log(f"📝 Using prompt: {prompt['prompt_name']}")
        self.add_log(f"🔗 URL: {url_info['url']}")
        self.add_log(f"📊 Site-specific extractor: {site['site_name']}")
        
        st.session_state.generation_in_progress = True
        st.session_state.selected_action = "extract"

        try:
            # Get Chrome profile path for this account
            from src.streamlit.ui.pages.accounts.chrome_session_manager import ChromeSessionManager
            chrome_manager = ChromeSessionManager(self.db_manager)
            profile_path = chrome_manager.get_profile_path(account['username'])
            
            # Ensure Chrome is running if needed
            if use_chrome_profile:
                chrome_ready = ensure_chrome_running(profile_path)
                if not chrome_ready:
                    self.add_log("⚠️ Chrome not ready, but continuing...", "WARNING")
            
            # Use orchestrator to extract
            results = self.orchestrator.extract_questions(
                account_id=account['account_id'],
                site_id=site['site_id'],
                url=url_info['url'],
                profile_path=profile_path,
                site_name=site['site_name'],
                max_questions=max_questions,
                include_details=include_details
            )
            
            if results.get('success'):
                # Mark URL as used
                self._mark_url_used(url_info['url_id'])
                
                self.add_log(f"📊 Found {results.get('questions_found', 0)} questions")
                self.add_log(f"✅ Extracted all questions successfully")
                
                st.session_state.generation_results = {
                    "action": "extract_questions",
                    "timestamp": datetime.now().isoformat(),
                    "account": {
                        "id": account['account_id'],
                        "username": account['username']
                    },
                    "site": {
                        "id": site['site_id'],
                        "name": site['site_name']
                    },
                    "url": {
                        "id": url_info['url_id'],
                        "url": url_info['url']
                    },
                    "prompt_used": prompt['prompt_name'],
                    "questions_extracted": results.get('questions_found', 0),
                    "inserted": results.get('inserted', 0),
                    "batch_id": results.get('batch_id'),
                    "execution_time_seconds": results.get('execution_time_seconds', 0),
                    "status": "success"
                }
            else:
                self.add_log(f"❌ Extraction failed: {results.get('error', 'Unknown error')}")
                st.session_state.generation_results = {
                    "action": "extract_questions",
                    "status": "failed",
                    "error": results.get('error', 'Unknown error'),
                    "timestamp": datetime.now().isoformat(),
                    "account": {
                        "id": account['account_id'],
                        "username": account['username']
                    },
                    "site": {
                        "id": site['site_id'],
                        "name": site['site_name']
                    }
                }
                
        except Exception as e:
            self.add_log(f"❌ Extraction error: {str(e)}")
            st.session_state.generation_results = {
                "action": "extract_questions",
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "account": {
                    "id": account['account_id'],
                    "username": account['username']
                },
                "site": {
                    "id": site['site_id'],
                    "name": site['site_name']
                }
            }
            
        finally:
            st.session_state.generation_in_progress = False
            st.rerun()

    # ============================================================================
    # TAB 2: ANSWER QUESTIONS (SIMPLE DEMO)
    # ============================================================================

    def _render_answer_questions_tab(self, account: Dict[str, Any], site: Dict[str, Any], prompt: Optional[Dict]):
        """Render answer questions tab - simple demo version."""
        st.subheader("📝 Answer Questions (Demo Version)")

        st.info(f"""
        **Account:** {account['username']}  
        **Survey Site:** {site['site_name']}  
        **Prompt:** {prompt['prompt_name'] if prompt else 'None'}
        
        ⚠️ **This is a demo version** - answers are simulated.
        In production, this would use AI to generate answers based on the prompt.
        """)

        if not prompt:
            st.error("❌ Cannot answer questions: Account has no prompt")
            return

        # Get questions for this account/site that have been extracted
        questions = self._get_unused_questions(account['account_id'], site['site_id'])
        
        if not questions:
            st.info("No questions available for this account/site. Extract questions first.")
            return

        st.success(f"✅ Found {len(questions)} questions ready for answering")

        # Show question preview
        with st.expander("📋 Available Questions", expanded=False):
            for q in questions[:5]:
                st.markdown(f"- {q['question_text'][:100]}... ({q['question_type']})")
            if len(questions) > 5:
                st.caption(f"... and {len(questions) - 5} more")

        # Simple options
        questions_to_answer = st.number_input(
            "Number of Questions to Answer (demo):",
            min_value=1,
            max_value=min(10, len(questions)),
            value=min(5, len(questions)),
            key="answer_questions_count"
        )

        # Preview prompt
        with st.expander("📋 View Prompt Being Used", expanded=False):
            st.markdown(f"**Prompt Name:** {prompt['prompt_name']}")
            st.text_area("Prompt Content:", value=prompt['content'], height=150, disabled=True)

        if st.button("🚀 Start Answer Generation (Demo)", type="primary", use_container_width=True, key="answer_btn"):
            self._handle_answer_questions_demo(account, site, prompt, questions_to_answer)

    def _handle_answer_questions_demo(self, account: Dict[str, Any], site: Dict[str, Any],
                                      prompt: Dict, questions_to_answer: int):
        """Handle answer questions action - demo version."""
        self.add_log(f"🚀 Starting demo answer generation for {account['username']} on {site['site_name']}...")
        self.add_log(f"📝 Using prompt: {prompt['prompt_name']}")
        
        st.session_state.generation_in_progress = True
        st.session_state.selected_action = "answer"

        # Simulate answer generation
        time.sleep(2)
        answers_generated = questions_to_answer

        self.add_log(f"🤖 Generated {answers_generated} demo answers")
        self.add_log(f"✅ Demo answers generated successfully")

        results = {
            "action": "answer_questions",
            "timestamp": datetime.now().isoformat(),
            "account": {
                "id": account['account_id'],
                "username": account['username']
            },
            "site": {
                "id": site['site_id'],
                "name": site['site_name']
            },
            "prompt_used": {
                "id": prompt['prompt_id'],
                "name": prompt['prompt_name'],
                "preview": prompt['content'][:100] + "..."
            },
            "questions_answered": answers_generated,
            "answers_generated": answers_generated,
            "execution_time_seconds": round(random.uniform(2, 4), 1),
            "status": "success",
            "is_demo": True
        }

        self.add_log(f"✅ Demo answer generation complete")
        st.session_state.generation_results = results
        st.session_state.generation_in_progress = False
        st.rerun()

    # ============================================================================
    # TAB 3: CREATE WORKFLOWS (MODULAR)
    # ============================================================================

    def _render_create_workflows_tab(self, account: Dict[str, Any], site: Dict[str, Any], prompt: Optional[Dict]):
        """Render create workflows tab - modular per site."""
        st.subheader("⚙️ Create Workflows from Questions")

        # Get creator info for this site
        creator_info = None
        if site['site_name'] in self.orchestrator.workflow_creators:
            creator_info = self.orchestrator.workflow_creators[site['site_name']].get_site_info()

        st.info(f"""
        **Account:** {account['username']}  
        **Survey Site:** {site['site_name']}  
        **Workflow Creator:** {creator_info.get('version', '1.0.0') if creator_info else 'Standard'}  
        **Template:** {creator_info.get('template_name', 'Default') if creator_info else 'Default'}
        **Prompt:** {prompt['prompt_name'] if prompt else 'None'}
        
        This site has a dedicated workflow creator with site-specific templates.
        """)

        if not prompt:
            st.warning("⚠️ Account has no prompt - workflows can still be created but may lack personalization")

        # Get questions for this account/site that haven't been used
        questions = self._get_unused_questions(account['account_id'], site['site_id'])
        
        if not questions:
            st.info("No unused questions available for this account/site. Extract questions first.")
            return

        st.success(f"✅ Found {len(questions)} unused questions")

        # Show question preview
        with st.expander("📋 Available Questions", expanded=False):
            for q in questions[:10]:
                st.markdown(f"- {q['question_text'][:100]}... ({q['question_type']})")
            if len(questions) > 10:
                st.caption(f"... and {len(questions) - 10} more")

        # Workflow creation options
        col1, col2 = st.columns(2)
        with col1:
            workflow_count = st.number_input(
                "Number of Workflows to Create:",
                min_value=1,
                max_value=min(10, len(questions)),
                value=min(3, len(questions)),
                key="create_workflow_count"
            )
        with col2:
            # Site-specific options
            st.info(f"Using template: {creator_info.get('template_name', 'Default') if creator_info else 'Default'}")

        # Advanced options
        with st.expander("🔧 Site-Specific Workflow Options", expanded=False):
            st.info(f"Options for {site['site_name']} workflows will appear here")
            include_click_elements = st.checkbox("Include Click Elements", value=True, key="create_include_click")
            include_input_elements = st.checkbox("Include Input Elements", value=True, key="create_include_input")

        if st.button("🚀 Create Workflows", type="primary", use_container_width=True, key="create_btn"):
            self._handle_create_workflows(account, site, prompt, workflow_count, questions, 
                                          include_click_elements, include_input_elements)

    def _handle_create_workflows(self, account: Dict[str, Any], site: Dict[str, Any],
                                 prompt: Optional[Dict], workflow_count: int, questions: List,
                                 include_click_elements: bool, include_input_elements: bool):
        """Handle create workflows action using orchestrator."""
        self.add_log(f"🚀 Creating workflows for {account['username']} on {site['site_name']}...")
        self.add_log(f"📊 Creating {workflow_count} workflows from {len(questions)} questions")
        
        st.session_state.generation_in_progress = True
        st.session_state.selected_action = "create"

        try:
            # Use orchestrator to create workflows
            results = self.orchestrator.create_workflows(
                account_id=account['account_id'],
                site_id=site['site_id'],
                questions=questions,
                prompt=prompt,
                site_name=site['site_name'],
                workflow_count=workflow_count,
                include_click_elements=include_click_elements,
                include_input_elements=include_input_elements
            )

            if results.get('success'):
                self.add_log(f"📦 Created {results.get('workflows_created', 0)} workflows")
                self.add_log(f"✅ Workflow creation complete")

                st.session_state.generation_results = {
                    "action": "create_workflows",
                    "timestamp": datetime.now().isoformat(),
                    "account": {
                        "id": account['account_id'],
                        "username": account['username']
                    },
                    "site": {
                        "id": site['site_id'],
                        "name": site['site_name']
                    },
                    "prompt_used": prompt['prompt_name'] if prompt else None,
                    "workflows_created": results.get('workflows_created', 0),
                    "workflows": results.get('workflows', []),
                    "inserted": results.get('inserted', 0),
                    "batch_id": results.get('batch_id'),
                    "execution_time_seconds": results.get('execution_time_seconds', 0),
                    "status": "success"
                }
            else:
                self.add_log(f"❌ Workflow creation failed: {results.get('error', 'Unknown error')}")
                st.session_state.generation_results = {
                    "action": "create_workflows",
                    "status": "failed",
                    "error": results.get('error', 'Unknown error'),
                    "timestamp": datetime.now().isoformat(),
                    "account": {
                        "id": account['account_id'],
                        "username": account['username']
                    },
                    "site": {
                        "id": site['site_id'],
                        "name": site['site_name']
                    }
                }

        except Exception as e:
            self.add_log(f"❌ Workflow creation error: {str(e)}")
            st.session_state.generation_results = {
                "action": "create_workflows",
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "account": {
                    "id": account['account_id'],
                    "username": account['username']
                },
                "site": {
                    "id": site['site_id'],
                    "name": site['site_name']
                }
            }

        finally:
            st.session_state.generation_in_progress = False
            st.rerun()

    # ============================================================================
    # TAB 4: DOWNLOAD WORKFLOWS (SIMPLE DEMO)
    # ============================================================================

    def _render_download_workflows_tab(self, account: Dict[str, Any], site: Dict[str, Any], prompt: Optional[Dict]):
        """Render download workflows tab - simple demo version."""
        st.subheader("📥 Download Workflows (Demo Version)")

        st.info(f"""
        **Account:** {account['username']}  
        **Survey Site:** {site['site_name']}  
        
        ⚠️ **This is a demo version** - downloads are simulated.
        In production, this would generate real workflow JSON files.
        """)

        # Get workflows for this account/site
        workflows = self._get_workflows_for_download(account['account_id'], site['site_id'])
        
        if not workflows:
            st.info("No workflows available for this account/site. Create workflows first.")
            return

        st.success(f"✅ Found {len(workflows)} workflows available")

        # Show workflow preview
        with st.expander("📋 Available Workflows", expanded=False):
            for wf in workflows[:5]:
                st.markdown(f"- **{wf['workflow_name']}** (Created: {wf['created_time'].strftime('%Y-%m-%d') if wf.get('created_time') else 'Unknown'})")
            if len(workflows) > 5:
                st.caption(f"... and {len(workflows) - 5} more")

        # Simple options
        col1, col2 = st.columns(2)
        with col1:
            include_manifest = st.checkbox("Include Manifest File", value=True, key="download_include_manifest")
        with col2:
            format_type = st.selectbox(
                "Download Format (demo):",
                options=["Single JSON", "ZIP Package"],
                key="download_format"
            )

        # Select which workflows to download (simplified - just take first few)
        workflows_to_include = st.slider(
            "Number of workflows to include (demo):",
            min_value=1,
            max_value=min(10, len(workflows)),
            value=min(5, len(workflows)),
            key="download_count"
        )

        if st.button("📦 Generate Download (Demo)", type="primary", use_container_width=True, key="download_btn"):
            self._handle_download_workflows_demo(account, site, workflows[:workflows_to_include], 
                                                include_manifest, format_type)

    def _handle_download_workflows_demo(self, account: Dict[str, Any], site: Dict[str, Any],
                                       workflows: List, include_manifest: bool, format_type: str):
        """Handle download workflows action - demo version."""
        self.add_log(f"🚀 Generating demo download package for {account['username']} on {site['site_name']}...")
        
        st.session_state.generation_in_progress = True
        st.session_state.selected_action = "download"

        # Simulate download preparation
        time.sleep(2)

        # Prepare demo download info
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{account['username']}_{site['site_name']}_workflows_{timestamp}.zip"

        self.add_log(f"📦 Prepared {len(workflows)} workflows for demo download")

        results = {
            "action": "download_workflows",
            "timestamp": datetime.now().isoformat(),
            "account": {
                "id": account['account_id'],
                "username": account['username']
            },
            "site": {
                "id": site['site_id'],
                "name": site['site_name']
            },
            "workflows_available": len(workflows),
            "workflows_included": len(workflows),
            "include_manifest": include_manifest,
            "format_type": format_type,
            "filename": filename,
            "execution_time_seconds": round(random.uniform(1, 3), 1),
            "status": "success",
            "is_demo": True,
            "download_url": f"/demo/download/{filename}"
        }

        # Add sample workflow names
        results["workflow_names"] = [wf['workflow_name'] for wf in workflows]

        self.add_log(f"✅ Demo download package ready")
        st.session_state.generation_results = results
        st.session_state.generation_in_progress = False
        st.rerun()

    # ============================================================================
    # HELPER METHODS
    # ============================================================================

    def _get_account_urls(self, account_id: int, site_id: int) -> List[Dict]:
        """Get URLs for account/site with used status."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT url_id, url, is_default, is_used, used_at, notes
                        FROM account_urls
                        WHERE account_id = %s AND site_id = %s
                        ORDER BY is_default DESC, created_at DESC
                    """, (account_id, site_id))
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error loading account URLs: {e}")
            return []

    def _mark_url_used(self, url_id: int):
        """Mark a URL as used."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE account_urls
                        SET is_used = TRUE, used_at = CURRENT_TIMESTAMP
                        WHERE url_id = %s
                    """, (url_id,))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error marking URL used: {e}")

    def _get_unused_questions(self, account_id: int, site_id: int) -> List[Dict]:
        """Get questions that haven't been used in workflows."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT q.question_id, q.question_text, q.question_type, 
                               q.click_element, q.options, q.required,
                               q.question_category, q.input_element, q.submit_element
                        FROM questions q
                        WHERE q.account_id = %s 
                          AND q.survey_site_id = %s
                          AND (q.used_in_workflow IS NULL OR q.used_in_workflow = FALSE)
                          AND q.is_active = TRUE
                        ORDER BY q.extracted_at DESC
                    """, (account_id, site_id))
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error loading unused questions: {e}")
            return []

    def _mark_question_used(self, question_id: int):
        """Mark a question as used in a workflow."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE questions
                        SET used_in_workflow = TRUE, used_at = CURRENT_TIMESTAMP
                        WHERE question_id = %s
                    """, (question_id,))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error marking question used: {e}")

    def _get_workflows_for_download(self, account_id: int, site_id: int) -> List[Dict]:
        """Get workflows for download."""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT workflow_id, workflow_name, workflow_data, created_time, question_id
                        FROM workflows
                        WHERE account_id = %s 
                          AND site_id = %s
                          AND is_active = TRUE
                        ORDER BY created_time DESC
                    """, (account_id, site_id))
                    return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error loading workflows: {e}")
            return []

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
        
        is_demo = results.get('is_demo', False)
        demo_badge = " (Demo)" if is_demo else ""
        
        st.subheader(f"✅ {action_titles.get(action, action.title())} Results{demo_badge}")

        if is_demo:
            st.info("⚠️ **This was a demo run** - no actual data was modified.")

        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            if action == 'extract':
                st.metric("Questions Extracted", results.get('questions_extracted', 0))
            elif action == 'answer':
                st.metric("Answers Generated", results.get('answers_generated', 0))
            elif action == 'create':
                st.metric("Workflows Created", results.get('workflows_created', 0))
            elif action == 'download':
                st.metric("Workflows Prepared", results.get('workflows_included', 0))

        with col2:
            st.metric("Account", results['account']['username'])

        with col3:
            st.metric("Survey Site", results['site']['name'])

        with col4:
            st.metric("Execution Time", f"{results.get('execution_time_seconds', 0)}s")

        st.markdown("---")

        # Detailed results
        if action == 'extract':
            col_left, col_right = st.columns(2)
            with col_left:
                st.markdown("**📋 Details**")
                st.json({
                    "account_id": results['account']['id'],
                    "site_id": results['site']['id'],
                    "url": results.get('url', {}).get('url', 'N/A'),
                    "batch_id": results.get('batch_id', 'N/A'),
                    "inserted": results.get('inserted', 0),
                    "timestamp": results['timestamp']
                })
            with col_right:
                st.markdown("**📊 Questions**")
                st.metric("Questions Found", results.get('questions_extracted', 0))

        elif action == 'answer':
            col_left, col_right = st.columns(2)
            with col_left:
                st.markdown("**📋 Details**")
                st.json({
                    "account_id": results['account']['id'],
                    "site_id": results['site']['id'],
                    "questions_answered": results.get('questions_answered', 0),
                    "is_demo": results.get('is_demo', True)
                })
            with col_right:
                if results.get('prompt_used'):
                    st.markdown("**📝 Prompt Used**")
                    if isinstance(results['prompt_used'], dict):
                        st.info(results['prompt_used']['name'])
                        st.caption(results['prompt_used']['preview'])

        elif action == 'create' and 'workflows' in results:
            st.markdown("**📦 Created Workflows**")
            
            for i, wf in enumerate(results['workflows']):
                with st.expander(f"📋 {wf.get('name', wf.get('workflow_name', f'Workflow {i+1}'))}", expanded=(i == 0)):
                    col1, col2 = st.columns(2)
                    with col1:
                        wf_id = wf.get('id', wf.get('workflow_id', 'N/A'))
                        st.metric("Workflow ID", str(wf_id)[:8] + "...")
                    with col2:
                        st.metric("Question ID", wf.get('question_id', 'N/A'))
                    
                    if 'question_text' in wf:
                        st.markdown(f"**Question:** {wf['question_text'][:200]}...")

        elif action == 'download':
            st.markdown("**📦 Download Package Info**")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Filename", results.get('filename', 'N/A'))
            with col2:
                st.metric("Format", results.get('format_type', 'N/A'))
            
            if 'workflow_names' in results:
                st.markdown("**Workflows Included:**")
                for name in results['workflow_names']:
                    st.markdown(f"- {name}")

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
                        SELECT site_id, site_name, description, created_at, is_active
                        FROM survey_sites
                        ORDER BY site_name
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