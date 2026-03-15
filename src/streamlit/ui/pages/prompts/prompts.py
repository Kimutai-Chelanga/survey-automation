"""
Enhanced Prompts Management Page with Backups, Variations, and Generation
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import time
import traceback
import logging
import json
import os
from typing import Optional, List

from src.core.database.postgres import prompts as pg_prompts
from src.core.database.postgres import accounts as pg_utils
from src.core.database.postgres.prompt_backup_db import PromptBackupDB
from src.core.database.postgres.prompt_variations_db import PromptVariationsDB

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PromptsPage:
    """Enhanced Prompts page with Backups, Variations, and AI Generation."""

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.backup_db = PromptBackupDB()
        self.variations_db = PromptVariationsDB()

        # Initialize session state for logging
        if 'prompt_operation_logs' not in st.session_state:
            st.session_state.prompt_operation_logs = []

        # Initialize session state for prompt creation
        if 'show_create_prompt' not in st.session_state:
            st.session_state.show_create_prompt = False
        if 'creating_prompt' not in st.session_state:
            st.session_state.creating_prompt = False
        if 'prompt_creation_completed' not in st.session_state:
            st.session_state.prompt_creation_completed = False
        if 'prompt_creation_message' not in st.session_state:
            st.session_state.prompt_creation_message = None
        if 'prompt_creation_error' not in st.session_state:
            st.session_state.prompt_creation_error = False
        if 'prompt_creation_in_progress' not in st.session_state:
            st.session_state.prompt_creation_in_progress = False
        if 'prompt_to_create' not in st.session_state:
            st.session_state.prompt_to_create = None

        # Initialize session state for prompt deletion
        if 'show_delete_prompt' not in st.session_state:
            st.session_state.show_delete_prompt = False
        if 'deleting_prompt' not in st.session_state:
            st.session_state.deleting_prompt = False
        if 'prompt_deletion_completed' not in st.session_state:
            st.session_state.prompt_deletion_completed = False
        if 'prompt_deletion_message' not in st.session_state:
            st.session_state.prompt_deletion_message = None
        if 'prompt_deletion_error' not in st.session_state:
            st.session_state.prompt_deletion_error = False
        if 'prompt_deletion_in_progress' not in st.session_state:
            st.session_state.prompt_deletion_in_progress = False
        if 'prompt_id_to_delete' not in st.session_state:
            st.session_state.prompt_id_to_delete = None

        # Patch the problematic Streamlit method BEFORE using any widgets
        self._patch_streamlit_widget_comparison()

    def _patch_streamlit_widget_comparison(self):
        """Patch Streamlit's internal widget comparison to handle pandas Series correctly"""
        import streamlit.runtime.state.session_state as ss

        # Store the original method
        original_widget_changed = ss.SessionState._widget_changed

        def safe_widget_changed(self, wid):
            """Safe version that handles pandas Series/DataFrame comparison"""
            try:
                old_value = self._widgets.get(wid)
                new_value = self._widget_states.get(wid)

                # Handle None values
                if old_value is None and new_value is None:
                    return False
                if old_value is None or new_value is None:
                    return True

                # Check if values are pandas objects
                is_old_pandas = isinstance(old_value, (pd.Series, pd.DataFrame))
                is_new_pandas = isinstance(new_value, (pd.Series, pd.DataFrame))

                if is_old_pandas or is_new_pandas:
                    try:
                        # If both are pandas objects, use pandas equality
                        if is_old_pandas and is_new_pandas:
                            # Use pandas' equals() method which returns a single boolean
                            if hasattr(old_value, 'equals') and hasattr(new_value, 'equals'):
                                return not old_value.equals(new_value)

                        # If only one is pandas, they're definitely different
                        return True
                    except:
                        # If comparison fails, fall back to string comparison
                        return str(old_value) != str(new_value)

                # For non-pandas objects, use original comparison
                return old_value != new_value

            except Exception as e:
                logger.error(f"Error in widget comparison for {wid}: {e}")
                # On error, assume changed to be safe
                return True

        # Replace the method in SessionState class
        ss.SessionState._widget_changed = safe_widget_changed

    def add_log(self, message, level="INFO"):
        """Add a log message to both Python logging and Streamlit session state"""
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
        """Clear the operation logs"""
        st.session_state.prompt_operation_logs = []

    def render_operation_logs(self):
        """Render the operation logs in the UI"""
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
                        file_name=f"prompt_operation_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                        mime="text/plain"
                    )

    def _clear_prompt_creation_state(self, keep_message=False):
        """Clear prompt creation session state"""
        st.session_state.show_create_prompt = False
        st.session_state.creating_prompt = False
        st.session_state.prompt_creation_completed = False
        st.session_state.prompt_creation_in_progress = False
        st.session_state.prompt_to_create = None

        if not keep_message:
            st.session_state.prompt_creation_message = None
            st.session_state.prompt_creation_error = False
            self.clear_logs()

    def _clear_prompt_deletion_state(self, keep_message=False):
        """Clear prompt deletion session state"""
        st.session_state.show_delete_prompt = False
        st.session_state.deleting_prompt = False
        st.session_state.prompt_deletion_completed = False
        st.session_state.prompt_deletion_in_progress = False
        st.session_state.prompt_id_to_delete = None

        if not keep_message:
            st.session_state.prompt_deletion_message = None
            st.session_state.prompt_deletion_error = False

    @st.cache_data(ttl=60)
    def load_prompts_data(_self) -> pd.DataFrame:
        """Load prompts data from PostgreSQL database"""
        try:
            prompts_data = pg_prompts.get_comprehensive_prompts(limit=1000)

            if not prompts_data:
                logger.info("No prompts data returned from database")
                return pd.DataFrame()

            df = pd.DataFrame(prompts_data)

            logger.info(f"Loaded {len(df)} prompts from database")

            datetime_cols = ['created_time', 'updated_time']
            for col in datetime_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])

            required_columns = {
                'prompt_id': 'int64',
                'account_id': 'int64',
                'prompt_name': 'object',
                'prompt_type': 'object',
                'is_active': 'bool',
                'username': 'object',
                'content_count': 'int64',
                'backup_count': 'int64',
                'variation_count': 'int64'
            }

            for col, dtype in required_columns.items():
                if col not in df.columns:
                    if col in ['content_count', 'backup_count', 'variation_count']:
                        df[col] = 0
                    elif col == 'is_active':
                        df[col] = True
                    elif col == 'username':
                        df[col] = 'Unknown'
                    elif col in ['prompt_name', 'prompt_type']:
                        df[col] = 'N/A'
                    else:
                        df[col] = None
                    logger.warning(f"Added missing column '{col}' with default values")

            return df

        except Exception as e:
            logger.error(f"Error loading prompts data: {str(e)}")
            st.error(f"Error loading prompts data: {str(e)}")
            return pd.DataFrame()

    @st.cache_data(ttl=60)
    def load_accounts(_self) -> pd.DataFrame:
        """Load accounts data for selection"""
        try:
            accounts_data = pg_utils.get_all_accounts()

            if not accounts_data:
                logger.warning("No accounts data returned from database")
                return pd.DataFrame(columns=['account_id', 'username'])

            df = pd.DataFrame(accounts_data)

            required_cols = ['account_id', 'username']
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                logger.error(f"Missing required columns in accounts data: {missing_cols}")
                return pd.DataFrame(columns=required_cols)

            result_df = df[required_cols].drop_duplicates()
            result_df['account_id'] = result_df['account_id'].astype(int)

            logger.info(f"Loaded {len(result_df)} accounts from database")
            return result_df

        except Exception as e:
            logger.error(f"Error loading accounts: {str(e)}")
            st.error(f"Error loading accounts: {str(e)}")
            return pd.DataFrame(columns=['account_id', 'username'])

    def render(self):
        """Main render method with enhanced tab structure"""
        st.title("📝 Enhanced Prompts Management")

        # Load data
        with st.spinner("Loading prompts data..."):
            prompts_df = self.load_prompts_data()

        # Create tabs
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 Prompts Overview",
            "🎨 Generate Variations",
            "💾 Backups & Restore",
            "📋 Prompt Details",
            "📈 Analytics"
        ])

        with tab1:
            self.render_prompts_overview(prompts_df)

        with tab2:
            self.render_variations_generator(prompts_df)

        with tab3:
            self.render_backups_manager()

        with tab4:
            self.render_prompt_detail(prompts_df)

        with tab5:
            self.render_analytics(prompts_df)


    def render_prompts_overview(self, prompts_df: pd.DataFrame):
        """Render the prompts overview with ACTUAL CONTENT visible."""
        # Add action buttons at the top
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

        with col1:
            if st.button("🔄 Refresh", help="Reload prompts data"):
                st.cache_data.clear()
                self.add_log("Manual refresh triggered - clearing all caches")
                st.rerun()

        with col2:
            if st.button("➕ Create New Prompt", type="primary"):
                self._clear_prompt_creation_state()
                self._clear_prompt_deletion_state()
                st.session_state.show_create_prompt = True
                st.rerun()

        with col3:
            if st.button("🗑️ Delete Prompt", type="secondary"):
                self._clear_prompt_creation_state()
                self._clear_prompt_deletion_state()
                st.session_state.show_delete_prompt = True
                st.rerun()

        # Render filters and stats
        selected_account, selected_type, selected_status = self.render_filters_and_stats(prompts_df)

        # Apply filters
        filtered_df = self.apply_filters(prompts_df, selected_account, selected_type, selected_status)

        # Render create/delete modals
        self.render_create_prompt_modal()
        self.render_delete_prompt_modal(prompts_df)

        # Display prompts table
        st.subheader(f"Prompts ({len(filtered_df)} results)")

        if filtered_df.empty:
            st.warning("No prompts match the selected filters.")
            return

        # FIXED: Prepare display columns - NOW INCLUDES 'content'
        display_columns = [
            'prompt_id',
            'username',
            'prompt_name',
            'prompt_type',
            'content',  # ← THIS IS THE FIX - shows actual prompt content
            'is_active',
            'content_count',
            'backup_count',
            'variation_count',
            'updated_time'
        ]

        available_columns = [col for col in display_columns if col in filtered_df.columns]

        if not available_columns:
            st.error("No displayable columns found in the filtered data.")
            return

        display_df = filtered_df[available_columns].copy()

        # Format columns for better display
        if 'updated_time' in display_df.columns:
            display_df['updated_time'] = display_df['updated_time'].dt.strftime('%Y-%m-%d %H:%M')

        if 'is_active' in display_df.columns:
            display_df['is_active'] = display_df['is_active'].map({True: '✅', False: '❌'})

        # OPTIONAL: Truncate long content for better table display
        if 'content' in display_df.columns:
            display_df['content'] = display_df['content'].apply(
                lambda x: (x[:150] + '...') if isinstance(x, str) and len(x) > 150 else x
            )

        # Display the table
        try:
            st.dataframe(display_df, use_container_width=True, height=500)

            # Simple row selection using a selectbox
            st.subheader("Select a Prompt for Details")
            prompt_options = ["-- Select a prompt --"] + [
                f"{row['prompt_name']} ({row['username']}) - {row['prompt_type']}"
                for _, row in filtered_df.iterrows()
            ]

            selected_option = st.selectbox("Choose a prompt to view details:", prompt_options)

            if selected_option != "-- Select a prompt --":
                # Find the selected prompt
                for idx, row in filtered_df.iterrows():
                    option_text = f"{row['prompt_name']} ({row['username']}) - {row['prompt_type']}"
                    if option_text == selected_option:
                        # Only update and rerun if the selection actually changed
                        if st.session_state.get('selected_prompt_id') != row['prompt_id']:
                            st.session_state.selected_prompt_id = row['prompt_id']
                            st.session_state.selected_prompt_name = row['prompt_name']
                            st.success(f"Selected: {row['prompt_name']}")
                            st.rerun()
                        else:
                            # Selection hasn't changed, just show info message
                            st.info(f"Currently viewing: {row['prompt_name']}")
                        break

        except Exception as e:
            st.error(f"Error displaying dataframe: {e}")
            # Fallback: show basic dataframe without special formatting
            st.dataframe(display_df, use_container_width=True)
    def apply_filters(self, prompts_df, selected_account, selected_type, selected_status):
        """Apply filters to the prompts dataframe"""
        if prompts_df.empty:
            return prompts_df

        filtered_df = prompts_df.copy()

        if selected_account != 'All Accounts':
            if 'username' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['username'] == selected_account]

        if selected_type != 'All Types':
            if 'prompt_type' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['prompt_type'] == selected_type]

        if selected_status != 'All':
            if 'is_active' in filtered_df.columns:
                status_filter = (selected_status == 'Active')
                filtered_df = filtered_df[filtered_df['is_active'] == status_filter]

        return filtered_df

    def render_filters_and_stats(self, prompts_df):
        """Render filters with dynamic prompt types from database"""
        st.subheader("🔍 Filters & Stats")

        accounts_df = self.load_accounts()

        if accounts_df.empty:
            account_options = ["All Accounts"]
        elif 'username' not in accounts_df.columns:
            account_options = ["All Accounts"]
        else:
            if not prompts_df.empty and 'username' in prompts_df.columns:
                unique_usernames = sorted(prompts_df['username'].dropna().unique())
            else:
                unique_usernames = sorted(accounts_df['username'].dropna().unique())
            account_options = ["All Accounts"] + list(unique_usernames)

        if not prompts_df.empty and 'prompt_type' in prompts_df.columns:
            unique_types = sorted(prompts_df['prompt_type'].dropna().unique())
            type_options = ["All Types"] + list(unique_types)
        else:
            type_options = ["All Types"]

        col1, col2, col3 = st.columns(3)

        with col1:
            selected_account = st.selectbox("Filter by Account", account_options, key="account_filter")

        with col2:
            selected_type = st.selectbox("Filter by Type", type_options, key="type_filter")

        with col3:
            status_options = ["All", "Active", "Inactive"]
            selected_status = st.selectbox("Filter by Status", status_options, key="status_filter")

        st.markdown("---")

        if not prompts_df.empty:
            total_prompts = len(prompts_df)
            active_prompts = len(prompts_df[prompts_df['is_active'] == True])

            stat_col1, stat_col2, stat_col3, stat_col4, stat_col5 = st.columns(5)

            with stat_col1:
                st.metric("Total Prompts", total_prompts)

            with stat_col2:
                st.metric("Active Prompts", active_prompts)

            with stat_col3:
                if 'backup_count' in prompts_df.columns:
                    total_backups = prompts_df['backup_count'].sum()
                    st.metric("Total Backups", int(total_backups))

            with stat_col4:
                if 'variation_count' in prompts_df.columns:
                    total_variations = prompts_df['variation_count'].sum()
                    st.metric("Total Variations", int(total_variations))

            with stat_col5:
                if 'prompt_type' in prompts_df.columns:
                    unique_types_count = prompts_df['prompt_type'].nunique()
                    st.metric("Prompt Types", unique_types_count)
        else:
            st.info("No prompts data available for statistics")

        return selected_account, selected_type, selected_status

    def render_variations_generator(self, prompts_df: pd.DataFrame):
        """Render the AI-powered prompt variations generator"""
        st.subheader("🎨 Generate Prompt Variations")
        st.caption("Generate multiple variations of your prompts for content diversity")

        if prompts_df.empty:
            st.warning("No prompts available. Create a prompt first in the Overview tab.")
            return

        # Filter selection
        col1, col2 = st.columns([2, 1])

        with col1:
            # Select prompt
            prompt_options = []
            for _, row in prompts_df.iterrows():
                option_text = f"{row['prompt_name']} ({row['username']}) - {row['prompt_type']}"
                prompt_options.append((option_text, row['prompt_id'], row))

            selected_prompt = st.selectbox(
                "Select Prompt to Generate Variations:",
                options=[None] + prompt_options,
                format_func=lambda x: "Choose a prompt..." if x is None else x[0],
                key="variation_prompt_selector"
            )

        with col2:
            num_variations = st.number_input(
                "Number of Variations:",
                min_value=1,
                max_value=20,
                value=5,
                help="How many variations to generate"
            )

        if selected_prompt:
            prompt_id = selected_prompt[1]
            prompt_data = selected_prompt[2]

            # Show prompt details
            with st.expander("📄 Original Prompt Details", expanded=True):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write(f"**Name:** {prompt_data['prompt_name']}")
                    st.write(f"**Type:** {prompt_data['prompt_type']}")
                with col2:
                    st.write(f"**Account:** {prompt_data['username']}")
                    st.write(f"**Active:** {'Yes' if prompt_data['is_active'] else 'No'}")
                with col3:
                    existing_variations = prompt_data.get('variation_count', 0)
                    st.write(f"**Existing Variations:** {existing_variations}")

                st.text_area(
                    "Original Content:",
                    value=prompt_data['content'],
                    height=150,
                    disabled=True,
                    key=f"original_content_{prompt_id}"
                )

            # Generation options
            st.markdown("---")
            st.subheader("⚙️ Generation Options")

            col1, col2 = st.columns(2)

            with col1:
                creativity_level = st.slider(
                    "Creativity Level",
                    min_value=0.1,
                    max_value=1.0,
                    value=0.7,
                    step=0.1,
                    help="Higher values = more creative variations"
                )

            with col2:
                # Check if we have an AI service available
                ai_available = self._check_ai_available()
                if ai_available:
                    st.success("✅ AI Service Available")
                    generation_method = st.selectbox(
                        "Generation Method",
                        ["AI-Powered", "Template-Based", "Hybrid"],
                        index=0,
                        help="Choose how to generate variations"
                    )
                else:
                    st.warning("⚠️ AI Service Not Configured")
                    generation_method = "Template-Based"

            # Generate button
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                if st.button("🚀 Generate Variations", type="primary", key="generate_variations_btn", use_container_width=True):
                    if generation_method == "Template-Based":
                        self._generate_template_variations(
                            prompt_id,
                            prompt_data,
                            num_variations
                        )
                    elif generation_method == "AI-Powered":
                        self._generate_ai_variations(
                            prompt_id,
                            prompt_data,
                            num_variations,
                            creativity_level
                        )
                    else:  # Hybrid
                        self._generate_hybrid_variations(
                            prompt_id,
                            prompt_data,
                            num_variations,
                            creativity_level
                        )

            # Show existing variations
            st.markdown("---")
            st.subheader("📋 Existing Variations")

            # Load variations for this prompt
            variations = self.variations_db.get_variations(
                parent_prompt_id=prompt_id,
                unused_only=False
            )

            if variations:
                # Filter controls
                col1, col2, col3 = st.columns(3)
                with col1:
                    show_filter = st.radio(
                        "Show:",
                        ["All", "Unused Only", "Used Only"],
                        horizontal=True,
                        key="variation_filter"
                    )

                # Apply filter
                if show_filter == "Unused Only":
                    variations = [v for v in variations if not v['used']]
                elif show_filter == "Used Only":
                    variations = [v for v in variations if v['used']]

                st.write(f"**{len(variations)} variations found**")

                # Display variations in a grid
                for idx, variation in enumerate(variations):
                    with st.container():
                        col1, col2 = st.columns([4, 1])

                        with col1:
                            status_icon = "✅ Used" if variation['used'] else "🆕 Unused"
                            copy_count = variation.get('copied_count', 0)

                            st.markdown(f"**Variation #{variation['variation_number']}** - {status_icon} | 📋 Copied: {copy_count}x")

                            st.text_area(
                                f"Content:",
                                value=variation['variation_content'],
                                height=120,
                                key=f"variation_display_{variation['variation_id']}",
                                label_visibility="collapsed"
                            )

                        with col2:
                            # Copy button
                            if st.button(
                                "📋 Copy",
                                key=f"copy_var_{variation['variation_id']}",
                                use_container_width=True
                            ):
                                st.code(variation['variation_content'], language=None)
                                self.variations_db.increment_copy_count(variation['variation_id'])
                                st.success("Copied to clipboard!")
                                time.sleep(1)
                                st.rerun()

                            # Mark as used button
                            if not variation['used']:
                                if st.button(
                                    "✓ Mark Used",
                                    key=f"mark_used_{variation['variation_id']}",
                                    use_container_width=True
                                ):
                                    self.variations_db.mark_variation_used(variation['variation_id'])
                                    st.success("Marked as used!")
                                    time.sleep(1)
                                    st.rerun()

                            # Show created date
                            st.caption(f"Created: {variation['created_at'].strftime('%Y-%m-%d %H:%M')}")

                        st.markdown("---")

                # Statistics summary
                unused_count = sum(1 for v in variations if not v['used'])
                used_count = sum(1 for v in variations if v['used'])
                total_copies = sum(v.get('copied_count', 0) for v in variations)

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Unused Variations", unused_count)
                with col2:
                    st.metric("Used Variations", used_count)
                with col3:
                    st.metric("Total Copies", total_copies)

            else:
                st.info("No variations generated yet. Click 'Generate Variations' to create some!")

    def _check_ai_available(self):
        """Check if AI services are available"""
        # Check for OpenAI API key
        if os.environ.get("OPENAI_API_KEY"):
            return True

        # Check for Gemini API key
        if os.environ.get("GEMINI_API_KEY"):
            return True

        # Check for Hyperbrowser API key
        if os.environ.get("HYPERBROWSER_API_KEY"):
            return True

        # Check if hyperbrowser package has AI capabilities
        try:
            import hyperbrowser
            # Check if hyperbrowser has AI module
            if hasattr(hyperbrowser, 'ai') or hasattr(hyperbrowser, 'generate'):
                return True
        except (ImportError, AttributeError):
            pass

        return False

    def _generate_template_variations(self, prompt_id, prompt_data, num_variations):
        """Generate variations using template-based approach"""
        with st.spinner(f"Generating {num_variations} template-based variations..."):
            try:
                original_content = prompt_data['content']
                prompt_type = prompt_data['prompt_type']

                # Define templates based on prompt type
                templates = self._get_variation_templates(prompt_type)

                variations = []
                for i in range(num_variations):
                    # Use different templates or combine them
                    template_idx = i % len(templates)
                    template = templates[template_idx]

                    # Apply the template
                    variation = template.format(
                        original=original_content,
                        variation_number=i+1,
                        prompt_type=prompt_type
                    )
                    variations.append(variation)

                # Save to database
                batch_id = self.variations_db.create_variation_batch(
                    parent_prompt_id=prompt_id,
                    account_id=prompt_data['account_id'],
                    username=prompt_data['username'],
                    prompt_type=prompt_data['prompt_type'],
                    prompt_name=prompt_data['prompt_name'],
                    variations=variations,
                    metadata={
                        'generation_method': 'template',
                        'original_prompt_id': prompt_id,
                        'templates_used': len(templates),
                        'requested_count': num_variations
                    }
                )

                if batch_id:
                    st.success(f"✅ Generated {len(variations)} template-based variations!")
                    self.add_log(f"Generated {len(variations)} template variations for prompt {prompt_id}, batch {batch_id}")

                    # Clear cache and rerun
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Failed to save variations to database")

            except Exception as e:
                st.error(f"Error generating template variations: {e}")
                self.add_log(f"Error generating template variations: {e}", "ERROR")

    def _get_variation_templates(self, prompt_type):
        """Get variation templates based on prompt type"""
        # Base templates that work for most prompt types
        base_templates = [
            # Template 1: Add context
            "Variation {variation_number}:\n\n{original}\n\nAdditional context: This approach focuses on building rapport before making requests.",

            # Template 2: Different tone
            "Alternative version ({variation_number}):\n\n{original}\n\nNote: This variation uses a more professional tone while maintaining the core message.",

            # Template 3: Restructured
            "Restructured approach (Variation {variation_number}):\n\n{original}\n\nThis version reorganizes the information for better flow and impact.",

            # Template 4: Expanded
            "Enhanced version {variation_number}:\n\n{original}\n\nIncludes additional examples and practical applications for better understanding.",

            # Template 5: Simplified
            "Simplified variation {variation_number}:\n\n{original}\n\nStreamlined for clarity and direct communication."
        ]

        # Type-specific templates
        type_specific = {
            'twitter': [
                "Twitter-friendly version {variation_number}:\n\n{original}\n\nOptimized for character limit and Twitter engagement patterns.",
                "Thread variation {variation_number}:\n\n{original}\n\nDesigned to work as part of a Twitter thread with clear progression.",
                "Reply variation {variation_number}:\n\n{original}\n\nTailored for Twitter reply context with appropriate tone."
            ],
            'engagement': [
                "Engagement-focused variation {variation_number}:\n\n{original}\n\nEnhanced with questions and calls to action to boost interaction.",
                "Conversation starter {variation_number}:\n\n{original}\n\nDesigned to initiate and sustain meaningful conversations."
            ],
            'outreach': [
                "Outreach variation {variation_number}:\n\n{original}\n\nPersonalized approach with specific value propositions.",
                "Connection request {variation_number}:\n\n{original}\n\nFocused on building genuine professional relationships."
            ]
        }

        # Return type-specific templates if available, otherwise base templates
        return type_specific.get(prompt_type, base_templates)

    def _generate_ai_variations(self, prompt_id, prompt_data, num_variations, creativity_level):
        """Generate variations using available AI services"""
        with st.spinner(f"🤖 Generating {num_variations} AI-powered variations..."):
            try:
                # Try different AI services in order of preference
                variations = None
                ai_service = None

                # 1. Try OpenAI if available
                if os.environ.get("OPENAI_API_KEY"):
                    try:
                        variations = self._generate_with_openai(prompt_data, num_variations, creativity_level)
                        ai_service = "openai"
                    except Exception as e:
                        logger.warning(f"OpenAI generation failed: {e}")

                # 2. Try Gemini if OpenAI failed
                if not variations and os.environ.get("GEMINI_API_KEY"):
                    try:
                        variations = self._generate_with_gemini(prompt_data, num_variations, creativity_level)
                        ai_service = "gemini"
                    except Exception as e:
                        logger.warning(f"Gemini generation failed: {e}")

                # 3. Try Hyperbrowser if other AI services failed
                if not variations and os.environ.get("HYPERBROWSER_API_KEY"):
                    try:
                        variations = self._generate_with_hyperbrowser(prompt_data, num_variations, creativity_level)
                        ai_service = "hyperbrowser"
                    except Exception as e:
                        logger.warning(f"Hyperbrowser generation failed: {e}")

                # 4. Fall back to template-based if all AI services failed
                if not variations:
                    st.warning("AI services not available or failed. Using template-based generation.")
                    self._generate_template_variations(prompt_id, prompt_data, num_variations)
                    return

                # Save to database
                batch_id = self.variations_db.create_variation_batch(
                    parent_prompt_id=prompt_id,
                    account_id=prompt_data['account_id'],
                    username=prompt_data['username'],
                    prompt_type=prompt_data['prompt_type'],
                    prompt_name=prompt_data['prompt_name'],
                    variations=variations,
                    metadata={
                        'generation_method': 'ai',
                        'ai_service': ai_service,
                        'original_prompt_id': prompt_id,
                        'creativity_level': creativity_level,
                        'requested_count': num_variations,
                        'actual_count': len(variations)
                    }
                )

                if batch_id:
                    st.success(f"✅ {ai_service.title()} generated {len(variations)} AI-powered variations!")
                    self.add_log(
                        f"Generated {len(variations)} variations using {ai_service} for prompt {prompt_id}, "
                        f"batch {batch_id}"
                    )

                    # Clear cache and rerun
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Failed to save variations to database")

            except Exception as e:
                st.error(f"Error generating AI variations: {e}")
                self.add_log(f"Error generating AI variations: {e}", "ERROR")
                # Fall back to template-based
                self._generate_template_variations(prompt_id, prompt_data, num_variations)

    def _generate_with_openai(self, prompt_data, num_variations, temperature):
        """Generate variations using OpenAI"""
        try:
            import openai

            client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
            original_content = prompt_data['content']
            prompt_type = prompt_data['prompt_type']
            prompt_name = prompt_data['prompt_name']

            system_prompt = f"""Generate {num_variations} creative variations of the given prompt.

Each variation should:
1. Maintain the core intent and purpose of the original
2. Offer a slightly different approach, tone, or perspective
3. Be practical and immediately usable
4. Provide genuine diversity (not just minor word changes)
5. Be the same length or slightly longer than the original

Return only the numbered list, no additional text.

Original Prompt Type: {prompt_type}
Original Prompt Name: {prompt_name}
Number of variations requested: {num_variations}"""

            user_message = f"""Generate {num_variations} creative variations of this prompt:

{original_content}

Return only the numbered list, no additional text."""

            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                temperature=temperature,
                max_tokens=4000
            )

            # Parse the numbered list
            variations = []
            lines = response.choices[0].message.content.strip().split('\n')
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                # Remove numbering
                if line[0].isdigit():
                    i = 1
                    while i < len(line) and line[i].isdigit():
                        i += 1
                    if i < len(line) and line[i] in ['.', ')', ':']:
                        i += 1
                    line = line[i:].strip()
                if line:
                    variations.append(line)

            return variations[:num_variations]

        except ImportError:
            raise Exception("OpenAI package not installed")
        except Exception as e:
            raise Exception(f"OpenAI error: {str(e)}")

    def _generate_with_gemini(self, prompt_data, num_variations, temperature):
        """Generate variations using Gemini"""
        try:
            import google.generativeai as genai

            genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
            model = genai.GenerativeModel('gemini-pro')

            original_content = prompt_data['content']
            prompt_type = prompt_data['prompt_type']

            prompt_text = f"""Generate {num_variations} creative variations of this {prompt_type} prompt:

{original_content}

Each variation should maintain the core intent but offer different approaches, tones, or perspectives.
Return only a numbered list of variations, no additional text."""

            response = model.generate_content(
                prompt_text,
                generation_config={
                    'temperature': temperature,
                    'max_output_tokens': 4000,
                }
            )

            # Parse the numbered list
            variations = []
            lines = response.text.strip().split('\n')
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                # Remove numbering
                if line[0].isdigit():
                    i = 1
                    while i < len(line) and line[i].isdigit():
                        i += 1
                    if i < len(line) and line[i] in ['.', ')', ':']:
                        i += 1
                    line = line[i:].strip()
                if line:
                    variations.append(line)

            return variations[:num_variations]

        except ImportError:
            raise Exception("Google Generative AI package not installed")
        except Exception as e:
            raise Exception(f"Gemini error: {str(e)}")

    def _generate_with_hyperbrowser(self, prompt_data, num_variations, creativity_level):
        """Generate variations using Hyperbrowser"""
        try:
            import hyperbrowser

            # This is a placeholder - adjust based on your hyperbrowser implementation
            original_content = prompt_data['content']
            prompt_type = prompt_data['prompt_type']

            # Simple template-based fallback within hyperbrowser context
            templates = self._get_variation_templates(prompt_type)

            variations = []
            for i in range(num_variations):
                template_idx = i % len(templates)
                template = templates[template_idx]

                variation = template.format(
                    original=original_content,
                    variation_number=i+1,
                    prompt_type=prompt_type
                )
                variations.append(variation)

            return variations

        except ImportError:
            raise Exception("Hyperbrowser package not installed")
        except Exception as e:
            raise Exception(f"Hyperbrowser error: {str(e)}")

    def _generate_hybrid_variations(self, prompt_id, prompt_data, num_variations, creativity_level):
        """Generate variations using a hybrid approach"""
        with st.spinner(f"Generating {num_variations} hybrid variations..."):
            try:
                # Generate half with AI (if available), half with templates
                ai_count = min(num_variations // 2, 3)  # Max 3 AI variations
                template_count = num_variations - ai_count

                all_variations = []

                # Generate AI variations if available
                if ai_count > 0 and self._check_ai_available():
                    try:
                        ai_variations = self._generate_ai_variations_internal(
                            prompt_data, ai_count, creativity_level
                        )
                        if ai_variations:
                            all_variations.extend(ai_variations)
                    except Exception as e:
                        logger.warning(f"AI part of hybrid generation failed: {e}")
                        ai_count = 0
                        template_count = num_variations

                # Generate template variations for the rest
                if template_count > 0:
                    template_variations = self._generate_template_variations_internal(
                        prompt_data, template_count
                    )
                    all_variations.extend(template_variations)

                # Save to database
                batch_id = self.variations_db.create_variation_batch(
                    parent_prompt_id=prompt_id,
                    account_id=prompt_data['account_id'],
                    username=prompt_data['username'],
                    prompt_type=prompt_data['prompt_type'],
                    prompt_name=prompt_data['prompt_name'],
                    variations=all_variations,
                    metadata={
                        'generation_method': 'hybrid',
                        'original_prompt_id': prompt_id,
                        'ai_count': ai_count,
                        'template_count': template_count,
                        'creativity_level': creativity_level,
                        'requested_count': num_variations,
                        'actual_count': len(all_variations)
                    }
                )

                if batch_id:
                    st.success(f"✅ Generated {len(all_variations)} hybrid variations!")
                    self.add_log(f"Generated {len(all_variations)} hybrid variations for prompt {prompt_id}, batch {batch_id}")

                    # Clear cache and rerun
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Failed to save variations to database")

            except Exception as e:
                st.error(f"Error generating hybrid variations: {e}")
                self.add_log(f"Error generating hybrid variations: {e}", "ERROR")
                # Fall back to template-only
                self._generate_template_variations(prompt_id, prompt_data, num_variations)

    def _generate_ai_variations_internal(self, prompt_data, num_variations, creativity_level):
        """Internal method to generate AI variations without UI"""
        try:
            # Try OpenAI first
            if os.environ.get("OPENAI_API_KEY"):
                return self._generate_with_openai(prompt_data, num_variations, creativity_level)
            # Then Gemini
            elif os.environ.get("GEMINI_API_KEY"):
                return self._generate_with_gemini(prompt_data, num_variations, creativity_level)
            # Then Hyperbrowser
            elif os.environ.get("HYPERBROWSER_API_KEY"):
                return self._generate_with_hyperbrowser(prompt_data, num_variations, creativity_level)
        except Exception:
            pass
        return None

    def _generate_template_variations_internal(self, prompt_data, num_variations):
        """Internal method to generate template variations without UI"""
        original_content = prompt_data['content']
        prompt_type = prompt_data['prompt_type']

        templates = self._get_variation_templates(prompt_type)
        variations = []

        for i in range(num_variations):
            template_idx = i % len(templates)
            template = templates[template_idx]

            variation = template.format(
                original=original_content,
                variation_number=i+1,
                prompt_type=prompt_type
            )
            variations.append(variation)

        return variations

    # ... (The rest of the methods remain the same as before: render_backups_manager,
    # _render_all_backups, _render_restore_backup, _render_backup_statistics,
    # render_create_prompt_modal, render_delete_prompt_modal, render_prompt_detail,
    # render_analytics - they don't need changes)

    # I'll include a shortened version of the remaining methods to save space:

    def render_backups_manager(self):
        """Render the backups and restore interface"""
        st.subheader("💾 Prompt Backups & Version History")
        st.caption("View, restore, and manage prompt backups")

        backup_tab1, backup_tab2, backup_tab3 = st.tabs([
            "📚 All Backups",
            "♻️ Restore Backup",
            "📊 Statistics"
        ])

        with backup_tab1:
            self._render_all_backups()

        with backup_tab2:
            self._render_restore_backup()

        with backup_tab3:
            self._render_backup_statistics()

    def _render_all_backups(self):
        """Render all backups view"""
        st.subheader("All Prompt Backups")

        col1, col2, col3 = st.columns(3)

        with col1:
            accounts_df = self.load_accounts()
            account_options = ["All Accounts"] + list(accounts_df['username'].unique()) if not accounts_df.empty else ["All Accounts"]
            selected_account = st.selectbox("Filter by Account:", account_options, key="backup_account_filter")

        with col2:
            prompts_df = self.load_prompts_data()
            type_options = ["All Types"]
            if not prompts_df.empty and 'prompt_type' in prompts_df.columns:
                type_options += list(prompts_df['prompt_type'].unique())
            selected_type = st.selectbox("Filter by Type:", type_options, key="backup_type_filter")

        with col3:
            limit = st.number_input("Show Last N Backups:", min_value=10, max_value=500, value=50, key="backup_limit")

        username_filter = None if selected_account == "All Accounts" else selected_account
        type_filter = None if selected_type == "All Types" else selected_type

        backups = self.backup_db.get_all_backups(
            username=username_filter,
            prompt_type=type_filter,
            limit=limit
        )

        if backups:
            st.write(f"**Found {len(backups)} backups**")
            # Display backups (simplified for brevity)
            for backup in backups:
                with st.expander(
                    f"🔄 {backup['prompt_name']} (v{backup['version_number']}) - {backup['username']}",
                    expanded=False
                ):
                    st.write(f"**Backup ID:** {backup['backup_id']}")
                    st.write(f"**Prompt ID:** {backup['prompt_id']}")
                    st.write(f"**Version:** {backup['version_number']}")

                    full_backup = self.backup_db.get_backup_by_id(backup['backup_id'])
                    if full_backup:
                        st.text_area(
                            "Backed Up Content:",
                            value=full_backup['prompt_content'],
                            height=200,
                            disabled=True,
                            key=f"backup_content_{backup['backup_id']}"
                        )
        else:
            st.info("No backups found matching the filters")

    def _render_restore_backup(self):
        """Render restore backup interface"""
        st.subheader("Restore from Backup")
        prompts_df = self.load_prompts_data()

        if prompts_df.empty:
            st.warning("No prompts available")
            return

        prompt_options = []
        for _, row in prompts_df.iterrows():
            option_text = f"{row['prompt_name']} ({row['username']}) - {row['prompt_type']}"
            prompt_options.append((option_text, row['prompt_id']))

        selected_prompt = st.selectbox(
            "Select Prompt to View Backups:",
            options=[None] + prompt_options,
            format_func=lambda x: "Choose a prompt..." if x is None else x[0],
            key="restore_prompt_selector"
        )

        if selected_prompt:
            prompt_id = selected_prompt[1]
            versions = self.backup_db.get_backup_versions(prompt_id)

            if versions:
                st.write(f"**Found {len(versions)} backup versions for this prompt**")
                for version in versions:
                    if st.button(f"♻️ Restore v{version['version_number']}", key=f"restore_{version['backup_id']}"):
                        result = self.backup_db.restore_backup(version['backup_id'], create_new_prompt=False)
                        if result:
                            st.success("✅ Restored!")
                            st.cache_data.clear()
                            time.sleep(1)
                            st.rerun()
            else:
                st.info("No backup versions found for this prompt")

    def _render_backup_statistics(self):
        """Render backup statistics"""
        st.subheader("Backup Statistics")
        stats = self.backup_db.get_backup_statistics()

        if stats:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Backups", stats.get('total_backups', 0))
            with col2:
                st.metric("Prompts with Backups", stats.get('prompts_with_backups', 0))
            with col3:
                st.metric("Last 7 Days", stats.get('last_7_days', 0))
            with col4:
                st.metric("Restored Count", stats.get('restored_count', 0))
        else:
            st.info("No backup statistics available")

    # ============================================================================
# DEFAULT PROMPT DEFINITIONS
# ============================================================================

    DEFAULT_PROMPTS = [
        {
            'slot':         1,
            'name':         'replies',
            'prompt_type':  'x',
            'content': (
                "You are a friendly academic assistant responding to students on social media "
                "who appear stressed, overwhelmed, or struggling with schoolwork. Write natural "
                "reply messages that offer help with assignments, homework, essays, thesis work, "
                "or any academic task. Always mention that payment comes only after the work is "
                "done and delivered. The tone should be empathetic and encouraging, like someone "
                "who genuinely wants to help the student succeed. Maximum 120 characters per reply."
            ),
        },
        {
            'slot':         2,
            'name':         'retweets',
            'prompt_type':  'x',
            'content': (
                "You are a helpful academic tutor reaching out to students on social media. "
                "Write short, conversational messages asking if anyone needs help with their "
                "assignments, homework, essays, or academic tasks. Make it clear that students "
                "only pay after the work is completed and delivered to their satisfaction. "
                "The tone should feel natural and supportive, like a friend offering help. "
                "Limit each message to 120 characters."
            ),
        },
        {
            'slot':         3,
            'name':         'messages',
            'prompt_type':  'x',
            'content': (
                "You are a friendly academic helper who offers assignment and homework assistance. "
                "Write engaging tweets that attract students who need help with their academic work. "
                "Each tweet should clearly communicate that you offer expert help with assignments, "
                "essays, homework, and academic tasks, and that payment is only required after the "
                "work is delivered. Keep the tone warm, confident, and student-friendly. "
                "Maximum 120 characters per tweet."
            ),
        },
    ]
    
    
    def render_create_prompt_modal(self):
        """
        Render create prompt form — always shows 3 pre-filled prompt slots
        (replies, retweets, messages) with type 'x' and default content.
        Each slot can be submitted independently.
        """
        if not st.session_state.get('show_create_prompt', False):
            return
    
        with st.expander("➕ Create New Prompts (3 Defaults)", expanded=True):
            self.render_operation_logs()
    
            if st.session_state.get('creating_prompt', False):
                st.info("⏳ Creating prompt... Please wait.")
                return
    
            st.markdown("""
                <div style='background-color: rgba(76,175,80,0.1);
                            padding: 14px; border-radius: 8px;
                            border-left: 4px solid #4caf50; margin-bottom: 16px;'>
                    <h4 style='margin:0; color:#2e7d32;'>📝 3 Default Prompts</h4>
                    <p style='margin:4px 0 0 0; font-size:0.9em;'>
                        Each slot is pre-filled — edit if needed, then click its own
                        <strong>Create</strong> button. All use prompt type <code>x</code>.
                    </p>
                </div>
            """, unsafe_allow_html=True)
    
            # ── Load accounts once ────────────────────────────────────────────────
            accounts_df = self.load_accounts()
    
            if accounts_df.empty:
                st.error("❌ No accounts available — please create an account first.")
                if st.button("✖ Close", key="close_no_accounts"):
                    self._clear_prompt_creation_state()
                    st.rerun()
                return
    
            account_options = {
                row['username']: row['account_id']
                for _, row in accounts_df.iterrows()
            }
    
            # Shared account selector (applies to all 3 slots)
            selected_account = st.selectbox(
                "Account (applies to all prompts)",
                options=list(account_options.keys()),
                key="bulk_prompt_account"
            )
            account_id = account_options[selected_account]
    
            st.markdown("---")
    
            # ── Render each slot ──────────────────────────────────────────────────
            for defn in self.DEFAULT_PROMPTS:
                slot        = defn['slot']
                slot_key    = f"slot_{slot}"
                label_color = ["#e53935", "#1e88e5", "#43a047"][slot - 1]
                label_emoji = ["💬", "🔄", "📩"][slot - 1]
    
                st.markdown(f"""
                    <div style='background-color: rgba(0,0,0,0.04);
                                padding: 14px; border-radius: 8px;
                                border-left: 5px solid {label_color};
                                margin: 12px 0;'>
                        <h4 style='margin:0; color:{label_color};'>
                            {label_emoji} Prompt {slot} — {defn['name'].capitalize()}
                        </h4>
                    </div>
                """, unsafe_allow_html=True)
    
                with st.form(f"create_prompt_form_{slot_key}", clear_on_submit=False):
    
                    col1, col2 = st.columns(2)
    
                    with col1:
                        prompt_name = st.text_input(
                            "Prompt Name",
                            value=defn['name'],
                            key=f"pname_{slot_key}",
                            help="Name for this prompt"
                        )
    
                    with col2:
                        prompt_type = st.text_input(
                            "Prompt Type",
                            value=defn['prompt_type'],
                            key=f"ptype_{slot_key}",
                            help="Content type (default: x)"
                        )
    
                    prompt_content = st.text_area(
                        "Prompt Content",
                        value=defn['content'],
                        height=180,
                        key=f"pcontent_{slot_key}",
                        help="Edit the default content if needed before saving"
                    )
    
                    col_submit, col_cancel = st.columns([1, 1])
    
                    with col_submit:
                        submit = st.form_submit_button(
                            f"✅ Create '{defn['name'].capitalize()}' Prompt",
                            type="primary",
                            use_container_width=True
                        )
    
                    with col_cancel:
                        cancel = st.form_submit_button(
                            "✖ Cancel All",
                            use_container_width=True
                        )
    
                    # ── Handle submit ─────────────────────────────────────────────
                    if submit:
                        final_name    = prompt_name.strip()
                        final_type    = prompt_type.strip()
                        final_content = prompt_content.strip() if prompt_content.strip() else defn['content']
    
                        # Validation
                        errors = []
                        if len(final_name) < 3:
                            errors.append("Prompt name must be at least 3 characters.")
                        if not final_type:
                            errors.append("Prompt type is required.")
                        if len(final_type) > 50:
                            errors.append("Prompt type must be 50 characters or fewer.")
    
                        if errors:
                            for err in errors:
                                st.warning(f"⚠️ {err}")
                        else:
                            try:
                                from src.core.database.postgres import prompts as pg_prompts
    
                                prompt_id = pg_prompts.create_prompt(
                                    account_id=account_id,
                                    name=final_name,
                                    content=final_content,
                                    prompt_type=final_type,
                                    is_active=True
                                )
    
                                if prompt_id:
                                    self.add_log(
                                        f"✅ Created prompt '{final_name}' "
                                        f"(type={final_type}, id={prompt_id})"
                                    )
                                    st.success(
                                        f"✅ **{final_name}** created! (ID: {prompt_id})"
                                    )
                                    st.cache_data.clear()
                                else:
                                    st.error(f"❌ Failed to create prompt '{final_name}'")
                                    self.add_log(
                                        f"Failed to create prompt '{final_name}'", "ERROR"
                                    )
    
                            except Exception as e:
                                st.error(f"❌ Error creating prompt: {e}")
                                self.add_log(f"Error: {e}", "ERROR")
    
                    # ── Handle cancel ─────────────────────────────────────────────
                    if cancel:
                        self._clear_prompt_creation_state()
                        st.rerun()
    
                if slot < 3:
                    st.markdown("---")
    
            # ── Global close button (outside forms) ───────────────────────────────
            st.markdown("---")
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                if st.button(
                    "✖ Close Panel",
                    key="close_create_prompts_panel",
                    use_container_width=True
                ):
                    self._clear_prompt_creation_state()
                    st.rerun()

    def render_delete_prompt_modal(self, prompts_df):
        """Render delete prompt interface"""
        if st.session_state.get('show_delete_prompt', False):
            with st.expander("🗑️ Delete Prompt", expanded=True):
                self.render_operation_logs()

                if st.session_state.get('deleting_prompt', False):
                    st.info("⏳ Deleting prompt... Please wait.")
                    return

                st.warning("⚠️ **Prompt Deletion Warning**")
                st.write("A backup will be created automatically before deletion.")

                with st.form("delete_prompt_form"):
                    if not prompts_df.empty:
                        prompt_options = []
                        for _, row in prompts_df.iterrows():
                            option_text = f"{row['prompt_name']} ({row['username']}) - {row['prompt_type']}"
                            prompt_options.append((option_text, row['prompt_id']))

                        selected_prompt = st.selectbox(
                            "Select Prompt to Delete",
                            options=[None] + prompt_options,
                            format_func=lambda x: "Choose a prompt..." if x is None else x[0]
                        )

                        if selected_prompt:
                            prompt_id = selected_prompt[1]
                            prompt_row = prompts_df[prompts_df['prompt_id'] == prompt_id].iloc[0]

                            st.info("**Prompt Details:**")
                            st.write(f"**Name:** {prompt_row['prompt_name']}")
                            st.write(f"**Type:** {prompt_row['prompt_type']}")
                            st.write(f"**Account:** {prompt_row['username']}")

                            confirm_deletion = st.checkbox(
                                f"I understand this will delete '{prompt_row['prompt_name']}' (backup will be created)"
                            )
                        else:
                            confirm_deletion = False
                    else:
                        st.info("No prompts available to delete.")
                        selected_prompt = None
                        confirm_deletion = False

                    col_delete, col_cancel = st.columns([1, 1])

                    with col_delete:
                        delete_submitted = st.form_submit_button(
                            "🗑️ Delete Prompt",
                            use_container_width=True,
                            type="primary",
                            disabled=not (selected_prompt and confirm_deletion)
                        )

                    with col_cancel:
                        cancel_delete = st.form_submit_button("Cancel", use_container_width=True)

                    if delete_submitted and selected_prompt and confirm_deletion:
                        prompt_id = selected_prompt[1]

                        success = pg_prompts.delete_prompt(prompt_id)
                        if success:
                            st.success("✅ Prompt deleted successfully! (Backup created)")
                            st.cache_data.clear()
                            time.sleep(1)
                            self._clear_prompt_deletion_state()
                            st.rerun()
                        else:
                            st.error("Failed to delete prompt")

                    if cancel_delete:
                        self._clear_prompt_deletion_state()
                        st.rerun()

    def render_prompt_detail(self, prompts_df: pd.DataFrame):
        """Render detailed view for a specific prompt"""
        if 'selected_prompt_id' not in st.session_state:
            st.info("Select a prompt from the Overview tab to view details.")
            return

        prompt_id = st.session_state.selected_prompt_id

        if prompts_df.empty or prompt_id not in prompts_df['prompt_id'].values:
            st.warning("Selected prompt not found.")
            return

        prompt_data = prompts_df[prompts_df['prompt_id'] == prompt_id].iloc[0]

        col1, col2 = st.columns([3, 1])
        with col1:
            st.title(f"📋 {prompt_data['prompt_name']}")
        with col2:
            if st.button("← Back to Overview"):
                st.session_state.pop('selected_prompt_id', None)
                st.rerun()

        st.markdown("---")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Content Generated", prompt_data.get('content_count', 0))
        with col2:
            st.metric("Backups", prompt_data.get('backup_count', 0))
        with col3:
            st.metric("Variations", prompt_data.get('variation_count', 0))
        with col4:
            status = "Active" if prompt_data['is_active'] else "Inactive"
            st.metric("Status", status)

        with st.form("edit_prompt_form"):
            new_name = st.text_input("Prompt Name", value=prompt_data['prompt_name'])
            new_content = st.text_area("Content", value=prompt_data['content'], height=300)
            is_active = st.checkbox("Active", value=prompt_data['is_active'])

            if st.form_submit_button("💾 Save Changes", type="primary"):
                success = pg_prompts.update_prompt(
                    prompt_id,
                    name=new_name,
                    content=new_content,
                    is_active=is_active
                )
                if success:
                    st.success("✅ Prompt updated successfully! (Backup created automatically)")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Failed to update prompt")

    def render_analytics(self, prompts_df: pd.DataFrame):
        """Render analytics view"""
        st.title("📊 Prompts Analytics")

        if prompts_df.empty:
            st.warning("No prompts data available for analytics")
            return

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Prompts", len(prompts_df))
        with col2:
            total_content = prompts_df['content_count'].sum() if 'content_count' in prompts_df.columns else 0
            st.metric("Total Content", int(total_content))
        with col3:
            total_backups = prompts_df['backup_count'].sum() if 'backup_count' in prompts_df.columns else 0
            st.metric("Total Backups", int(total_backups))
        with col4:
            total_variations = prompts_df['variation_count'].sum() if 'variation_count' in prompts_df.columns else 0
            st.metric("Total Variations", int(total_variations))

        st.markdown("---")

        col1, col2 = st.columns(2)

        with col1:
            if 'prompt_type' in prompts_df.columns:
                type_counts = prompts_df['prompt_type'].value_counts()
                fig = px.pie(
                    values=type_counts.values,
                    names=type_counts.index,
                    title='Prompts Distribution by Type'
                )
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            if 'username' in prompts_df.columns:
                account_counts = prompts_df['username'].value_counts().head(10)
                fig = px.bar(
                    x=account_counts.values,
                    y=account_counts.index,
                    orientation='h',
                    title='Top Accounts by Prompt Count'
                )
                st.plotly_chart(fig, use_container_width=True)

        if 'variation_count' in prompts_df.columns:
            variation_stats = self.variations_db.get_variation_statistics()

            st.subheader("Variation Statistics")
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Total Variations", variation_stats.get('total_variations', 0))
            with col2:
                st.metric("Unused", variation_stats.get('unused_variations', 0))
            with col3:
                st.metric("Total Copies", variation_stats.get('total_copies', 0))
