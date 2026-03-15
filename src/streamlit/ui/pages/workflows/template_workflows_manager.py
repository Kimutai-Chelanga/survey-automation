"""FILE: ui/components/template_workflows_manager.py
Component for creating, uploading, and managing workflow templates
"""

import streamlit as st
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from bson import ObjectId
from src.core.database.mongodb.connection import get_mongo_collection


class TemplateWorkflowsManager:
    """Manager for workflow templates - upload, create, view, and manage templates."""

    def __init__(self):
        self.templates_collection = get_mongo_collection("workflow_templates")

    def render(self):
        """Render the complete template workflows management interface."""
        st.header("📝 Create Template Workflows")
        st.caption("Upload or paste workflow templates to use as starting points for new workflows")

        # Main tabs for different actions - ADDED DESTINATION TAB
        tab1, tab2, tab3 = st.tabs([
            "➕ Create New Template",
            "📚 View Templates",
            "📁 Destination Categories"
        ])

        with tab1:
            self._render_create_template_section()

        with tab2:
            self._render_view_templates_section()

        with tab3:
            self._render_destination_categories_section()

    def _render_destination_categories_section(self):
        """Render section for managing workflow destination categories and types."""
        st.subheader("📁 Workflow Destination Categories & Types")
        st.caption("Create and manage workflow categories and their workflow types")

        # Import settings functions
        from ...settings.settings_manager import (
            get_system_setting,
            update_system_setting
        )

        # Load workflow categories from settings
        workflow_categories = get_system_setting('workflow_categories', {})

        if not isinstance(workflow_categories, dict):
            workflow_categories = {}
            update_system_setting('workflow_categories', workflow_categories)

        # Create two columns for better layout
        col1, col2 = st.columns([2, 1])

        with col1:
            st.markdown("### ➕ Create New Category / Type")

        with col2:
            if st.button("🔄 Refresh", key="refresh_dest_categories"):
                if 'destination_categories_cache' in st.session_state:
                    del st.session_state['destination_categories_cache']
                st.rerun()

        # Form for creating new category/type
        with st.form("create_destination_category_form"):
            st.markdown("#### Add Category and Type")

            col1, col2 = st.columns(2)

            with col1:
                # Get existing categories
                existing_categories = sorted(list(workflow_categories.keys()))
                category_options = ["➕ Create New Category"] + existing_categories

                selected_category_option = st.selectbox(
                    "Select or Create Category:",
                    options=category_options,
                    key="dest_cat_select"
                )

                if selected_category_option == "➕ Create New Category":
                    new_category = st.text_input(
                        "New Category Name:",
                        key="new_dest_cat_input",
                        placeholder="e.g., Social Media, Marketing, Operations"
                    )
                    category_name = new_category.strip()
                else:
                    category_name = selected_category_option

            with col2:
                # Show workflow type input
                if category_name:
                    # Get existing types for this category
                    existing_types = workflow_categories.get(category_name, [])

                    if existing_types:
                        st.info(f"📋 Existing types: {', '.join(existing_types)}")

                    workflow_type = st.text_input(
                        "Workflow Type:",
                        key="new_workflow_type",
                        placeholder="e.g., scheduled_posts, auto_replies"
                    )
                else:
                    st.info("👆 Select or create a category first")
                    workflow_type = ""

            # Submit button
            submit_category = st.form_submit_button(
                "💾 Save Category/Type",
                type="primary",
                use_container_width=True
            )

            if submit_category:
                if not category_name:
                    st.error("❌ Please provide a category name")
                elif not workflow_type or not workflow_type.strip():
                    st.error("❌ Please provide a workflow type")
                else:
                    try:
                        # Add category if it doesn't exist
                        if category_name not in workflow_categories:
                            workflow_categories[category_name] = []

                        # Add workflow type if it doesn't exist
                        workflow_type_clean = workflow_type.strip()
                        if workflow_type_clean not in workflow_categories[category_name]:
                            workflow_categories[category_name].append(workflow_type_clean)
                            workflow_categories[category_name].sort()

                            # Save to settings
                            update_system_setting('workflow_categories', workflow_categories)

                            st.success(f"✅ Added '{workflow_type_clean}' to category '{category_name}'")
                            st.balloons()

                            # Clear cache
                            if 'destination_categories_cache' in st.session_state:
                                del st.session_state['destination_categories_cache']

                            st.rerun()
                        else:
                            st.warning(f"⚠️ Workflow type '{workflow_type_clean}' already exists in '{category_name}'")

                    except Exception as e:
                        st.error(f"❌ Error saving category/type: {e}")

        st.markdown("---")

        # Display existing categories and types
        st.markdown("### 📚 Existing Categories & Types")

        if not workflow_categories:
            st.info("📭 No workflow categories created yet. Create your first one above!")
            return

        st.success(f"Found {len(workflow_categories)} categories")

        # Filter options
        col1, col2 = st.columns([2, 1])

        with col1:
            search_term = st.text_input(
                "Search categories or types:",
                key="dest_search",
                placeholder="Search..."
            )

        # Display each category
        for category_name in sorted(workflow_categories.keys()):
            workflow_types = workflow_categories[category_name]

            # Apply search filter
            if search_term:
                search_lower = search_term.lower()
                if (search_lower not in category_name.lower() and
                    not any(search_lower in wt.lower() for wt in workflow_types)):
                    continue

            with st.expander(
                f"📁 {category_name.title()} ({len(workflow_types)} types)",
                expanded=False
            ):
                # Category info
                col1, col2 = st.columns([3, 1])

                with col1:
                    st.markdown(f"**Category:** {category_name}")
                    st.markdown(f"**Total Types:** {len(workflow_types)}")

                with col2:
                    # Delete entire category
                    if st.button(
                        "🗑️ Delete Category",
                        key=f"delete_category_{category_name}",
                        help="Delete this entire category and all its types"
                    ):
                        try:
                            del workflow_categories[category_name]
                            update_system_setting('workflow_categories', workflow_categories)
                            st.success(f"✅ Deleted category '{category_name}'")

                            if 'destination_categories_cache' in st.session_state:
                                del st.session_state['destination_categories_cache']

                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error deleting category: {e}")

                st.markdown("---")

                # Display workflow types
                if workflow_types:
                    st.markdown("**Workflow Types:**")

                    # Display in a grid
                    for i in range(0, len(workflow_types), 2):
                        cols = st.columns([2, 1, 2, 1])

                        # First type in row
                        with cols[0]:
                            st.write(f"• **{workflow_types[i]}**")

                        with cols[1]:
                            if st.button(
                                "🗑️",
                                key=f"delete_type_{category_name}_{workflow_types[i]}",
                                help=f"Delete {workflow_types[i]}"
                            ):
                                try:
                                    workflow_categories[category_name].remove(workflow_types[i])
                                    update_system_setting('workflow_categories', workflow_categories)
                                    st.success(f"✅ Deleted '{workflow_types[i]}'")

                                    if 'destination_categories_cache' in st.session_state:
                                        del st.session_state['destination_categories_cache']

                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Error deleting type: {e}")

                        # Second type in row (if exists)
                        if i + 1 < len(workflow_types):
                            with cols[2]:
                                st.write(f"• **{workflow_types[i + 1]}**")

                            with cols[3]:
                                if st.button(
                                    "🗑️",
                                    key=f"delete_type_{category_name}_{workflow_types[i + 1]}",
                                    help=f"Delete {workflow_types[i + 1]}"
                                ):
                                    try:
                                        workflow_categories[category_name].remove(workflow_types[i + 1])
                                        update_system_setting('workflow_categories', workflow_categories)
                                        st.success(f"✅ Deleted '{workflow_types[i + 1]}'")

                                        if 'destination_categories_cache' in st.session_state:
                                            del st.session_state['destination_categories_cache']

                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"❌ Error deleting type: {e}")

                else:
                    st.info("No workflow types in this category")

                # Add new type to existing category
                st.markdown("---")
                st.markdown("**Add New Type to This Category:**")

                add_type_col1, add_type_col2 = st.columns([3, 1])

                with add_type_col1:
                    new_type = st.text_input(
                        "New Workflow Type:",
                        key=f"add_type_to_{category_name}",
                        placeholder="Enter new workflow type"
                    )

                with add_type_col2:
                    st.write("")  # Spacing
                    st.write("")  # Spacing
                    if st.button(
                        "➕ Add Type",
                        key=f"add_type_btn_{category_name}",
                        use_container_width=True
                    ):
                        if new_type and new_type.strip():
                            try:
                                new_type_clean = new_type.strip()
                                if new_type_clean not in workflow_categories[category_name]:
                                    workflow_categories[category_name].append(new_type_clean)
                                    workflow_categories[category_name].sort()
                                    update_system_setting('workflow_categories', workflow_categories)
                                    st.success(f"✅ Added '{new_type_clean}' to '{category_name}'")

                                    if 'destination_categories_cache' in st.session_state:
                                        del st.session_state['destination_categories_cache']

                                    st.rerun()
                                else:
                                    st.warning(f"⚠️ Type '{new_type_clean}' already exists")
                            except Exception as e:
                                st.error(f"❌ Error adding type: {e}")
                        else:
                            st.warning("⚠️ Please enter a workflow type name")

        # Statistics section
        st.markdown("---")
        st.markdown("### 📊 Statistics")

        total_types = sum(len(types) for types in workflow_categories.values())

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Categories", len(workflow_categories))

        with col2:
            st.metric("Total Workflow Types", total_types)

        with col3:
            avg_types = total_types / len(workflow_categories) if workflow_categories else 0
            st.metric("Avg Types per Category", f"{avg_types:.1f}")

    def _render_create_template_section(self):
        """Render the section for creating new templates."""
        st.subheader("Create New Workflow Template")

        # Input method selection
        input_method = st.radio(
            "Choose input method:",
            ["Upload JSON File", "Paste JSON Data"],
            key="template_input_method",
            horizontal=True
        )

        workflow_data = None

        if input_method == "Upload JSON File":
            workflow_data = self._render_file_upload()
        else:
            workflow_data = self._render_paste_json()

        # If we have workflow data, show preview and save options
        if workflow_data:
            self._render_template_preview_and_save(workflow_data)

    def _render_file_upload(self) -> Optional[Dict[str, Any]]:
        """Render file upload interface."""
        st.markdown("---")
        st.markdown("**Upload Workflow JSON File**")

        uploaded_file = st.file_uploader(
            "Choose a JSON file",
            type=['json'],
            key="template_file_upload",
            help="Upload an Automa workflow JSON file"
        )

        if uploaded_file is not None:
            try:
                workflow_data = json.load(uploaded_file)
                st.success(f"✅ File '{uploaded_file.name}' loaded successfully!")
                return workflow_data
            except json.JSONDecodeError as e:
                st.error(f"❌ Invalid JSON file: {e}")
                return None
            except Exception as e:
                st.error(f"❌ Error loading file: {e}")
                return None

        return None

    def _render_paste_json(self) -> Optional[Dict[str, Any]]:
        """Render JSON paste interface with default template."""
        st.markdown("---")
        st.markdown("**Paste Workflow JSON Data**")

        # Updated default template - properly formatted JSON string
        default_template = """{"extVersion":"1.30.00","name":"complete_twitter_template","icon":"riGlobalLine","table":[],"version":"1.30.00","drawflow":{"nodes":[{"id":"gg8f6b5","type":"BlockGroup","initialized":false,"position":{"x":-266.15384615384613,"y":63.07692307692312},"data":{"blocks":[{"data":{"active":true,"customUserAgent":false,"description":"","disableBlock":false,"inGroup":false,"tabZoom":1,"updatePrevTab":true,"url":"https://x.com/YungMiami305/status/2014769409928184038","userAgent":"","waitTabLoaded":true},"id":"new-tab","itemId":"_qCaE"},{"data":{"disableBlock":false,"time":"20000","description":"Human-like pause (3516ms)","timeout":3516},"id":"delay","itemId":"delay_6c8327db"},{"data":{"description":"","disableBlock":false,"findBy":"cssSelector","markEl":false,"multiple":false,"onError":{"dataToInsert":[],"enable":true,"insertData":false,"retry":false,"retryInterval":2,"retryTimes":1,"toDo":"continue"},"selector":"div.public-DraftStyleDefault-block","settings":{"blockTimeout":0,"debugMode":false},"waitForSelector":true,"waitSelectorTimeout":16259},"id":"event-click","itemId":"UEITS"},{"data":{"disableBlock":false,"time":"20000","timeout":3216},"id":"delay","itemId":"7BPK-"},{"data":{"assignVariable":true,"captureActiveTab":true,"dataColumn":"","description":"","disableBlock":false,"ext":"png","fileName":"pre_account_1_complete_7_20260124_001430","fullPage":true,"quality":100,"saveToColumn":false,"saveToComputer":true,"selector":"","type":"fullpage","variableName":""},"id":"take-screenshot","itemId":"lgBP6"}],"disableBlock":false,"name":"one"},"label":"blocks-group"},{"id":"28t34wl","type":"BlockGroup","initialized":false,"position":{"x":900.2564102564097,"y":159.23076923076928},"data":{"blocks":[{"data":{"description":"","disableBlock":false,"findBy":"cssSelector","markEl":false,"multiple":false,"selector":"button[data-testid='tweetButtonInline']","waitForSelector":true,"waitSelectorTimeout":18865},"id":"event-click","itemId":"NpyBu"},{"data":{"disableBlock":false,"time":"20000","description":"Human-like pause (3080ms)","timeout":3080},"id":"delay","itemId":"delay_25fd3954"},{"data":{"description":"","disableBlock":false,"findBy":"cssSelector","markEl":false,"multiple":false,"selector":"article [data-testid='like'] svg","waitForSelector":true,"waitSelectorTimeout":22802},"id":"event-click","itemId":"kg4d2"},{"data":{"description":"","disableBlock":false,"findBy":"cssSelector","markEl":false,"multiple":false,"selector":"article [data-testid='retweet'] svg","waitForSelector":true,"waitSelectorTimeout":39306},"id":"event-click","itemId":"H1EuL"},{"data":{"description":"","disableBlock":false,"findBy":"cssSelector","markEl":false,"multiple":false,"selector":"a.r-3pj75a:nth-child(2)","waitForSelector":true,"waitSelectorTimeout":18235},"id":"event-click","itemId":"8qRAY"}],"disableBlock":false,"name":"kags"},"label":"blocks-group"},{"id":"d2kb6g5","type":"BlockBasic","initialized":false,"position":{"x":-598.0411169355232,"y":165.17326086790632},"data":{"activeInInput":false,"contextMenuName":"","contextTypes":[],"date":"","days":[],"delay":5,"description":"","disableBlock":false,"interval":60,"isUrlRegex":false,"observeElement":{"baseElOptions":{"attributeFilter":[],"attributes":false,"characterData":false,"childList":true,"subtree":false},"baseSelector":"","matchPattern":"","selector":"","targetOptions":{"attributeFilter":[],"attributes":false,"characterData":false,"childList":true,"subtree":false}},"parameters":[],"preferParamsInTab":false,"shortcut":"","time":"00:00","triggers":[{"data":{"activeInInput":false,"contextMenuName":"","contextTypes":[],"date":"","days":[],"delay":5,"description":"","disableBlock":false,"interval":60,"isUrlRegex":false,"observeElement":{"baseElOptions":{"attributeFilter":[],"attributes":false,"characterData":false,"childList":true,"subtree":false},"baseSelector":"","matchPattern":"","selector":"","targetOptions":{"attributeFilter":[],"attributes":false,"characterData":false,"childList":true,"subtree":false}},"parameters":[],"preferParamsInTab":false,"shortcut":"","time":"00:00","type":"manual","url":""},"id":"nd8S0","type":"manual"},{"data":null,"id":"QngQr","type":"on-startup"}],"type":"manual","url":""},"label":"trigger"},{"id":"6xi15k7","type":"BlockGroup","initialized":false,"position":{"x":-480.4786559685758,"y":667.7005131055984},"data":{"blocks":[{"data":{"description":"","disableBlock":false,"findBy":"cssSelector","markEl":false,"multiple":false,"selector":"button[data-testid='tweetButton']","waitForSelector":true,"waitSelectorTimeout":21606},"id":"event-click","itemId":"NpyBu"},{"data":{"disableBlock":false,"time":"20000","description":"Human-like pause (3272ms)","timeout":3272},"id":"delay","itemId":"delay_7fd9ffeb"}],"disableBlock":false,"name":"kags"},"label":"blocks-group"},{"id":"tczsl4t","type":"BlockGroup","initialized":false,"position":{"x":1061.5378087190843,"y":724.9015735528582},"data":{"blocks":[{"data":{"description":"","disableBlock":false,"findBy":"cssSelector","markEl":false,"multiple":false,"selector":"button[data-testid='dm-composer-send-button']","waitForSelector":true,"waitSelectorTimeout":18926},"id":"event-click","itemId":"NpyBu"},{"data":{"description":"Human-like pause (4051ms)","disableBlock":false,"time":"20000","timeout":4051},"id":"delay","itemId":"delay_da7b3ffc"},{"data":{"assignVariable":true,"captureActiveTab":true,"dataColumn":"","description":"","disableBlock":false,"ext":"png","fileName":"post_account_1_complete_7_20260124_001430","fullPage":true,"quality":100,"saveToColumn":false,"saveToComputer":true,"selector":"","type":"fullpage","variableName":""},"id":"take-screenshot","itemId":"RWjSX"},{"data":{"disableBlock":false,"time":"20000"},"id":"delay","itemId":"zE1wr"}],"disableBlock":false,"name":"kags"},"label":"blocks-group"},{"id":"1vysegv","type":"BlockGroup","initialized":false,"position":{"x":-248.07472194256593,"y":650.4144261953722},"data":{"blocks":[{"data":{"description":"","disableBlock":false,"findBy":"cssSelector","markEl":false,"multiple":false,"selector":"aside [data-testid='UserCell'] a","waitForSelector":true,"waitSelectorTimeout":18658},"id":"event-click","itemId":"H1EuL"},{"data":{"disableBlock":false,"time":"20000","description":"Human-like pause (1128ms)","timeout":1128},"id":"delay","itemId":"delay_1d5e3f88"},{"data":{"description":"","disableBlock":false,"findBy":"xpath","markEl":false,"multiple":false,"selector":"id('react-root')/DIV[1]/DIV[1]/DIV[2]/MAIN[1]/DIV[1]/DIV[1]/DIV[1]/DIV[1]/DIV[1]/DIV[3]/DIV[1]/DIV[1]/DIV[1]/DIV[1]/DIV[1]/DIV[2]/DIV[1]/DIV[1]/BUTTON[1]","waitForSelector":true,"waitSelectorTimeout":22386},"id":"event-click","itemId":"NpyBu"}],"disableBlock":false,"name":"kags","settings":{"blockTimeout":0,"debugMode":false}},"label":"blocks-group"},{"id":"5s6bh34","type":"BlockGroup","initialized":false,"position":{"x":321.3470792207934,"y":184.11112490074674},"data":{"blocks":[{"data":{"disableBlock":false,"keys":"CONTENT1","selector":"","pressTime":"0","description":"Type the message","keysToPress":"","action":"press-key","onError":{"dataToInsert":[],"enable":true,"insertData":false,"retry":false,"retryInterval":2,"retryTimes":1,"toDo":"continue"},"settings":{"blockTimeout":0,"debugMode":false}},"id":"press-key","itemId":"message-placeholder"}],"description":"Group for typing messages","disableBlock":false,"name":"typing_group"},"label":"blocks-group"},{"id":"mikp2rl","type":"BlockGroup","initialized":false,"position":{"x":1512.7283678747235,"y":217.20831246866715},"data":{"blocks":[{"data":{"action":"press-key","description":"","disableBlock":false,"keys":"CONTENT2","keysToPress":"","pressTime":"0","selector":""},"id":"press-key","itemId":"bj9Bb"}],"disableBlock":false,"name":""},"label":"blocks-group"},{"id":"3s8f05k","type":"BlockGroup","initialized":false,"position":{"x":552.8222052858412,"y":734.9755291285686},"data":{"blocks":[{"data":{"action":"press-key","description":"","disableBlock":false,"keys":"CONTENT3","keysToPress":"","pressTime":"0","selector":""},"id":"press-key","itemId":"6K6vU"}],"disableBlock":false,"name":""},"label":"blocks-group"},{"id":"6hftvy2","type":"BlockBasic","initialized":false,"position":{"x":347,"y":666},"data":{"description":"","disableBlock":false,"findBy":"cssSelector","markEl":false,"multiple":false,"selector":"[data-testid='sendDMFromProfile'] > .css-146c3p1","waitForSelector":true,"waitSelectorTimeout":18599,"settings":{"blockTimeout":0,"debugMode":false},"onError":{"retry":false,"enable":true,"retryTimes":1,"retryInterval":2,"toDo":"continue","errorMessage":"","insertData":false,"dataToInsert":[]}},"label":"event-click"},{"id":"r41eo2o","type":"BlockElementExists","initialized":false,"position":{"x":116,"y":718},"data":{"disableBlock":false,"description":"","findBy":"cssSelector","selector":"[data-testid='sendDMFromProfile'] > .css-146c3p1","tryCount":2,"timeout":10000,"markEl":false,"throwError":false,"settings":{"blockTimeout":0,"debugMode":false}},"label":"element-exists"}],"edges":[{"id":"vueflow__edge-d2kb6g5d2kb6g5-output-1-gg8f6b5gg8f6b5-input-1","type":"custom","source":"d2kb6g5","target":"gg8f6b5","sourceHandle":"d2kb6g5-output-1","targetHandle":"gg8f6b5-input-1","updatable":true,"selectable":true,"data":{},"label":"","markerEnd":"arrowclosed","sourceX":-382.04110930612865,"sourceY":201.17326086790632,"targetX":-290.15384615384613,"targetY":233.07693833571219},{"id":"vueflow__edge-6xi15k76xi15k7-output-1-1vysegv1vysegv-input-1","type":"custom","source":"6xi15k7","target":"1vysegv","sourceHandle":"6xi15k7-output-1","targetHandle":"1vysegv-input-1","updatable":true,"selectable":true,"data":{},"label":"","markerEnd":"arrowclosed","sourceX":-200.47865596857582,"sourceY":791.867169599739,"targetX":-272.07472194256593,"targetY":794.5810521719347},{"id":"vueflow__edge-gg8f6b5gg8f6b5-output-1-5s6bh345s6bh34-input-1","type":"custom","source":"gg8f6b5","target":"5s6bh34","sourceHandle":"gg8f6b5-output-1","targetHandle":"5s6bh34-input-1","updatable":true,"selectable":true,"data":{},"label":"","markerEnd":"arrowclosed","sourceX":13.846153846153868,"sourceY":233.07693833571219,"targetX":297.3470792207934,"targetY":288.27779665367643},{"id":"vueflow__edge-5s6bh345s6bh34-output-1-28t34wl28t34wl-input-1","type":"custom","source":"5s6bh34","target":"28t34wl","sourceHandle":"5s6bh34-output-1","targetHandle":"28t34wl-input-1","updatable":true,"selectable":true,"data":{},"label":"","markerEnd":"arrowclosed","sourceX":601.3471402559496,"sourceY":288.27779665367643,"targetX":876.2562881860972,"targetY":329.2307692307693},{"id":"vueflow__edge-28t34wl28t34wl-output-1-mikp2rlmikp2rl-input-1","type":"custom","source":"28t34wl","target":"mikp2rl","sourceHandle":"28t34wl-output-1","targetHandle":"mikp2rl-input-1","updatable":true,"selectable":true,"data":{},"label":"","markerEnd":"arrowclosed","sourceX":1180.2564102564097,"sourceY":329.2307692307693,"targetX":1488.7283678747235,"targetY":313.8749765922023},{"id":"vueflow__edge-mikp2rlmikp2rl-output-1-6xi15k76xi15k7-input-1","type":"custom","source":"mikp2rl","target":"6xi15k7","sourceHandle":"mikp2rl-output-1","targetHandle":"6xi15k7-input-1","updatable":true,"selectable":true,"data":{},"label":"","markerEnd":"arrowclosed","sourceX":1792.728489945036,"sourceY":313.8749765922023,"targetX":-504.4786559685758,"targetY":791.867169599739},{"id":"vueflow__edge-3s8f05k3s8f05k-output-1-tczsl4ttczsl4t-input-1","type":"custom","source":"3s8f05k","target":"tczsl4t","sourceHandle":"3s8f05k-output-1","targetHandle":"tczsl4t-input-1","updatable":true,"selectable":true,"data":{},"label":"","markerEnd":"arrowclosed","sourceX":832.8222052858412,"sourceY":831.6421856227092,"targetX":1037.5378087190843,"targetY":889.0682605645769},{"id":"vueflow__edge-1vysegv1vysegv-output-1-r41eo2or41eo2o-input-1","type":"custom","source":"1vysegv","target":"r41eo2o","sourceHandle":"1vysegv-output-1","targetHandle":"r41eo2o-input-1","updatable":true,"selectable":true,"data":{},"label":"","markerEnd":"arrowclosed","sourceX":31.925293316223133,"sourceY":794.5810521719347,"targetX":92,"targetY":790.4166870117188},{"id":"vueflow__edge-r41eo2or41eo2o-output-1-6hftvy26hftvy2-input-1","type":"custom","source":"r41eo2o","target":"6hftvy2","sourceHandle":"r41eo2o-output-1","targetHandle":"6hftvy2-input-1","updatable":true,"selectable":true,"data":{},"label":"","markerEnd":"arrowclosed","sourceX":335,"sourceY":790.4166870117188,"targetX":323,"targetY":702},{"id":"vueflow__edge-6hftvy26hftvy2-output-1-3s8f05k3s8f05k-input-1","type":"custom","source":"6hftvy2","target":"3s8f05k","sourceHandle":"6hftvy2-output-1","targetHandle":"3s8f05k-input-1","updatable":true,"selectable":true,"data":{},"label":"","markerEnd":"arrowclosed","sourceX":563,"sourceY":702,"targetX":528.8222052858412,"targetY":831.6421856227092}],"position":[53.875,-454],"zoom":1,"viewport":{"x":53.875,"y":-454,"zoom":1}},"settings":{"publicId":"","aipowerToken":"","blockDelay":0,"saveLog":true,"debugMode":false,"restartTimes":3,"notification":true,"execContext":"popup","reuseLastState":false,"inputAutocomplete":true,"onError":"stop-workflow","executedBlockOnWeb":false,"insertDefaultColumn":false,"defaultColumnName":"column"},"globalData":"{'key': 'value'}","description":"Complete reply,retweet, and message","includedWorkflows":{}}"""

        json_text = st.text_area(
            "Paste your workflow JSON here:",
            value=default_template,
            height=300,
            key="template_json_paste",
            placeholder='{"name": "My Workflow", "version": "1.0", "drawflow": {...}}',
            help="Default template is pre-loaded. Clear and paste your own JSON if needed."
        )

        if json_text:
            try:
                # Clean the JSON string
                cleaned_json = json_text.strip()

                # Remove BOM if present
                if cleaned_json.startswith('\ufeff'):
                    cleaned_json = cleaned_json[1:]

                # Parse the JSON
                workflow_data = json.loads(cleaned_json)
                st.success("✅ JSON loaded successfully!")
                return workflow_data

            except json.JSONDecodeError as e:
                st.error(f"❌ Invalid JSON: {e}")
                return None
            except Exception as e:
                st.error(f"❌ Error loading JSON: {e}")
                return None

        return None

    def _render_template_preview_and_save(self, workflow_data: Dict[str, Any]):
        """Render preview of workflow and save options."""
        st.markdown("---")
        st.subheader("📋 Template Preview & Save")

        # Extract workflow information
        workflow_name = workflow_data.get('name', 'Unknown Workflow')
        workflow_version = workflow_data.get('version', '1.0')
        workflow_description = workflow_data.get('description', '')

        # Show workflow info
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Workflow Name:** {workflow_name}")
            st.write(f"**Version:** {workflow_version}")
        with col2:
            # Count blocks
            drawflow = workflow_data.get('drawflow', {})
            nodes = drawflow.get('nodes', {})
            if isinstance(nodes, dict):
                block_count = len(nodes)
            elif isinstance(nodes, list):
                block_count = len(nodes)
            else:
                block_count = 0

            st.write(f"**Total Blocks:** {block_count}")
            st.write(f"**Has Settings:** {'Yes' if workflow_data.get('settings') else 'No'}")

        if workflow_description:
            st.write(f"**Description:** {workflow_description}")

        # Template naming and categorization
        st.markdown("---")
        st.markdown("**Template Information**")

        col1, col2 = st.columns(2)

        with col1:
            template_name = st.text_input(
                "Template Name:",
                value=workflow_name,
                key="template_name_input",
                help="Give this template a descriptive name"
            )

        with col2:
            # Load existing categories from database
            existing_categories = self._get_existing_categories()

            # Add option to create new category
            category_options = existing_categories + ["➕ Create New Category"]

            selected_category = st.selectbox(
                "Template Category:",
                options=category_options,
                key="template_category",
                help="Categorize this template for easier organization"
            )

            # If user wants to create new category, show text input
            if selected_category == "➕ Create New Category":
                new_category = st.text_input(
                    "New Category Name:",
                    key="new_category_input",
                    placeholder="Enter new category name"
                )
                template_category = new_category
            else:
                template_category = selected_category

        template_description = st.text_area(
            "Template Description (Optional):",
            value=workflow_description,
            key="template_description_input",
            height=100,
            help="Add notes about when to use this template"
        )

        # Tags
        template_tags = st.text_input(
            "Tags (comma-separated):",
            key="template_tags",
            placeholder="e.g., twitter, automation, social-media",
            help="Add tags to help find this template later"
        )

        # Save button
        st.markdown("---")
        col1, col2, col3 = st.columns([2, 1, 1])

        with col1:
            if st.button("💾 Save Template", key="save_template_btn", type="primary", use_container_width=True):
                # Validate category
                if not template_category or template_category == "➕ Create New Category":
                    st.error("❌ Please provide a valid category name!")
                    return

                self._save_template(
                    workflow_data=workflow_data,
                    template_name=template_name,
                    template_category=template_category.lower(),
                    template_description=template_description,
                    template_tags=template_tags
                )

        with col2:
            if st.button("🔄 Reset", key="reset_template_btn", use_container_width=True):
                st.rerun()

        # Show raw JSON in expandable section
        with st.expander("🔍 View Raw Workflow JSON"):
            st.json(workflow_data)

    def _save_template(
        self,
        workflow_data: Dict[str, Any],
        template_name: str,
        template_category: str,
        template_description: str,
        template_tags: str
    ):
        """Save template to database."""
        if not template_name:
            st.error("❌ Please provide a template name!")
            return

        try:
            # Parse tags
            tags_list = [tag.strip() for tag in template_tags.split(',') if tag.strip()]

            # Create template document
            template_doc = {
                "template_name": template_name,
                "category": template_category.lower(),
                "description": template_description,
                "tags": tags_list,
                "workflow_data": workflow_data,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
                "version": workflow_data.get('version', '1.0'),
                "original_workflow_name": workflow_data.get('name', 'Unknown'),
                "block_count": self._count_blocks(workflow_data),
                "is_active": True
            }

            # Save to database
            if self.templates_collection is not None:
                result = self.templates_collection.insert_one(template_doc)

                if result.inserted_id:
                    st.success(f"✅ Template '{template_name}' saved successfully!")
                    st.info(f"📋 Template ID: {result.inserted_id}")

                    # Clear cache
                    if 'templates_cache' in st.session_state:
                        del st.session_state['templates_cache']
                    # Also clear categories cache
                    if 'template_categories_cache' in st.session_state:
                        del st.session_state['template_categories_cache']
                    st.rerun()
                else:
                    st.error("❌ Failed to save template")
            else:
                st.error("❌ Cannot connect to templates collection")

        except Exception as e:
            st.error(f"❌ Error saving template: {e}")
            import traceback
            st.code(traceback.format_exc())

    def _render_view_templates_section(self):
        """Render section for viewing existing templates."""
        st.subheader("📚 Saved Workflow Templates")

        # Refresh button
        col1, col2 = st.columns([4, 1])
        with col2:
            if st.button("🔄 Refresh", key="refresh_templates"):
                if 'templates_cache' in st.session_state:
                    del st.session_state['templates_cache']
                if 'template_categories_cache' in st.session_state:
                    del st.session_state['template_categories_cache']
                st.rerun()

        # Load templates
        templates = self._load_templates()

        if not templates:
            st.info("📭 No templates saved yet. Create your first template above!")
            return

        st.success(f"Found {len(templates)} templates")

        # Filters
        col1, col2, col3 = st.columns(3)

        with col1:
            # Get existing categories
            existing_categories = self._get_existing_categories()
            category_options = ["All"] + existing_categories

            category_filter = st.selectbox(
                "Filter by Category:",
                category_options,
                key="template_category_filter"
            )

        with col2:
            search_term = st.text_input(
                "Search templates:",
                key="template_search",
                placeholder="Search by name or tag..."
            )

        with col3:
            sort_by = st.selectbox(
                "Sort by:",
                ["Name (A-Z)", "Name (Z-A)", "Date (Newest)", "Date (Oldest)"],
                key="template_sort"
            )

        # Apply filters
        filtered_templates = self._filter_templates(templates, category_filter, search_term)
        filtered_templates = self._sort_templates(filtered_templates, sort_by)

        if not filtered_templates:
            st.warning("No templates match your filters")
            return

        st.markdown("---")

        # Display templates
        for template in filtered_templates:
            self._render_template_card(template)

    def _render_template_card(self, template: Dict[str, Any]):
        """Render a single template card."""
        template_id = str(template['_id'])
        template_name = template.get('template_name', 'Unknown')
        category = template.get('category', 'general').title()

        with st.expander(
            f"📋 {template_name} ({category})",
            expanded=False
        ):
            # Template info
            col1, col2, col3 = st.columns(3)

            with col1:
                st.write(f"**Category:** {category}")
                st.write(f"**Block Count:** {template.get('block_count', 0)}")

            with col2:
                st.write(f"**Version:** {template.get('version', 'N/A')}")
                created_at = template.get('created_at', '')
                if created_at:
                    try:
                        date_obj = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        st.write(f"**Created:** {date_obj.strftime('%Y-%m-%d')}")
                    except:
                        st.write(f"**Created:** {created_at[:10] if len(created_at) >= 10 else 'N/A'}")

            with col3:
                tags = template.get('tags', [])
                if tags:
                    st.write(f"**Tags:** {', '.join(tags)}")
                else:
                    st.write("**Tags:** None")

            # Description
            if template.get('description'):
                st.markdown("**Description:**")
                st.info(template['description'])

            # Actions
            st.markdown("---")
            col1, col2, col3, col4, col5 = st.columns(5)

            with col1:
                if st.button("📥 Download", key=f"download_template_{template_id}"):
                    self._download_template(template)

            with col2:
                if st.button("📊 Analyze", key=f"analyze_template_{template_id}"):
                    st.session_state[f'show_analysis_{template_id}'] = True

            with col3:
                if st.button("📋 View Details", key=f"view_template_{template_id}"):
                    st.session_state[f'show_details_{template_id}'] = True

            with col4:
                if st.button("✏️ Edit", key=f"edit_template_{template_id}"):
                    st.info("Edit functionality coming soon!")

            with col5:
                if st.button("🗑️ Delete", key=f"delete_template_{template_id}"):
                    self._delete_template(template_id)

            # Show analysis if requested
            if st.session_state.get(f'show_analysis_{template_id}', False):
                st.markdown("---")
                st.markdown("### 📊 Workflow Analysis")

                try:
                    from .workflow_analyzer_ui import render_workflow_analysis
                    render_workflow_analysis(template.get('workflow_data', {}), show_raw=False)
                except ImportError:
                    st.error("❌ Workflow analyzer not found. Please ensure workflow_analyzer_ui.py is installed.")
                except Exception as e:
                    st.error(f"❌ Error analyzing workflow: {e}")

            # Show details if requested
            if st.session_state.get(f'show_details_{template_id}', False):
                st.markdown("---")
                st.markdown("**Workflow Data:**")
                st.json(template.get('workflow_data', {}))

    def _load_templates(self) -> List[Dict[str, Any]]:
        """Load all templates from database."""
        if 'templates_cache' in st.session_state:
            return st.session_state['templates_cache']

        try:
            if self.templates_collection is not None:
                templates = list(self.templates_collection.find({"is_active": True}))
                st.session_state['templates_cache'] = templates
                return templates
            else:
                st.warning("Cannot connect to templates collection")
                return []
        except Exception as e:
            st.error(f"Error loading templates: {e}")
            return []

    def _filter_templates(
        self,
        templates: List[Dict[str, Any]],
        category: str,
        search_term: str
    ) -> List[Dict[str, Any]]:
        """Filter templates based on criteria."""
        filtered = templates

        # Category filter
        if category != "All":
            filtered = [t for t in filtered if t.get('category', '').lower() == category.lower()]

        # Search filter
        if search_term:
            search_lower = search_term.lower()
            filtered = [
                t for t in filtered
                if search_lower in t.get('template_name', '').lower()
                or search_lower in t.get('description', '').lower()
                or any(search_lower in tag.lower() for tag in t.get('tags', []))
            ]

        return filtered

    def _sort_templates(
        self,
        templates: List[Dict[str, Any]],
        sort_by: str
    ) -> List[Dict[str, Any]]:
        """Sort templates based on criteria."""
        if sort_by == "Name (A-Z)":
            return sorted(templates, key=lambda t: t.get('template_name', '').lower())
        elif sort_by == "Name (Z-A)":
            return sorted(templates, key=lambda t: t.get('template_name', '').lower(), reverse=True)
        elif sort_by == "Date (Newest)":
            return sorted(templates, key=lambda t: t.get('created_at', ''), reverse=True)
        elif sort_by == "Date (Oldest)":
            return sorted(templates, key=lambda t: t.get('created_at', ''))

        return templates

    def _download_template(self, template: Dict[str, Any]):
        """Download template as JSON file."""
        try:
            workflow_data = template.get('workflow_data', {})
            json_data = json.dumps(workflow_data, indent=2, default=str)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            template_name = template.get('template_name', 'template').replace(' ', '_')

            st.download_button(
                label="📥 Download Template JSON",
                data=json_data,
                file_name=f"template_{template_name}_{timestamp}.json",
                mime="application/json",
                key=f"download_btn_{template['_id']}_{timestamp}"
            )
        except Exception as e:
            st.error(f"Error preparing download: {e}")

    def _delete_template(self, template_id: str):
        """Soft delete a template."""
        try:
            if self.templates_collection is not None:
                result = self.templates_collection.update_one(
                    {"_id": ObjectId(template_id)},
                    {"$set": {
                        "is_active": False,
                        "deleted_at": datetime.now().isoformat()
                    }}
                )

                if result.modified_count > 0:
                    st.success("✅ Template deleted successfully!")
                    if 'templates_cache' in st.session_state:
                        del st.session_state['templates_cache']
                    if 'template_categories_cache' in st.session_state:
                        del st.session_state['template_categories_cache']
                    st.rerun()
                else:
                    st.error("❌ Failed to delete template")
            else:
                st.error("❌ Cannot connect to templates collection")
        except Exception as e:
            st.error(f"❌ Error deleting template: {e}")
            import traceback
            st.code(traceback.format_exc())

    def _count_blocks(self, workflow_data: Dict[str, Any]) -> int:
        """Count the number of blocks in a workflow."""
        drawflow = workflow_data.get('drawflow', {})
        nodes = drawflow.get('nodes', {})

        if isinstance(nodes, dict):
            return len(nodes)
        elif isinstance(nodes, list):
            return len(nodes)
        else:
            return 0

    def _get_existing_categories(self) -> List[str]:
        """Get all unique categories from existing templates."""
        # Check cache first
        if 'template_categories_cache' in st.session_state:
            return st.session_state['template_categories_cache']

        try:
            if self.templates_collection is not None:
                # Get distinct categories from active templates
                categories = self.templates_collection.distinct(
                    "category",
                    {"is_active": True}
                )
                # Remove empty strings and None, then sort alphabetically
                categories = [cat.title() for cat in categories if cat and isinstance(cat, str)]
                categories.sort()

                # Cache the result
                st.session_state['template_categories_cache'] = categories
                return categories
            else:
                return []
        except Exception as e:
            st.error(f"Error loading categories: {e}")
            return []
