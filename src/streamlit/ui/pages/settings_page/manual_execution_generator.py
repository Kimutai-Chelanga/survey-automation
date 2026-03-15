import streamlit as st
from datetime import date, datetime
import json
import os
import zipfile
from io import BytesIO
from bson import ObjectId
from pymongo import MongoClient


class ManualExecutionGenerator:
    """Generates manual execution workflows from filtered workflows."""

    def __init__(self):
        self.mongo_uri = os.getenv(
            'MONGODB_URI',
            'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'
        )

    def get_mongo_connection(self):
        """Get MongoDB connection."""
        return MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)

    def convert_objectid_to_str(self, obj):
        """
        Recursively convert ObjectId instances to strings in nested structures.
        """
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, dict):
            return {key: self.convert_objectid_to_str(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_objectid_to_str(item) for item in obj]
        else:
            return obj

    def get_filtered_workflows(self, category=None, workflow_type=None, collection_name=None):
        """
        Get workflows that were assigned links today (from filter_links DAG).

        Returns workflows with their metadata including:
        - Workflow name and ID
        - Category and type
        - Collection location
        - Link assignment info
        """
        try:
            client = self.get_mongo_connection()
            db = client['messages_db']

            # Build query for workflows assigned links today
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

            query = {
                'has_link': True,
                'link_assigned_at': {'$gte': today}
            }

            # Add filters if provided
            if category:
                query['category'] = category.lower()
            if workflow_type:
                query['workflow_type'] = workflow_type
            if collection_name:
                query['collection_name'] = collection_name

            # Get workflow metadata
            workflows = list(db.workflow_metadata.find(
                query,
                {
                    'automa_workflow_id': 1,
                    'workflow_name': 1,
                    'workflow_type': 1,
                    'category': 1,
                    'collection_name': 1,
                    'database_name': 1,
                    'link_url': 1,
                    'account_id': 1,
                    'link_assigned_at': 1,
                    'artifacts_folder': 1
                }
            ).sort('link_assigned_at', 1))

            if not workflows:
                return []

            # Fetch actual workflow documents from their collections
            result = []
            for meta in workflows:
                database_name = meta.get('database_name', 'execution_workflows')
                collection_name = meta.get('collection_name')

                if not collection_name:
                    st.warning(f"No collection for workflow {meta['workflow_name']}")
                    continue

                # Get actual workflow document
                workflow_db = client[database_name]
                workflow_collection = workflow_db[collection_name]

                workflow_doc = workflow_collection.find_one(
                    {'_id': meta['automa_workflow_id']},
                    {'name': 1, 'drawflow': 1, 'settings': 1, 'globalData': 1, 'description': 1}
                )

                if workflow_doc:
                    # Convert ObjectIds to strings
                    workflow_doc = self.convert_objectid_to_str(workflow_doc)
                    meta = self.convert_objectid_to_str(meta)

                    result.append({
                        'metadata': meta,
                        'workflow': workflow_doc
                    })

            client.close()
            return result

        except Exception as e:
            st.error(f"Error fetching filtered workflows: {e}")
            return []

    def load_execution_template(self):
        """Load the manual execution template."""
        template = {
            "extVersion": "1.29.12",
            "name": "exec",
            "icon": "riGlobalLine",
            "table": [],
            "version": "1.29.12",
            "drawflow": {
                "nodes": [
                    {
                        "id": "v4CVdjgfnR5iqNA7zb7Ti",
                        "type": "BlockBasic",
                        "initialized": False,
                        "position": {"x": 7.69, "y": 220.38},
                        "data": {
                            "disableBlock": False,
                            "description": "",
                            "type": "manual",
                            "interval": 60,
                            "delay": 5,
                            "date": "",
                            "time": "00:00",
                            "url": "",
                            "shortcut": "",
                            "activeInInput": False,
                            "isUrlRegex": False,
                            "days": [],
                            "contextMenuName": "",
                            "contextTypes": [],
                            "parameters": [],
                            "preferParamsInTab": False
                        },
                        "label": "trigger"
                    }
                ],
                "edges": [],
                "position": [0, 0],
                "zoom": 1.3,
                "viewport": {"x": 0, "y": 0, "zoom": 1.3}
            },
            "settings": {
                "publicId": "",
                "aipowerToken": "",
                "blockDelay": 0,
                "saveLog": True,
                "debugMode": False,
                "restartTimes": 3,
                "notification": True,
                "execContext": "popup",
                "reuseLastState": False,
                "inputAutocomplete": True,
                "onError": "stop-workflow",
                "executedBlockOnWeb": False,
                "insertDefaultColumn": False,
                "defaultColumnName": "column"
            },
            "globalData": '{\n\t"key": "value"\n}',
            "description": "",
            "includedWorkflows": {}
        }
        return template

    def generate_execute_workflow_node(self, workflow_id, workflow_name, position_x, position_y):
        """Generate an execute-workflow node."""
        import uuid
        node_id = str(uuid.uuid4())[:8]

        return {
            "id": node_id,
            "type": "BlockBasic",
            "initialized": False,
            "position": {"x": position_x, "y": position_y},
            "data": {
                "disableBlock": False,
                "executeId": "",
                "workflowId": workflow_id,
                "globalData": "",
                "description": workflow_name,
                "insertAllVars": True,
                "insertAllGlobalData": False
            },
            "label": "execute-workflow"
        }

    def generate_edge(self, source_id, target_id, source_x, source_y, target_x, target_y):
        """Generate an edge between two nodes."""
        edge_id = f"vueflow__edge-{source_id}{source_id}-output-1-{target_id}{target_id}-input-1"

        return {
            "id": edge_id,
            "type": "custom",
            "source": source_id,
            "target": target_id,
            "sourceHandle": f"{source_id}-output-1",
            "targetHandle": f"{target_id}-input-1",
            "updatable": True,
            "selectable": True,
            "data": {},
            "label": "",
            "markerEnd": "arrowclosed",
            "sourceX": source_x + 215,
            "sourceY": source_y + 36,
            "targetX": target_x - 24,
            "targetY": target_y + 36
        }

    def create_individual_execution_workflow(self, workflow_doc):
        """Create a single execution workflow (trigger -> workflow)."""
        workflow_id = str(workflow_doc['_id'])
        workflow_name = workflow_doc.get('name', 'Unnamed')

        # Generate execute node
        exec_node = self.generate_execute_workflow_node(
            workflow_id,
            workflow_name,
            380,
            216
        )

        individual_exec = {
            "extVersion": "1.29.12",
            "name": f"exec_{workflow_name}",
            "icon": "riGlobalLine",
            "table": [],
            "version": "1.29.12",
            "drawflow": {
                "nodes": [
                    # Trigger node
                    {
                        "id": "trigger_node",
                        "type": "BlockBasic",
                        "initialized": False,
                        "position": {"x": 10, "y": 220},
                        "data": {
                            "disableBlock": False,
                            "type": "manual",
                            "interval": 60,
                            "delay": 5
                        },
                        "label": "trigger"
                    },
                    # Execute workflow node
                    exec_node
                ],
                "edges": [
                    self.generate_edge(
                        "trigger_node",
                        exec_node['id'],
                        10, 220,
                        380, 216
                    )
                ],
                "position": [0, 0],
                "zoom": 1.3,
                "viewport": {"x": 0, "y": 0, "zoom": 1.3}
            },
            "settings": {
                "blockDelay": 0,
                "saveLog": True,
                "debugMode": False,
                "onError": "stop-workflow"
            },
            "globalData": '{\n\t"key": "value"\n}',
            "description": f"Execute {workflow_name}",
            "includedWorkflows": {
                workflow_id: workflow_doc
            }
        }

        return individual_exec

    def create_master_execution_workflow(self, workflow_docs):
        """
        Create master execution workflow that runs all workflows sequentially.

        Layout: trigger -> workflow1 -> workflow2 -> workflow3 -> ...
        """
        template = self.load_execution_template()

        # Get trigger node
        trigger_node = template['drawflow']['nodes'][0]
        nodes = [trigger_node]
        edges = []

        # Starting positions
        x_position = 380
        y_position = 216
        x_gap = 250

        previous_node_id = trigger_node['id']
        previous_x = trigger_node['position']['x']
        previous_y = trigger_node['position']['y']

        # Create execute-workflow nodes for each workflow
        for idx, wf_data in enumerate(workflow_docs):
            workflow_doc = wf_data['workflow']
            workflow_id = str(workflow_doc['_id'])
            workflow_name = workflow_doc.get('name', f'Workflow_{idx+1}')

            # Create execute node
            exec_node = self.generate_execute_workflow_node(
                workflow_id,
                workflow_name,
                x_position,
                y_position
            )

            nodes.append(exec_node)

            # Create edge from previous node to this node
            edge = self.generate_edge(
                previous_node_id,
                exec_node['id'],
                previous_x,
                previous_y,
                x_position,
                y_position
            )
            edges.append(edge)

            # Add workflow to includedWorkflows
            template['includedWorkflows'][workflow_id] = workflow_doc

            # Update positions for next node
            previous_node_id = exec_node['id']
            previous_x = x_position
            previous_y = y_position
            x_position += x_gap

        # Update template
        template['drawflow']['nodes'] = nodes
        template['drawflow']['edges'] = edges
        template['name'] = f"master_execution_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        template['description'] = f"Execute {len(workflow_docs)} workflows sequentially"

        return template

    def create_execution_package(self, filtered_workflows):
        """
        Create complete execution package:
        - Master execution workflow
        - Individual execution workflows
        - Original workflow files
        - Metadata file
        """
        package = {
            'master_execution': None,
            'individual_executions': [],
            'workflows': [],
            'metadata': {
                'created_at': datetime.now().isoformat(),
                'total_workflows': len(filtered_workflows),
                'workflow_names': []
            }
        }

        # Extract workflow documents
        workflow_docs = []
        for wf_data in filtered_workflows:
            workflow_doc = wf_data['workflow']
            metadata = wf_data['metadata']

            workflow_docs.append(wf_data)

            # Add to package
            package['workflows'].append({
                'name': workflow_doc.get('name', 'Unknown'),
                'workflow': workflow_doc,
                'metadata': {
                    'category': metadata.get('category', ''),
                    'workflow_type': metadata.get('workflow_type', ''),
                    'link_url': metadata.get('link_url', ''),
                    'account_id': metadata.get('account_id', 1)
                }
            })

            package['metadata']['workflow_names'].append(
                workflow_doc.get('name', 'Unknown')
            )

            # Create individual execution workflow
            individual_exec = self.create_individual_execution_workflow(workflow_doc)
            package['individual_executions'].append({
                'name': individual_exec['name'],
                'workflow': individual_exec
            })

        # Create master execution workflow
        package['master_execution'] = self.create_master_execution_workflow(workflow_docs)

        return package

    def create_zip_download(self, package):
        """Create a ZIP file with all workflows."""
        zip_buffer = BytesIO()

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # 1. Master execution workflow
            master_name = package['master_execution']['name']
            zip_file.writestr(
                f"master_execution/{master_name}.json",
                json.dumps(package['master_execution'], indent=2)
            )

            # 2. Individual execution workflows
            for exec_wf in package['individual_executions']:
                zip_file.writestr(
                    f"individual_executions/{exec_wf['name']}.json",
                    json.dumps(exec_wf['workflow'], indent=2)
                )

            # 3. Original workflows
            for wf in package['workflows']:
                safe_name = wf['name'].replace('/', '_').replace('\\', '_')
                zip_file.writestr(
                    f"workflows/{safe_name}.json",
                    json.dumps(wf['workflow'], indent=2)
                )

            # 4. Metadata file
            zip_file.writestr(
                "metadata.json",
                json.dumps(package['metadata'], indent=2)
            )

            # 5. README file
            readme = f"""# Manual Execution Package
Generated: {package['metadata']['created_at']}
Total Workflows: {package['metadata']['total_workflows']}

## Structure:
- master_execution/ - Master workflow that executes all workflows sequentially
- individual_executions/ - Individual execution workflows for each workflow
- workflows/ - Original workflow files
- metadata.json - Package metadata

## Workflows Included:
{chr(10).join([f"- {name}" for name in package['metadata']['workflow_names']])}

## Usage:
1. Import the master execution workflow into Automa
2. Run it to execute all workflows sequentially

Or:
1. Import individual execution workflows for selective execution
"""
            zip_file.writestr("README.md", readme)

        zip_buffer.seek(0)
        return zip_buffer


def render_manual_execution_generator():
    """Render the manual execution generator UI."""
    st.subheader("🚀 Manual Execution Workflow Generator")
    st.markdown("""
    Generate manual execution workflows from filtered workflows that were assigned links today.

    **This tool will:**
    1. Fetch workflows that were filtered and assigned links
    2. Create individual execution workflows for each
    3. Create a master execution workflow that runs all sequentially
    4. Package everything into a downloadable ZIP file
    """)

    generator = ManualExecutionGenerator()

    # Get filtering configuration
    from ...settings.settings_manager import get_filtering_settings
    filtering_config = get_filtering_settings()

    st.info(f"""
    **Current Filtering Configuration:**
    - Category: {filtering_config.get('destination_category', 'Not set')}
    - Type: {filtering_config.get('workflow_type_name', 'Not set')}
    - Collection: {filtering_config.get('collection_name', 'Not set')}
    """)

    # Fetch filtered workflows
    with st.spinner("Fetching filtered workflows..."):
        filtered_workflows = generator.get_filtered_workflows(
            category=filtering_config.get('destination_category'),
            workflow_type=filtering_config.get('workflow_type_name'),
            collection_name=filtering_config.get('collection_name')
        )

    if not filtered_workflows:
        st.warning("""
        ⚠️ No workflows found that were assigned links today.

        **Possible reasons:**
        - The filter_links DAG hasn't run yet today
        - No workflows matched the current filtering configuration
        - All workflows already have links assigned

        **Try:**
        1. Run the filter_links DAG manually
        2. Check your filtering configuration
        3. Verify workflows exist in the specified collection
        """)
        return

    # Display found workflows
    st.success(f"✅ Found {len(filtered_workflows)} filtered workflows")

    with st.expander(f"📋 View {len(filtered_workflows)} Workflows", expanded=True):
        for idx, wf_data in enumerate(filtered_workflows, 1):
            meta = wf_data['metadata']
            wf = wf_data['workflow']

            col1, col2, col3 = st.columns([2, 2, 1])

            with col1:
                st.write(f"**{idx}. {wf.get('name', 'Unknown')}**")
                st.caption(f"ID: {str(meta['automa_workflow_id'])[:8]}...")

            with col2:
                st.write(f"Category: {meta.get('category', 'N/A')}")
                st.write(f"Type: {meta.get('workflow_type', 'N/A')}")

            with col3:
                st.write(f"Account: {meta.get('account_id', 1)}")

            st.caption(f"🔗 Link: {meta.get('link_url', 'N/A')[:50]}...")
            st.markdown("---")

    # Generation options
    st.subheader("⚙️ Generation Options")

    col1, col2 = st.columns(2)

    with col1:
        include_master = st.checkbox(
            "Include Master Execution Workflow",
            value=True,
            help="Creates a single workflow that executes all workflows sequentially"
        )

    with col2:
        include_individual = st.checkbox(
            "Include Individual Execution Workflows",
            value=True,
            help="Creates separate execution workflows for each workflow"
        )

    # Generate button
    st.markdown("---")

    if st.button("🎯 Generate Execution Package", type="primary", use_container_width=True):
        if not include_master and not include_individual:
            st.error("⚠️ Please select at least one generation option")
            return

        with st.spinner("Generating execution package..."):
            try:
                # Create package
                package = generator.create_execution_package(filtered_workflows)

                # Create ZIP file
                zip_buffer = generator.create_zip_download(package)

                # Display success
                st.success(f"""
                ✅ Execution package generated successfully!

                **Package Contents:**
                - Master Execution: {package['master_execution']['name']}
                - Individual Executions: {len(package['individual_executions'])}
                - Original Workflows: {len(package['workflows'])}
                - Total Files: {len(package['individual_executions']) + len(package['workflows']) + 2}
                """)

                # Download button
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"manual_execution_package_{timestamp}.zip"

                st.download_button(
                    label="📥 Download Execution Package",
                    data=zip_buffer,
                    file_name=filename,
                    mime="application/zip",
                    use_container_width=True
                )

                # Show package details
                with st.expander("📊 Package Details"):
                    st.json(package['metadata'])

                st.balloons()

            except Exception as e:
                st.error(f"❌ Error generating package: {e}")
                import traceback
                st.code(traceback.format_exc())
