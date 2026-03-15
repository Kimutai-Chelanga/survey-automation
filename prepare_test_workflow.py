#!/usr/bin/env python3
"""
Workflow Testing Preparation Script
Loads workflows from workflows_testing folder and prepares them for execution
"""

import os
import json
from pathlib import Path
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId

# Configuration
WORKFLOWS_TESTING_FOLDER = Path("workflows_testing")
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://admin:admin123@localhost:27017/messages_db?authSource=admin")
MONGODB_DB_NAME = os.environ.get("MONGODB_DB_NAME", "messages_db")

# Account configuration for testing
DEFAULT_ACCOUNT_ID = 1  # brian's account
DEFAULT_USERNAME = "brian"


class WorkflowPreparer:
    def __init__(self, workflows_folder, mongo_uri, db_name):
        self.workflows_folder = Path(workflows_folder)
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.client = None
        self.db = None
        
    def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            # Test connection
            self.db.command('ping')
            print(f"✓ Connected to MongoDB: {self.db_name}")
            return True
        except Exception as e:
            print(f"✗ Failed to connect to MongoDB: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
            print("✓ Disconnected from MongoDB")
    
    def load_workflows_from_folder(self):
        """Load all workflow JSON files from the workflows_testing folder"""
        if not self.workflows_folder.exists():
            print(f"✗ Workflows folder not found: {self.workflows_folder}")
            return []
        
        workflow_files = list(self.workflows_folder.glob("*.json"))
        print(f"\n📂 Found {len(workflow_files)} workflow file(s) in {self.workflows_folder}")
        
        workflows = []
        for file_path in workflow_files:
            try:
                with open(file_path, 'r') as f:
                    workflow_data = json.load(f)
                    workflows.append({
                        'file_name': file_path.name,
                        'file_path': str(file_path),
                        'data': workflow_data
                    })
                    print(f"  ✓ Loaded: {file_path.name}")
            except Exception as e:
                print(f"  ✗ Error loading {file_path.name}: {e}")
        
        return workflows
    
    def ensure_automa_workflows_exist(self, workflows):
        """Ensure automa_workflows collection has entries for each workflow"""
        automa_collection = self.db['automa_workflows']
        created = 0
        existing = 0
        
        print("\n📝 Checking/Creating Automa Workflows...")
        
        for workflow in workflows:
            data = workflow['data']
            workflow_name = data.get('name', workflow['file_name'].replace('.json', ''))
            
            # Check if workflow already exists
            existing_workflow = automa_collection.find_one({
                'name': workflow_name
            })
            
            if existing_workflow:
                workflow['automa_id'] = existing_workflow['_id']
                existing += 1
                print(f"  ✓ Found existing: {workflow_name}")
            else:
                # Create new automa_workflows entry
                automa_doc = {
                    'name': workflow_name,
                    'workflow_data': data,
                    'created_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow(),
                    'source': 'testing',
                    'description': f"Test workflow from {workflow['file_name']}",
                    'is_active': True
                }
                
                result = automa_collection.insert_one(automa_doc)
                workflow['automa_id'] = result.inserted_id
                created += 1
                print(f"  ✓ Created: {workflow_name} (ID: {result.inserted_id})")
        
        print(f"\n📊 Automa Workflows: {created} created, {existing} existing")
        return workflows
    
    def create_workflow_executions(self, workflows, account_id, username):
        """Create workflow_executions entries ready for execution"""
        executions_collection = self.db['workflow_executions']
        created_executions = []
        
        print(f"\n🚀 Creating Workflow Executions for account: {username} (ID: {account_id})")
        
        # Get account profile info
        account = self.db['accounts'].find_one({'postgres_account_id': account_id})
        if not account:
            print(f"  ⚠️  Warning: Account {account_id} not found in database")
            return []
        
        profile_id = account.get('profile_id')
        if not profile_id:
            print(f"  ✗ Account {account_id} does not have a profile_id configured")
            return []
        
        print(f"  ✓ Using profile: {profile_id}")
        
        for workflow in workflows:
            data = workflow['data']
            workflow_name = data.get('name', workflow['file_name'].replace('.json', ''))
            
            # Create workflow_executions entry
            execution_doc = {
                'postgres_account_id': account_id,
                'account_username': username,
                'profile_id': profile_id,
                'automa_workflow_id': workflow['automa_id'],
                'workflow_name': workflow_name,
                'workflow_type': 'test_execution',
                'content_type': 'test',
                'postgres_content_id': f"test_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                
                # Execution flags
                'has_link': True,
                'executed': False,
                'execution_success': None,
                'execution_mode': 'manual_test',
                
                # Metadata
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow(),
                'source_file': workflow['file_name'],
                'test_workflow': True,
                
                # Will be populated during execution
                'executed_at': None,
                'execution_time': None,
                'final_result': None,
                'execution_error': None,
                'video_recording_session_id': None
            }
            
            result = executions_collection.insert_one(execution_doc)
            created_executions.append({
                'id': result.inserted_id,
                'workflow_name': workflow_name,
                'account': username
            })
            print(f"  ✓ Created execution: {workflow_name} (ID: {result.inserted_id})")
        
        print(f"\n✅ Created {len(created_executions)} workflow execution(s)")
        return created_executions
    
    def clear_existing_test_workflows(self):
        """Clear any existing test workflow executions"""
        executions_collection = self.db['workflow_executions']
        
        result = executions_collection.delete_many({
            'test_workflow': True,
            'executed': False
        })
        
        if result.deleted_count > 0:
            print(f"🧹 Cleared {result.deleted_count} existing test workflow(s)")
        
        return result.deleted_count
    
    def get_execution_summary(self):
        """Get summary of workflows ready for execution"""
        executions_collection = self.db['workflow_executions']
        
        pending = executions_collection.count_documents({
            'test_workflow': True,
            'executed': False
        })
        
        executed = executions_collection.count_documents({
            'test_workflow': True,
            'executed': True
        })
        
        print(f"\n📊 Workflow Execution Summary:")
        print(f"  Pending: {pending}")
        print(f"  Executed: {executed}")
        
        if pending > 0:
            print(f"\n✨ Ready to execute! Run your DAG to process {pending} workflow(s)")
        
        return {'pending': pending, 'executed': executed}


def main():
    print("=" * 70)
    print("🧪 WORKFLOW TESTING PREPARATION SCRIPT")
    print("=" * 70)
    
    preparer = WorkflowPreparer(
        workflows_folder=WORKFLOWS_TESTING_FOLDER,
        mongo_uri=MONGODB_URI,
        db_name=MONGODB_DB_NAME
    )
    
    # Connect to MongoDB
    if not preparer.connect():
        return
    
    try:
        # Clear existing test workflows
        preparer.clear_existing_test_workflows()
        
        # Load workflows from folder
        workflows = preparer.load_workflows_from_folder()
        
        if not workflows:
            print("\n⚠️  No workflows found to prepare")
            return
        
        # Ensure automa_workflows entries exist
        workflows = preparer.ensure_automa_workflows_exist(workflows)
        
        # Create workflow_executions entries
        executions = preparer.create_workflow_executions(
            workflows=workflows,
            account_id=DEFAULT_ACCOUNT_ID,
            username=DEFAULT_USERNAME
        )
        
        # Show summary
        preparer.get_execution_summary()
        
        print("\n" + "=" * 70)
        print("✅ PREPARATION COMPLETE")
        print("=" * 70)
        print("\n💡 Next steps:")
        print("   1. Go to Airflow UI (http://localhost:8080)")
        print("   2. Find the 'execute' DAG")
        print("   3. Click 'Trigger DAG' to run your test workflows")
        print("   4. Monitor execution in Airflow logs")
        print("\n")
        
    finally:
        preparer.disconnect()


if __name__ == "__main__":
    main()