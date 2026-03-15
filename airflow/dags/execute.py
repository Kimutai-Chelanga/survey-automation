"""
Executor DAG - Smart Scheduler Version
Checks extraction settings every hour and runs only at configured times
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import os
import sys

# Add src to path for settings access
sys.path.insert(0, '/opt/airflow/src')

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 30),  # Updated to current date
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Configuration
JS_EXECUTOR_PATH = os.environ.get('JS_EXECUTOR_PATH', '/opt/airflow/src/scripts/execute')


def check_should_run_extraction(**context):
    """
    Check if extraction should run based on configured schedule.
    Returns True if should run, False if should skip.
    """
    from settings.settings_manager import get_extraction_setting

    try:
        # Get current time (Airflow execution time)
        execution_date = context.get('execution_date')
        if execution_date:
            now = execution_date
        else:
            now = datetime.now()

        current_date = now.date()
        current_time = now.strftime("%H:%M")
        current_hour = now.strftime("%H")

        print(f"⏰ Checking extraction schedule for {current_date} at {current_time}")

        # Load extraction schedule
        extraction_cfg = get_extraction_setting(
            "extraction_schedule",
            {"global_url": "", "dates": {}}
        )

        # Check if today has a schedule
        date_key = current_date.isoformat()
        day_cfg = extraction_cfg.get("dates", {}).get(date_key, {})

        if not day_cfg:
            print(f"❌ No extraction schedule configured for {date_key}")
            return False

        # Get configured times for today
        times = day_cfg.get("times", [])
        if not times:
            print(f"❌ No extraction times configured for {date_key}")
            return False

        print(f"📋 Configured times for today: {[t.get('time') for t in times]}")

        # Check if current hour matches any configured time
        for time_entry in times:
            scheduled_time = time_entry.get("time", "")
            scheduled_hour = scheduled_time.split(":")[0] if ":" in scheduled_time else ""

            if scheduled_hour == current_hour:
                url = extraction_cfg.get('global_url', 'Not set')
                print(f"✅ MATCH FOUND!")
                print(f"   Current time: {current_time}")
                print(f"   Scheduled time: {scheduled_time}")
                print(f"   Extraction URL: {url}")
                return True

        print(f"⏭️  No match: Current hour {current_hour} not in scheduled hours")
        return False

    except Exception as e:
        print(f"❌ ERROR checking schedule: {e}")
        import traceback
        traceback.print_exc()
        return False


# Initialize the smart scheduler DAG
with DAG(
    'executor',  # This ID matches what the trigger is looking for
    default_args=default_args,
    description='Smart scheduler: Checks settings hourly and runs at configured times',
    schedule_interval='@hourly',  # Check every hour
    start_date=datetime(2025, 1, 30),
    catchup=False,
    max_active_runs=1,
    tags=['workflow', 'hyperbrowser', 'javascript', 'modular', 'smart-scheduler']
) as dag:

    # Start marker
    start = DummyOperator(
        task_id='start',
        dag=dag
    )

    # Check if should run based on settings
    check_schedule = ShortCircuitOperator(
        task_id='check_schedule',
        python_callable=check_should_run_extraction,
        provide_context=True,
        dag=dag
    )

    # Health check task
    health_check_task = BashOperator(
        task_id='health_check',
        bash_command=f'''
            cd {JS_EXECUTOR_PATH} && \
            echo "=== Health Check ===" && \
            echo "Node version: $(node --version)" && \
            echo "NPM version: $(npm --version)" && \
            echo "Checking package.json..." && \
            [ -f package.json ] && echo "✓ package.json found" || echo "✗ package.json missing" && \
            echo "Checking node_modules..." && \
            [ -d node_modules ] && echo "✓ node_modules found" || echo "⚠ node_modules missing - run npm install" && \
            echo "Health check completed"
        ''',
        env={
            'HYPERBROWSER_API_KEY': os.environ.get('HYPERBROWSER_API_KEY', ''),
            'MONGODB_URI': os.environ.get('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
            'NODE_ENV': 'production'
        },
        dag=dag,
        retries=0,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Main workflow execution task
    # Main workflow execution task
    execute_workflows_task = BashOperator(
        task_id='working_executor',
        bash_command=f'cd {JS_EXECUTOR_PATH} && node orchestrator.js',
        env={
            # Core Hyperbrowser configuration
            'HYPERBROWSER_API_KEY': os.environ.get('HYPERBROWSER_API_KEY', ''),
            'HYPERBROWSER_MAX_STEPS': os.environ.get('HYPERBROWSER_MAX_STEPS', '25'),
            'AUTOMA_EXTENSION_ID': os.environ.get('AUTOMA_EXTENSION_ID', 'infppggnoaenmfagbfknfkancpbljcca'),

            # Database configuration
            'MONGODB_URI': os.environ.get('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
            'MONGODB_DB_NAME': os.environ.get('MONGODB_DB_NAME', 'messages_db'),
            'POSTGRES_HOST': os.environ.get('POSTGRES_HOST', 'postgres'),
            'POSTGRES_PORT': os.environ.get('POSTGRES_PORT', '5432'),
            'POSTGRES_DB': os.environ.get('POSTGRES_DB', 'messages'),
            'POSTGRES_USER': os.environ.get('POSTGRES_USER', 'airflow'),
            'POSTGRES_PASSWORD': os.environ.get('POSTGRES_PASSWORD', 'airflow'),

            # ✅ NEW: SUCCESS/FAILURE TRACKING CONFIGURATION
            'TRACK_SUCCESS_FAILURE': 'true',
            'UPDATE_POSTGRES_SUCCESS': 'true',
            'UPDATE_POSTGRES_FAILURE': 'true',
            'VALIDATE_DATA_CONSISTENCY': 'true',

            # Execution configuration
            'AIRFLOW_CTX_DAG_RUN_ID': '{{ dag_run.run_id }}',
            'AIRFLOW_CTX_EXECUTION_DATE': '{{ ds }}',
            'AIRFLOW_CTX_TASK_ID': '{{ task_instance.task_id }}',

            # ✅ NEW: DAG EXECUTION SETTINGS (from Streamlit UI)
            'DAG_EXECUTION_MODE': 'local_executor',  # or 'filter_links_report' based on UI config
            'TRIGGER_REPORT_DAG': 'true',  # Whether to trigger reporting DAG

            # Timezone configuration (matches Streamlit UI)
            'EXECUTION_TIMEZONE': 'Africa/Nairobi',
            'EXECUTION_DAY_OVERRIDE': '',  # Empty = use current day

            # Daily execution settings (loaded from Streamlit)
            'DESTINATION_CATEGORY': '',  # Will be populated by orchestrator
            'WORKFLOW_TYPE_NAME': '',    # Will be populated by orchestrator
            'COLLECTION_NAME': '',       # Will be populated by orchestrator
            'MAX_WORKFLOWS': '50',       # Default, overridden by settings
            'GAP_SECONDS': '30',         # Default, overridden by settings

            # Environment settings
            'NODE_ENV': 'production',
            'LOG_LEVEL': os.environ.get('LOG_LEVEL', 'info'),

            # ✅ NEW: Detailed logging for success/failure tracking
            'LOG_SUCCESS_DETAILS': 'true',
            'LOG_FAILURE_DETAILS': 'true',
            'LOG_DATA_VALIDATION': 'true',

            # Optional workflow settings
            'WORKFLOW_GAP_SECONDS': os.environ.get('WORKFLOW_GAP_SECONDS', '15'),
            'MAX_CONCURRENT_WORKFLOWS': os.environ.get('MAX_CONCURRENT_WORKFLOWS', '1'),
            'ENABLE_VIDEO_RECORDING': os.environ.get('ENABLE_VIDEO_RECORDING', 'true'),
            'CONTINUE_ON_FAILURE': os.environ.get('CONTINUE_ON_FAILURE', 'true'),

            # ✅ NEW: Failure handling
            'MAX_FAILURES_BEFORE_STOP': '5',
            'RETRY_FAILED_WORKFLOWS': 'false',
            'STORE_FAILURE_REASONS': 'true',

            # ✅ NEW: Post-execution reporting
            'GENERATE_EXECUTION_SUMMARY': 'true',
            'SEND_SUCCESS_STATS_TO_MONGO': 'true',
            'VALIDATE_POSTGRES_UPDATES': 'true'
        },
        dag=dag,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=2)
    )

    # Cleanup task
    cleanup_task = BashOperator(
        task_id='cleanup_temp_files',
        bash_command=f'''
            cd {JS_EXECUTOR_PATH} && \
            echo "=== Cleanup Started ===" && \
            (npm run clean 2>&1 || echo "Cleanup script not found, skipping...") && \
            echo "=== Cleanup Completed ===" && \
            exit 0
        ''',
        env={
            'NODE_ENV': 'production'
        },
        dag=dag,
        retries=0,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # End marker
    end = DummyOperator(
        task_id='end',
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Set task dependencies
    # The check_schedule uses ShortCircuit - if it returns False, downstream tasks are skipped
    start >> check_schedule >> health_check_task >> execute_workflows_task >> cleanup_task >> end
