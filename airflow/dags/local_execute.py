"""
Local Chrome Multi-Profile Dynamic Orchestrator DAG

WHAT'S NEW:
- Uses OrchestratorBuilder to create a master workflow dynamically
- Executes ONE orchestrator workflow that runs all eligible workflows
- Randomized delays between workflows (configured in Streamlit)
- More efficient than executing workflows one-by-one
- Comprehensive logging of orchestrator execution

ARCHITECTURE:
1. Fetch eligible workflows from MongoDB (based on execution settings)
2. Build dynamic orchestrator workflow with all workflows + delays
3. Execute single orchestrator workflow in Chrome
4. Orchestrator internally runs all workflows sequentially
5. Track success/failure and update databases

FIX HISTORY (2026-02-15):
  - Added PATH, HOME, USER to execute_dynamic_orchestrator env block so that
    `node` resolves correctly when Airflow spawns the BashOperator subprocess.
  - Wrapped node invocation in bash that explicitly prints exit code so silent
    exits are visible in Airflow logs.
  - Added --unhandled-rejections=strict Node flag so promise rejections always
    become fatal errors with a non-zero exit code.
  - Added stderr capture to the bash command (2>&1) so Node errors appear in
    Airflow task logs rather than being silently discarded.

✅ PERMISSION FIXES (NO ROOT REQUIRED):
  - Uses aggressive chmod 777/666 on all files
  - Removes ALL Chrome lock files before startup
  - Safe process cleanup using pgrep + kill
  - Ensures /tmp/.X11-unix exists with correct permissions
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta, datetime
import os

# ─── Default arguments ───────────────────────────────────────────────────────
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 31),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ─── Configuration ────────────────────────────────────────────────────────────
JS_EXECUTOR_PATH        = os.environ.get('JS_EXECUTOR_PATH',   '/opt/airflow/src/scripts/local_execute')
CHROME_PROFILE_BASE_DIR = os.environ.get('CHROME_PROFILE_DIR', '/workspace/chrome_profiles')
RECORDINGS_DIR          = os.environ.get('RECORDINGS_DIR',     '/workspace/recordings')
CHROME_EXECUTABLE       = os.environ.get('CHROME_EXECUTABLE',  '/usr/bin/google-chrome-stable')
DOWNLOADS_DIR           = os.environ.get('DOWNLOADS_DIR',      '/workspace/downloads')
MONGODB_URI             = os.environ.get('MONGODB_URI',        'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin')


# ─── Python tasks ─────────────────────────────────────────────────────────────

def verify_profiles_exist(**context):
    """
    Verify Local Chrome profiles exist in MongoDB (created by Streamlit).
    Uses aggressive permissions (777/666) to ensure Chrome can write regardless of ownership.
    """
    import os
    from pymongo import MongoClient

    mongodb_uri        = os.environ.get('MONGODB_URI', MONGODB_URI)
    chrome_profile_dir = os.environ.get('CHROME_PROFILE_DIR', CHROME_PROFILE_BASE_DIR)
    running_uid        = os.getuid()
    running_gid        = os.getgid()

    print("=" * 80)
    print("VERIFYING LOCAL CHROME PROFILES")
    print("=" * 80)
    print(f"MongoDB URI:       {mongodb_uri}")
    print(f"Profile Directory: {chrome_profile_dir}")
    print(f"Running as UID:    {running_uid}  GID: {running_gid}")
    print()

    os.makedirs(chrome_profile_dir, exist_ok=True)

    try:
        os.chmod(chrome_profile_dir, 0o777)
        print(f"✓ Profile root permissions set to 777 (world-writable)")
    except (PermissionError, OSError) as e:
        print(f"⚠ Could not chmod {chrome_profile_dir}: {e}")

    client = MongoClient(mongodb_uri)
    db     = client['messages_db']

    accounts = list(db.accounts.find(
        {'profile_type': 'local_chrome', 'is_active': {'$ne': False}},
        {'postgres_account_id': 1, 'username': 1, 'profile_id': 1, 'profile_path': 1}
    ))
    client.close()

    print(f"Found {len(accounts)} active LOCAL Chrome account(s) in MongoDB")

    if len(accounts) == 0:
        print()
        print("ERROR: No Local Chrome profiles found!")
        print("Action required:")
        print("  1. Go to Streamlit (http://localhost:8501)")
        print("  2. Navigate to Accounts → Add Account → Local Chrome")
        print("  3. Create at least one account")
        raise Exception("No Local Chrome profiles found! Create profiles via Streamlit first.")

    print()
    print("Verifying profile directories...")
    verified_accounts = []

    for acc in accounts:
        profile_path = acc.get('profile_path')
        if not profile_path:
            profile_path = f"{chrome_profile_dir}/account_{acc['username']}"
            print(f"  No path in MongoDB, using: {profile_path}")

        os.makedirs(profile_path, exist_ok=True)

        try:
            os.chmod(profile_path, 0o777)

            for root, dirs, files in os.walk(profile_path):
                for d in dirs:
                    dir_path = os.path.join(root, d)
                    try:
                        os.chmod(dir_path, 0o777)
                    except (PermissionError, OSError):
                        pass

                for f in files:
                    file_path = os.path.join(root, f)
                    try:
                        os.chmod(file_path, 0o666)
                    except (PermissionError, OSError):
                        pass

            print(f"  ✓ Permissions fixed (777/666): {profile_path}")
        except (PermissionError, OSError) as e:
            print(f"  ⚠ Could not fix all permissions in {profile_path}: {e}")

        default_dir   = os.path.join(profile_path, 'Default')
        has_been_used = os.path.exists(default_dir)

        verified_accounts.append({
            'account_id':    acc['postgres_account_id'],
            'username':      acc['username'],
            'profile_id':    acc.get('profile_id', f"account_{acc['username']}"),
            'profile_path':  profile_path,
            'has_been_used': has_been_used,
        })

        status = "✓ READY" if has_been_used else "✓ NEW (directory created)"
        print(f"  {status}: {acc['username']} at {profile_path}")

    if len(verified_accounts) == 0:
        raise Exception("No valid profile directories could be verified!")

    context['task_instance'].xcom_push(key='accounts', value=verified_accounts)

    print()
    print("=" * 80)
    print(f"✓ VERIFICATION COMPLETE: {len(verified_accounts)} profile(s) ready")
    print("=" * 80)
    for acc in verified_accounts:
        print(f"  - {acc['username']} (ID: {acc['account_id']})")
    print()

    return verified_accounts


def verify_orchestrator_template(**context):
    """
    Verify that the orchestrator template has been uploaded via Streamlit.
    """
    from pymongo import MongoClient

    mongodb_uri = os.environ.get('MONGODB_URI', MONGODB_URI)

    print("=" * 80)
    print("VERIFYING ORCHESTRATOR TEMPLATE")
    print("=" * 80)

    try:
        client       = MongoClient(mongodb_uri)
        db           = client['messages_db']
        settings_doc = db.settings.find_one({'category': 'system'})

        if not settings_doc or not settings_doc.get('settings'):
            print("❌ No system settings found!")
            print("Action required: Configure settings in Streamlit UI")
            raise Exception("System settings not found in MongoDB")

        template = settings_doc['settings'].get('execution_orchestrator_template')

        if not template:
            print()
            print("❌ ERROR: No orchestrator template found!")
            print()
            print("Action required:")
            print("  1. Go to Streamlit (http://localhost:8501)")
            print("  2. Navigate to Settings → Execution Configuration")
            print("  3. Scroll to 'Workflow Orchestrator' section")
            print("  4. Upload or paste orchestrator template JSON")
            print("  5. Click 'Save Orchestrator Template'")
            print()
            raise Exception("No orchestrator template found! Upload via Streamlit first.")

        node_count = len(
            template.get('template_data', {}).get('drawflow', {}).get('nodes', [])
        )

        print(f"✓ Orchestrator template found:")
        print(f"  Name    : {template.get('template_name', 'Unknown')}")
        print(f"  Uploaded: {template.get('uploaded_at', 'Unknown')}")
        print(f"  Nodes   : {node_count}")

        client.close()

        print()
        print("=" * 80)
        print("✓ ORCHESTRATOR TEMPLATE VERIFIED")
        print("=" * 80)
        print()

        return True

    except Exception as e:
        print(f"❌ Template verification failed: {e}")
        raise


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    'local_executor',
    default_args=default_args,
    description='Execute workflows using dynamic orchestrator (one master workflow runs all)',
    schedule_interval=None,
    start_date=datetime(2025, 7, 31),
    catchup=False,
    max_active_runs=1,
    tags=['workflow', 'orchestrator', 'dynamic', 'local-chrome'],
) as dag:

    start = DummyOperator(task_id='start')

    # ── System check ──────────────────────────────────────────────────────────
    system_check = BashOperator(
        task_id='system_check',
        bash_command=f'''
            set -e
            echo "=== System Requirements Check ==="

            echo "Checking Chrome..."
            {CHROME_EXECUTABLE} --version && echo "✓ Chrome OK"

            echo "Checking FFmpeg..."
            ffmpeg -version 2>&1 | head -n 1 && echo "✓ FFmpeg OK"

            echo "Checking Xvfb..."
            which Xvfb && echo "✓ Xvfb OK"

            echo "Checking Node.js..."
            node --version && echo "✓ Node.js OK"

            echo "Checking npm..."
            npm --version && echo "✓ npm OK"

            echo "Checking node_modules..."
            if [ -d "{JS_EXECUTOR_PATH}/node_modules" ]; then
                echo "✓ node_modules present"
            else
                echo "⚠ node_modules missing — running npm install..."
                cd "{JS_EXECUTOR_PATH}" && npm install
                echo "✓ npm install complete"
            fi

            echo "=== System Check Completed ==="
        ''',
    )

    # ── Network check ─────────────────────────────────────────────────────────
    network_check = BashOperator(
        task_id='network_check',
        bash_command='''
            set -e
            echo "=== Network Connectivity Check ==="
            nc -zv mongodb 27017 && echo "✓ MongoDB accessible"
            nc -zv postgres 5432  && echo "✓ PostgreSQL accessible"
            echo "=== Network Check Completed ==="
        ''',
        retries=2,
        retry_delay=timedelta(seconds=30),
    )

    # ── Environment setup ─────────────────────────────────────────────────────
    environment_setup = BashOperator(
        task_id='environment_setup',
        bash_command=f'''
            set -e
            echo "=== Environment Setup (Aggressive Permission Mode) ==="
            AIRFLOW_UID=$(id -u)
            AIRFLOW_GID=$(id -g)
            echo "Running as UID=${{AIRFLOW_UID}} GID=${{AIRFLOW_GID}}"

            mkdir -p {CHROME_PROFILE_BASE_DIR}
            mkdir -p {RECORDINGS_DIR}
            mkdir -p {DOWNLOADS_DIR}

            echo "Making directories world-writable (777)..."
            find {CHROME_PROFILE_BASE_DIR} -type d -exec chmod 777 {{}} \\; 2>/dev/null || true
            chmod 777 {CHROME_PROFILE_BASE_DIR} 2>/dev/null || true
            chmod 777 {RECORDINGS_DIR}          2>/dev/null || true
            chmod 777 {DOWNLOADS_DIR}           2>/dev/null || true

            echo "Making files world-writable (666)..."
            find {CHROME_PROFILE_BASE_DIR} -type f -exec chmod 666 {{}} \\; 2>/dev/null || true

            echo "✓ Profile directory:    {CHROME_PROFILE_BASE_DIR}  $(stat -c '%U:%G %a' {CHROME_PROFILE_BASE_DIR})"
            echo "✓ Recordings directory: {RECORDINGS_DIR}  $(stat -c '%U:%G %a' {RECORDINGS_DIR})"
            echo "✓ Downloads directory:  {DOWNLOADS_DIR}  $(stat -c '%U:%G %a' {DOWNLOADS_DIR})"

            if [ ! -d /tmp/.X11-unix ]; then
                echo "Creating /tmp/.X11-unix..."
                mkdir -p /tmp/.X11-unix && chmod 1777 /tmp/.X11-unix || true
            fi
            echo "✓ /tmp/.X11-unix ready"

            if [ -f {JS_EXECUTOR_PATH}/dynamic-orchestrator.js ]; then
                echo "✓ dynamic-orchestrator.js found"
            else
                echo "✗ dynamic-orchestrator.js MISSING at {JS_EXECUTOR_PATH}/dynamic-orchestrator.js"
                echo "  Ensure the file is deployed to the correct path."
                exit 1
            fi

            echo ""
            echo "Listing profile directories:"
            ls -la {CHROME_PROFILE_BASE_DIR}/ 2>/dev/null || echo "(No profiles yet)"
            echo "=== Environment Setup Completed ==="
        ''',
    )

    # ── Verify profiles ───────────────────────────────────────────────────────
    verify_profiles = PythonOperator(
        task_id='verify_profiles_exist',
        python_callable=verify_profiles_exist,
        provide_context=True,
    )

    # ── Verify orchestrator template ──────────────────────────────────────────
    verify_template = PythonOperator(
        task_id='verify_orchestrator_template',
        python_callable=verify_orchestrator_template,
        provide_context=True,
    )

    # ── Setup display ─────────────────────────────────────────────────────────
    setup_display = BashOperator(
        task_id='setup_display',
        bash_command=f'''
            set -e

            echo ""
            echo "════════════════════════════════════════════════════════════════════════════════"
            echo "DISPLAY SETUP WITH AGGRESSIVE PERMISSION FIXES"
            echo "════════════════════════════════════════════════════════════════════════════════"
            echo ""

            MY_PID=$$

            echo "Cleaning up any stale Chrome processes..."
            CHROME_PIDS=$(pgrep -f "google-chrome.*user-data-dir" 2>/dev/null | grep -v "^${{MY_PID}}$" || true)
            if [ -n "${{CHROME_PIDS}}" ]; then
                echo "  Killing Chrome PIDs: ${{CHROME_PIDS}}"
                echo "${{CHROME_PIDS}}" | xargs kill -9 2>/dev/null || true
                sleep 1
            else
                echo "  No stale Chrome processes found"
            fi

            echo ""
            echo "Applying aggressive permission fixes to all profiles..."

            for profile_dir in {CHROME_PROFILE_BASE_DIR}/account_*; do
                if [ -d "$profile_dir" ]; then
                    profile_name=$(basename "$profile_dir")
                    echo "  Processing: $profile_name"

                    find "$profile_dir" -type d -exec chmod 777 {{}} \\; 2>/dev/null || true
                    find "$profile_dir" -type f -exec chmod 666 {{}} \\; 2>/dev/null || true

                    # Remove all Chrome singleton lock files
                    find "$profile_dir" -name "SingletonLock"          -delete 2>/dev/null || true
                    find "$profile_dir" -name "SingletonCookie"        -delete 2>/dev/null || true
                    find "$profile_dir" -name "SingletonSocket"        -delete 2>/dev/null || true
                    find "$profile_dir" -name ".org.chromium.Chromium.*" -delete 2>/dev/null || true
                    find "$profile_dir" -name ".com.google.Chrome.*"   -delete 2>/dev/null || true

                    if [ -d "$profile_dir/Default/Extensions" ]; then
                        find "$profile_dir/Default/Extensions" -type d -exec chmod 777 {{}} \\; 2>/dev/null || true
                        find "$profile_dir/Default/Extensions" -type f -exec chmod 666 {{}} \\; 2>/dev/null || true
                    fi

                    echo "    ✓ Permissions 777/666 set, lock files removed"
                fi
            done

            echo ""
            echo "✓ All profiles have world-writable permissions"
            echo "✓ All Chrome lock files removed"

            echo ""
            echo "Cleaning up stale Xvfb..."
            XVFB_PIDS=$(pgrep -f "Xvfb.*:99" 2>/dev/null | grep -v "^${{MY_PID}}$" || true)
            if [ -n "${{XVFB_PIDS}}" ]; then
                echo "  Killing Xvfb PIDs: ${{XVFB_PIDS}}"
                echo "${{XVFB_PIDS}}" | xargs kill -9 2>/dev/null || true
                sleep 1
            fi
            rm -f /tmp/.X99-lock      2>/dev/null || true
            rm -f /tmp/.X11-unix/X99 2>/dev/null || true

            export DISPLAY=:99
            echo ""
            echo "Starting Xvfb on DISPLAY=:99..."
            nohup Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp \
                > /tmp/xvfb_orchestrator.log 2>&1 &
            XVFB_PID=$!
            sleep 3

            if ! ps -p $XVFB_PID > /dev/null 2>&1; then
                echo "✗ Xvfb failed to start. Log:"
                cat /tmp/xvfb_orchestrator.log 2>/dev/null || true
                exit 1
            fi
            echo "✓ Xvfb running (PID: ${{XVFB_PID}}, DISPLAY=:99)"

            echo ""
            echo "════════════════════════════════════════════════════════════════════════════════"
            echo "✓✓✓ DISPLAY SETUP COMPLETED ✓✓✓"
            echo "════════════════════════════════════════════════════════════════════════════════"
            echo ""
        ''',
    )

    # ── Execute dynamic orchestrator ──────────────────────────────────────────
    #
    # KEY FIXES vs. original:
    #  1. PATH is explicitly set so `node` resolves in the subprocess.
    #  2. stderr is merged with stdout (2>&1) so Node errors show in Airflow logs.
    #  3. --unhandled-rejections=strict makes all unhandled promise rejections
    #     fatal, which causes a non-zero exit code (previously silent exit 0).
    #  4. The bash wrapper prints the exit code explicitly for debugging.
    #  5. HOME and USER are forwarded so Chrome / Puppeteer find their dirs.
    # ─────────────────────────────────────────────────────────────────────────
    execute_orchestrator = BashOperator(
        task_id='execute_dynamic_orchestrator',
        bash_command=f'''
            set -e
            echo "=== Dynamic Orchestrator Execution Starting ==="
            echo "Working directory: {JS_EXECUTOR_PATH}"
            echo "Node version: $(node --version)"
            echo "Display: $DISPLAY"
            echo ""

            cd {JS_EXECUTOR_PATH}

            echo "--- Node output below ---"
            node --unhandled-rejections=strict dynamic-orchestrator.js 2>&1
            EXIT_CODE=$?

            echo ""
            echo "--- Node process exited with code: $EXIT_CODE ---"

            if [ $EXIT_CODE -ne 0 ]; then
                echo "✗ Orchestrator FAILED (exit code $EXIT_CODE)"
                exit $EXIT_CODE
            fi

            echo "✓ Orchestrator completed successfully"
            echo "=== Dynamic Orchestrator Execution Finished ==="
        ''',
        env={
            # ── PATH: critical fix — without this, `node` may not be found ──
            'PATH':           '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/lib/nodejs/bin',
            'HOME':           os.environ.get('HOME', '/root'),
            'USER':           os.environ.get('USER', 'airflow'),

            # ── Display ─────────────────────────────────────────────────────
            'DISPLAY':        ':99',

            # ── Database connections ────────────────────────────────────────
            'MONGODB_URI':        os.environ.get('MONGODB_URI',       MONGODB_URI),
            'MONGODB_DB_NAME':    'messages_db',
            'POSTGRES_HOST':      'postgres',
            'POSTGRES_PORT':      '5432',
            'POSTGRES_DB':        os.environ.get('POSTGRES_DB',       'messages'),
            'POSTGRES_USER':      os.environ.get('POSTGRES_USER',     'airflow'),
            'POSTGRES_PASSWORD':  os.environ.get('POSTGRES_PASSWORD', 'airflow'),

            # ── Chrome & paths ──────────────────────────────────────────────
            'CHROME_PROFILE_DIR': CHROME_PROFILE_BASE_DIR,
            'CHROME_EXECUTABLE':  CHROME_EXECUTABLE,
            'RECORDINGS_DIR':     RECORDINGS_DIR,
            'DOWNLOADS_DIR':      DOWNLOADS_DIR,

            # ── Airflow context ─────────────────────────────────────────────
            'AIRFLOW_CTX_DAG_RUN_ID':     '{{ dag_run.run_id }}',
            'AIRFLOW_CTX_EXECUTION_DATE': '{{ ds }}',
            'AIRFLOW_CTX_TASK_ID':        '{{ task_instance.task_id }}',

            # ── Runtime flags ───────────────────────────────────────────────
            'TZ':                  os.environ.get('TZ', 'Africa/Nairobi'),
            'NODE_ENV':            'production',
            'LOG_LEVEL':           'info',
            'CONTINUE_ON_FAILURE': 'true',
            'DEBUG_MODE':          'false',
        },
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=6),
    )

    # ── Cleanup ───────────────────────────────────────────────────────────────
    cleanup = BashOperator(
        task_id='cleanup',
        bash_command=f'''
            echo "=== Cleanup Started ==="

            MY_PID=$$
            CHROME_PIDS=$(pgrep -f "google-chrome.*user-data-dir" 2>/dev/null | grep -v "^${{MY_PID}}$" || true)
            if [ -n "${{CHROME_PIDS}}" ]; then
                echo "Stopping Chrome processes: ${{CHROME_PIDS}}"
                echo "${{CHROME_PIDS}}" | xargs kill -15 2>/dev/null || true
                sleep 2
                echo "${{CHROME_PIDS}}" | xargs kill -9 2>/dev/null || true
            fi

            echo "Removing Chrome lock files (profile data preserved)..."
            find {CHROME_PROFILE_BASE_DIR} -name "SingletonLock"   -delete 2>/dev/null || true
            find {CHROME_PROFILE_BASE_DIR} -name "SingletonSocket" -delete 2>/dev/null || true
            find {CHROME_PROFILE_BASE_DIR} -name "SingletonCookie" -delete 2>/dev/null || true
            echo "✓ Chrome lock files cleaned"

            echo "Updating MongoDB sessions..."
            python3 -c "
from pymongo import MongoClient
from datetime import datetime
import os

mongo_uri = os.environ.get('MONGODB_URI', '{MONGODB_URI}')
try:
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    db = client['messages_db']
    result = db.browser_sessions.update_many(
        {{'created_by': 'airflow', 'is_active': True}},
        {{'\$set': {{
            'is_active': False,
            'ended_at': datetime.now(),
            'session_status': 'completed_by_dag'
        }}}}
    )
    print(f'✓ Updated {{result.modified_count}} session record(s) in MongoDB')
    client.close()
except Exception as e:
    print(f'⚠ Could not update MongoDB sessions: {{e}}')
"
            echo "=== Cleanup Completed ==="
        ''',
        env={
            'MONGODB_URI': os.environ.get('MONGODB_URI', MONGODB_URI),
        },
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    # ── Statistics ────────────────────────────────────────────────────────────
    statistics = BashOperator(
        task_id='execution_statistics',
        bash_command=f'''
            echo "=== Orchestrator Execution Statistics ==="
            echo "Profile Directory:    {CHROME_PROFILE_BASE_DIR}"
            echo "Recordings Directory: {RECORDINGS_DIR}"
            echo "Downloads Directory:  {DOWNLOADS_DIR}"
            echo ""
            echo "Total profiles:    $(find {CHROME_PROFILE_BASE_DIR} -maxdepth 1 -type d -name "account_*" 2>/dev/null | wc -l || echo 0)"
            echo "Total recordings:  $(find {RECORDINGS_DIR} -name "*.mp4"  2>/dev/null | wc -l || echo 0)"
            echo "Total screenshots: $(find {DOWNLOADS_DIR}  -name "*.png"  2>/dev/null | wc -l || echo 0)"
            echo "Disk usage (profiles):   $(du -sh {CHROME_PROFILE_BASE_DIR} 2>/dev/null | cut -f1 || echo 'N/A')"
            echo "Disk usage (recordings): $(du -sh {RECORDINGS_DIR}          2>/dev/null | cut -f1 || echo 'N/A')"
            echo "Disk usage (downloads):  $(du -sh {DOWNLOADS_DIR}           2>/dev/null | cut -f1 || echo 'N/A')"
            echo "=== Statistics Completed ==="
        ''',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Trigger report DAG ────────────────────────────────────────────────────
    trigger_report = TriggerDagRunOperator(
        task_id='trigger_execution_report',
        trigger_dag_id='execution_report',
        conf={
            'hours_back':     24,
            'triggered_by':   'local_executor_orchestrator',
            'dag_run_id':     '{{ dag_run.run_id }}',
            'execution_date': '{{ ds }}',
        },
        trigger_rule=TriggerRule.ALL_DONE,
        wait_for_completion=False,
        poke_interval=30,
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    start >> [system_check, network_check] >> environment_setup >> [verify_profiles, verify_template]
    [verify_profiles, verify_template] >> setup_display >> execute_orchestrator >> [cleanup, statistics]
    [cleanup, statistics] >> trigger_report >> end
