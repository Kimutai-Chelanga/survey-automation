"""
Workflow Execution Reporting DAG
Generates comprehensive execution reports and sends via email.
Triggered by local_executor DAG completion.
Includes success/failure statistics and PostgreSQL success tracking.

UPDATED (2026-02-20):
  - Changed MongoDB collection from 'automa_execution_logs' to 'automa_logs'
  - Added support for new orchestrator timing fields (estimated_wait_ms, wait_margin_ms, actual_wait_ms)
  - Updated session filtering to include 'local_chrome' sessions (orchestrator uses this, not 'local_chrome_with_recording')
  - Added timing metrics to reports
  - Improved error handling for missing fields
"""

import sys
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
import json

sys.path.append('/opt/airflow/src')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from pymongo import MongoClient
from bson import ObjectId
from core.database.postgres.connection import get_postgres_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv(
    'MONGODB_URI',
    'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'
)

DEFAULT_ARGS = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_mongo_connection():
    client = MongoClient(MONGODB_URI)
    return client['messages_db']


def clean_doc(doc):
    """Recursively convert ObjectIds to strings so docs are JSON-serialisable."""
    if isinstance(doc, dict):
        return {
            k: (str(v) if isinstance(v, ObjectId)
                else clean_doc(v) if isinstance(v, (dict, list))
                else v)
            for k, v in doc.items()
        }
    if isinstance(doc, list):
        return [clean_doc(i) if isinstance(i, (dict, list)) else i for i in doc]
    return doc


def _row_val(row, int_index: int, str_key: str):
    """
    Safely extract a value from a DB row that may be either:
      - a plain tuple/list  (standard cursor)   → use int index
      - a dict / RealDictRow (RealDictCursor)    → use string key
    This prevents the KeyError: 0 crash.
    """
    try:
        if isinstance(row, dict):
            return row[str_key]
        return row[int_index]
    except (KeyError, IndexError) as e:
        raise KeyError(
            f"Could not read column '{str_key}' (index {int_index}) from row "
            f"of type {type(row).__name__}: {row}"
        ) from e


def _safe_float(value, default=0.0):
    """Safely convert to float, return default if None or invalid."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _safe_int(value, default=0):
    """Safely convert to int, return default if None or invalid."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


# ============================================================================
# EMAIL UTILITIES
# ============================================================================

def send_email_report(subject, text_body, html_body=None):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    try:
        email_host     = os.getenv('EMAIL_HOST', 'smtp.gmail.com')
        email_port     = int(os.getenv('EMAIL_PORT', '587'))
        email_username = os.getenv('EMAIL_USERNAME')
        email_password = os.getenv('EMAIL_PASSWORD')
        email_from     = os.getenv('EMAIL_FROM', email_username)
        email_to       = os.getenv('EMAIL_TO', email_username)

        if not email_username or not email_password:
            logger.warning("Email credentials not configured — skipping send")
            return False

        msg = MIMEMultipart('alternative')
        msg['From']    = email_from
        msg['To']      = email_to
        msg['Subject'] = subject

        msg.attach(MIMEText(text_body, 'plain'))
        if html_body:
            msg.attach(MIMEText(html_body, 'html'))

        use_tls = os.getenv('EMAIL_USE_TLS', 'true').lower() == 'true'
        if use_tls:
            server = smtplib.SMTP(email_host, email_port)
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(email_host, email_port)

        server.login(email_username, email_password)
        server.send_message(msg)
        server.quit()

        logger.info(f"✅ Report email sent to {email_to}")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to send email: {e}")
        return False


# ============================================================================
# DATA COLLECTION
# ============================================================================

def collect_execution_data(**kwargs):
    """
    Collect execution data from the last N hours.

    UPDATED:
      - Changed session_type filter to include 'local_chrome' (orchestrator sessions)
      - Changed automa_logs collection from 'automa_execution_logs' to 'automa_logs'
      - Added timing fields from execution_sessions
    """
    try:
        mongo_db = get_mongo_connection()
        dag_run  = kwargs.get('dag_run')

        hours_back = 24
        if dag_run and dag_run.conf and dag_run.conf.get('hours_back'):
            hours_back = dag_run.conf['hours_back']

        end_time   = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=hours_back)

        logger.info(f"📊 Collecting data from {start_time} to {end_time}")

        # ── MongoDB collections ───────────────────────────────────────────────
        # Orchestrator sessions use session_type='local_chrome' (not 'local_chrome_with_recording')
        sessions = list(mongo_db.execution_sessions.find({
            'created_at': {'$gte': start_time, '$lte': end_time},
            'session_type': 'local_chrome'
        }).sort('created_at', -1))

        workflows = list(mongo_db.workflow_metadata.find({
            'executed_at': {'$gte': start_time.isoformat(), '$lte': end_time.isoformat()},
            'executed': True
        }))

        # UPDATED: Use 'automa_logs' collection (dynamic-orchestrator.js writes here)
        automa_logs = list(mongo_db.automa_logs.find({
            'created_at': {'$gte': start_time, '$lte': end_time}
        }))

        videos = list(mongo_db.video_recording_metadata.find({
            'created_at': {'$gte': start_time, '$lte': end_time}
        }))

        screenshots = list(mongo_db.screenshot_metadata.find({
            'created_at': {'$gte': start_time, '$lte': end_time}
        }))

        # NEW: Get generated orchestrators for timing data
        orchestrators = list(mongo_db.generated_orchestrators.find({
            'created_at': {'$gte': start_time, '$lte': end_time}
        }).sort('created_at', -1))

        # ── PostgreSQL ────────────────────────────────────────────────────────
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    SELECT
                        links_id,
                        link,
                        workflow_type,
                        workflow_status,
                        executed,
                        success,
                        failure,
                        workflow_processed_time,
                        account_id
                    FROM links
                    WHERE workflow_processed_time >= %s
                      AND workflow_processed_time <= %s
                      AND executed = TRUE
                    ORDER BY workflow_processed_time DESC
                """, (start_time, end_time))
                pg_link_rows = cur.fetchall()

                logger.info(f"PostgreSQL returned {len(pg_link_rows)} link rows")

                cur.execute("""
                    SELECT
                        COUNT(*)                                                      AS total_executed,
                        SUM(CASE WHEN success = TRUE  THEN 1 ELSE 0 END)             AS total_success,
                        SUM(CASE WHEN failure = TRUE  THEN 1 ELSE 0 END)             AS total_failure,
                        SUM(CASE WHEN success = TRUE AND failure = TRUE THEN 1 ELSE 0 END) AS both_true,
                        AVG(CASE
                            WHEN executed = TRUE AND success = TRUE THEN 1.0
                            WHEN executed = TRUE AND failure = TRUE THEN 0.0
                            ELSE NULL
                        END) * 100                                                    AS success_rate_pct
                    FROM links
                    WHERE executed = TRUE
                      AND workflow_processed_time >= %s
                      AND workflow_processed_time <= %s
                """, (start_time, end_time))
                stats_row = cur.fetchone()

        # ── Build pg_links using _row_val ─────────────────────────────────────
        pg_links = []
        for row in pg_link_rows:
            processed_time = _row_val(row, 7, 'workflow_processed_time')
            pg_links.append({
                'links_id':               _row_val(row, 0, 'links_id'),
                'link':                   _row_val(row, 1, 'link'),
                'workflow_type':          _row_val(row, 2, 'workflow_type'),
                'workflow_status':        _row_val(row, 3, 'workflow_status'),
                'executed':               _row_val(row, 4, 'executed'),
                'success':                _row_val(row, 5, 'success'),
                'failure':                _row_val(row, 6, 'failure'),
                'workflow_processed_time': processed_time.isoformat() if processed_time else None,
                'account_id':             _row_val(row, 8, 'account_id'),
            })

        # ── Build success_stats using _row_val ────────────────────────────────
        def _s(idx, key, default=0):
            if stats_row is None:
                return default
            val = _row_val(stats_row, idx, key)
            return val if val is not None else default

        success_rate_raw = _s(4, 'success_rate_pct', 0.0)
        success_stats = {
            'total_executed': int(_s(0, 'total_executed', 0)),
            'total_success':  int(_s(1, 'total_success', 0)),
            'total_failure':  int(_s(2, 'total_failure', 0)),
            'both_true':      int(_s(3, 'both_true', 0)),
            'success_rate':   round(float(success_rate_raw), 1),
        }

        # ── Calculate timing metrics from sessions ────────────────────────────
        timing_stats = {
            'total_estimated_ms': 0,
            'total_configured_ms': 0,
            'total_actual_ms': 0,
            'total_wait_margin_ms': 0,
            'sessions_with_timing': 0,
            'avg_estimated_s': 0,
            'avg_configured_s': 0,
            'avg_actual_s': 0,
            'avg_margin_s': 0,
        }

        for session in sessions:
            if session.get('estimated_wait_ms'):
                timing_stats['total_estimated_ms'] += _safe_float(session['estimated_wait_ms'])
                timing_stats['sessions_with_timing'] += 1
            if session.get('configured_wait_ms'):
                timing_stats['total_configured_ms'] += _safe_float(session['configured_wait_ms'])
            if session.get('actual_wait_ms'):
                timing_stats['total_actual_ms'] += _safe_float(session['actual_wait_ms'])
            if session.get('wait_margin_ms'):
                timing_stats['total_wait_margin_ms'] += _safe_float(session['wait_margin_ms'])

        if timing_stats['sessions_with_timing'] > 0:
            timing_stats['avg_estimated_s'] = timing_stats['total_estimated_ms'] / (timing_stats['sessions_with_timing'] * 1000)
            timing_stats['avg_configured_s'] = timing_stats['total_configured_ms'] / (timing_stats['sessions_with_timing'] * 1000)
            timing_stats['avg_actual_s'] = timing_stats['total_actual_ms'] / (timing_stats['sessions_with_timing'] * 1000)
            timing_stats['avg_margin_s'] = timing_stats['total_wait_margin_ms'] / (timing_stats['sessions_with_timing'] * 1000)

        data = {
            'sessions':      [clean_doc(s) for s in sessions],
            'workflows':     [clean_doc(w) for w in workflows],
            'automa_logs':   [clean_doc(l) for l in automa_logs],
            'videos':        [clean_doc(v) for v in videos],
            'screenshots':   [clean_doc(s) for s in screenshots],
            'orchestrators': [clean_doc(o) for o in orchestrators],
            'pg_links':      pg_links,
            'success_stats': success_stats,
            'timing_stats':  timing_stats,
            'start_time':    start_time.isoformat(),
            'end_time':      end_time.isoformat(),
        }

        logger.info(f"✅ Collected: {len(sessions)} sessions, {len(workflows)} workflows, "
                    f"{len(pg_links)} pg_links, {len(orchestrators)} orchestrators")
        logger.info(f"✅ Success stats: {success_stats}")
        logger.info(f"✅ Timing stats: {timing_stats}")

        kwargs['ti'].xcom_push(key='execution_data', value=data)
        return data

    except Exception as e:
        logger.error(f"❌ Error collecting data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


# ============================================================================
# ANALYSIS
# ============================================================================

def analyze_execution_metrics(**kwargs):
    try:
        ti   = kwargs['ti']
        data = ti.xcom_pull(key='execution_data', task_ids='collect_data')

        if not data:
            raise Exception("No execution data found in XCom")

        sessions      = data['sessions']
        workflows     = data['workflows']
        automa_logs   = data['automa_logs']
        videos        = data['videos']
        screenshots   = data['screenshots']
        orchestrators = data.get('orchestrators', [])
        pg_links      = data['pg_links']
        success_stats = data.get('success_stats', {})
        timing_stats  = data.get('timing_stats', {})

        metrics = {
            'summary': {},
            'postgres_success_stats': success_stats,
            'timing_stats': timing_stats,
            'by_account': {},
            'by_workflow_type': {},
            'by_category': {},
            'performance': {},
            'errors': []
        }

        metrics['summary'] = {
            'total_sessions':            len(sessions),
            'total_workflows':           len(workflows),
            'total_videos':              len(videos),
            'total_screenshots':         len(screenshots),
            'total_orchestrators':       len(orchestrators),
            'postgres_links_executed':   len(pg_links),
            'postgres_success_count':    success_stats.get('total_success', 0),
            'postgres_failure_count':    success_stats.get('total_failure', 0),
            'postgres_success_rate':     f"{success_stats.get('success_rate', 0)}%",
            'postgres_both_true_error':  success_stats.get('both_true', 0),
            'sessions_with_timing':      timing_stats.get('sessions_with_timing', 0),
            'avg_estimated_s':           round(timing_stats.get('avg_estimated_s', 0), 1),
            'avg_configured_s':          round(timing_stats.get('avg_configured_s', 0), 1),
            'avg_actual_s':              round(timing_stats.get('avg_actual_s', 0), 1),
            'avg_margin_s':              round(timing_stats.get('avg_margin_s', 0), 1),
        }

        if sessions:
            successful = sum(1 for s in sessions if s.get('session_status') == 'completed')
            metrics['summary'].update({
                'successful_sessions': successful,
                'failed_sessions':     len(sessions) - successful,
                'session_success_rate': f"{(successful/len(sessions)*100):.1f}%",
            })

        if workflows:
            successful_wf = sum(1 for w in workflows if w.get('success'))
            metrics['summary'].update({
                'successful_workflows': successful_wf,
                'failed_workflows':     len(workflows) - successful_wf,
                'workflow_success_rate': f"{(successful_wf/len(workflows)*100):.1f}%",
            })

        # By account
        account_stats: Dict[str, Any] = {}
        for link in pg_links:
            account = f"Account_{link.get('account_id', 'Unknown')}"
            s = account_stats.setdefault(account, {'total': 0, 'success': 0, 'failure': 0})
            s['total'] += 1
            if link.get('success'):
                s['success'] += 1
            if link.get('failure'):
                s['failure'] += 1
        for s in account_stats.values():
            s['success_rate'] = f"{(s['success']/s['total']*100):.1f}%" if s['total'] else '0%'
        metrics['by_account'] = account_stats

        # By workflow type
        type_stats: Dict[str, Any] = {}
        for link in pg_links:
            wf_type = link.get('workflow_type', 'Unknown')
            s = type_stats.setdefault(wf_type, {'total': 0, 'success': 0, 'failure': 0})
            s['total'] += 1
            if link.get('success'):
                s['success'] += 1
            if link.get('failure'):
                s['failure'] += 1
        for s in type_stats.values():
            s['success_rate'] = f"{(s['success']/s['total']*100):.1f}%" if s['total'] else '0%'
        metrics['by_workflow_type'] = type_stats

        # By category (MongoDB)
        category_stats: Dict[str, Any] = {}
        for wf in workflows:
            cat = wf.get('category', 'Unknown')
            s = category_stats.setdefault(cat, {'total': 0, 'successful': 0, 'failed': 0})
            s['total'] += 1
            if wf.get('success'):
                s['successful'] += 1
            else:
                s['failed'] += 1
        metrics['by_category'] = category_stats

        # Performance
        if sessions:
            durations = [s.get('total_execution_time_seconds', 0)
                         for s in sessions if s.get('total_execution_time_seconds')]
            if durations:
                metrics['performance'] = {
                    'avg_duration_seconds':   sum(durations) / len(durations),
                    'min_duration_seconds':   min(durations),
                    'max_duration_seconds':   max(durations),
                    'total_duration_seconds': sum(durations),
                    'total_duration_hours':   sum(durations) / 3600,
                }

        # Errors from automa_logs
        if automa_logs:
            for log in automa_logs:
                # Check for errors in the log data
                log_data = log.get('logs', {})
                if isinstance(log_data, str):
                    try:
                        log_data = json.loads(log_data)
                    except:
                        log_data = {}
                
                # Look for error count in metadata
                metadata = log_data.get('metadata', {}) if isinstance(log_data, dict) else {}
                if metadata.get('status') == 'failed' or log.get('workflow_status') == 'error':
                    metrics['errors'].append({
                        'execution_id':  log.get('execution_id'),
                        'workflow_name': log.get('workflow_name', 'Unknown'),
                        'error_count':   1,
                        'workflow_type': log.get('workflow_type', 'Unknown'),
                        'account':       log.get('account', 'Unknown'),
                    })
            
            total_errors = len(metrics['errors'])
            total_logs = len(automa_logs)
            metrics['summary']['total_automa_errors'] = total_errors
            metrics['summary']['total_automa_logs'] = total_logs
            if total_logs:
                metrics['summary']['automa_error_rate'] = f"{(total_errors/total_logs*100):.2f}%"

        logger.info("✅ Analysis complete")
        kwargs['ti'].xcom_push(key='metrics', value=metrics)
        return metrics

    except Exception as e:
        logger.error(f"❌ Error analyzing metrics: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


# ============================================================================
# REPORT GENERATION
# ============================================================================

def generate_text_report(**kwargs):
    try:
        ti      = kwargs['ti']
        data    = ti.xcom_pull(key='execution_data', task_ids='collect_data')
        metrics = ti.xcom_pull(key='metrics',        task_ids='analyze_metrics')

        lines = [
            "=" * 80, "WORKFLOW EXECUTION REPORT", "=" * 80, "",
            f"Report Period: {data['start_time']} to {data['end_time']}",
            f"Generated:     {datetime.now(timezone.utc).isoformat()}", "",
        ]

        summary       = metrics.get('summary', {})
        success_stats = metrics.get('postgres_success_stats', {})
        timing_stats  = metrics.get('timing_stats', {})

        lines += [
            "=" * 80, "EXECUTIVE SUMMARY", "=" * 80,
            f"Total Sessions:          {summary.get('total_sessions', 0)}",
            f"  ✓ Successful:          {summary.get('successful_sessions', 0)}",
            f"  ✗ Failed:              {summary.get('failed_sessions', 0)}",
            f"  Success Rate:          {summary.get('session_success_rate', '0%')}", "",
            f"Total Workflows:         {summary.get('total_workflows', 0)}",
            f"  ✓ Successful:          {summary.get('successful_workflows', 0)}",
            f"  ✗ Failed:              {summary.get('failed_workflows', 0)}",
            f"  Success Rate:          {summary.get('workflow_success_rate', '0%')}", "",
            f"Total Orchestrators:     {summary.get('total_orchestrators', 0)}", "",
            "=" * 80, "POSTGRES SUCCESS/FAILURE TRACKING", "=" * 80,
            f"Links Executed:          {summary.get('postgres_links_executed', 0)}",
            f"✅ Success:              {summary.get('postgres_success_count', 0)}",
            f"❌ Failure:              {summary.get('postgres_failure_count', 0)}",
            f"📈 Success Rate:         {summary.get('postgres_success_rate', '0%')}",
        ]
        if summary.get('postgres_both_true_error', 0) > 0:
            lines.append(f"⚠️  Data Error:           {summary['postgres_both_true_error']} "
                         f"links with both success AND failure = TRUE")
        lines += [
            "",
            f"Video Recordings:        {summary.get('total_videos', 0)}",
            f"Screenshots:             {summary.get('total_screenshots', 0)}",
            f"Automa Logs:             {summary.get('total_automa_logs', 0)}",
            f"  Error Logs:            {summary.get('total_automa_errors', 0)}",
            f"  Error Rate:            {summary.get('automa_error_rate', '0%')}", "",
        ]

        if summary.get('sessions_with_timing', 0) > 0:
            lines += [
                "=" * 80, "ORCHESTRATOR TIMING METRICS", "=" * 80,
                f"Sessions with timing:    {summary.get('sessions_with_timing', 0)}",
                f"Average Estimated Wait:  {summary.get('avg_estimated_s', 0)}s",
                f"Average Wait Margin:     {summary.get('avg_margin_s', 0)}s",
                f"Average Configured Wait: {summary.get('avg_configured_s', 0)}s",
                f"Average Actual Wait:     {summary.get('avg_actual_s', 0)}s", "",
            ]

        if 'performance' in metrics:
            perf = metrics['performance']
            lines += [
                "=" * 80, "PERFORMANCE METRICS", "=" * 80,
                f"Average Duration:        {perf.get('avg_duration_seconds', 0):.1f}s",
                f"Minimum Duration:        {perf.get('min_duration_seconds', 0):.1f}s",
                f"Maximum Duration:        {perf.get('max_duration_seconds', 0):.1f}s",
                f"Total Duration:          {perf.get('total_duration_hours', 0):.2f}h", "",
            ]

        if metrics.get('by_account'):
            lines += ["=" * 80, "SUCCESS BY ACCOUNT", "=" * 80]
            for account, s in metrics['by_account'].items():
                lines += [
                    f"Account: {account}",
                    f"  Total:       {s['total']}",
                    f"  ✅ Success:  {s.get('success', 0)}",
                    f"  ❌ Failure:  {s.get('failure', 0)}",
                    f"  Rate:        {s.get('success_rate', '0%')}", "",
                ]

        if metrics.get('by_workflow_type'):
            lines += ["=" * 80, "SUCCESS BY WORKFLOW TYPE", "=" * 80]
            for wf_type, s in metrics['by_workflow_type'].items():
                lines += [
                    f"Type: {wf_type}",
                    f"  Total:       {s['total']}",
                    f"  ✅ Success:  {s.get('success', 0)}",
                    f"  ❌ Failure:  {s.get('failure', 0)}",
                    f"  Rate:        {s.get('success_rate', '0%')}", "",
                ]

        if metrics.get('by_category'):
            lines += ["=" * 80, "METRICS BY CATEGORY", "=" * 80]
            for cat, s in metrics['by_category'].items():
                rate = (s['successful'] / s['total'] * 100) if s['total'] else 0
                lines += [
                    f"Category: {cat}",
                    f"  Total:     {s['total']}",
                    f"  ✓ Success: {s['successful']}",
                    f"  ✗ Failed:  {s['failed']}",
                    f"  Rate:      {rate:.1f}%", "",
                ]

        if metrics.get('errors'):
            lines += ["=" * 80, "EXECUTION ERRORS (Top 10)", "=" * 80]
            for err in metrics['errors'][:10]:
                lines += [
                    f"Workflow: {err.get('workflow_name')}",
                    f"  Type:    {err.get('workflow_type')}",
                    f"  Account: {err.get('account')}",
                    f"  Errors:  {err.get('error_count')}", "",
                ]

        lines += ["=" * 80, "END OF REPORT", "=" * 80]

        text_report = "\n".join(lines)
        kwargs['ti'].xcom_push(key='text_report', value=text_report)
        logger.info("✅ Text report generated")
        return text_report

    except Exception as e:
        logger.error(f"❌ Error generating text report: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


def generate_html_report(**kwargs):
    try:
        ti      = kwargs['ti']
        data    = ti.xcom_pull(key='execution_data', task_ids='collect_data')
        metrics = ti.xcom_pull(key='metrics',        task_ids='analyze_metrics')
        summary = metrics.get('summary', {})

        def pct(key):
            return summary.get(key, '0%').rstrip('%')

        html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><style>
body{{font-family:'Segoe UI',sans-serif;max-width:1200px;margin:0 auto;padding:20px;background:#f5f7fa}}
.header{{background:linear-gradient(135deg,#667eea,#764ba2);color:white;padding:40px;border-radius:12px;margin-bottom:30px}}
.header h1{{margin:0 0 15px;font-size:32px}}.header p{{margin:5px 0;opacity:.95}}
.section{{background:white;padding:30px;margin-bottom:25px;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.08)}}
.section h2{{color:#667eea;margin-top:0;border-bottom:3px solid #667eea;padding-bottom:12px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:20px;margin:25px 0}}
.card{{background:linear-gradient(135deg,#f8f9fa,#e9ecef);padding:25px;border-radius:10px;border-left:5px solid #667eea}}
.card h3{{margin:0 0 12px;color:#495057;font-size:13px;text-transform:uppercase;letter-spacing:1px}}
.card .val{{font-size:36px;font-weight:bold;color:#667eea;line-height:1}}
.card .sub{{font-size:14px;color:#6c757d;margin-top:8px}}
.ok{{color:#28a745;font-weight:600}}.bad{{color:#dc3545;font-weight:600}}
table{{width:100%;border-collapse:collapse;margin:20px 0}}
th{{background:#667eea;color:white;padding:14px;text-align:left;font-size:13px;text-transform:uppercase}}
td{{padding:14px;border-bottom:1px solid #dee2e6}}tr:hover{{background:#f8f9fa}}
.bar{{width:100%;height:24px;background:#e9ecef;border-radius:12px;overflow:hidden;margin:12px 0}}
.fill{{height:100%;background:linear-gradient(90deg,#28a745,#20c997);display:flex;align-items:center;
       justify-content:center;color:white;font-weight:bold;font-size:12px}}
.pg{{background:linear-gradient(135deg,#e3f2fd,#bbdefb);border-left:5px solid #2196f3}}
.timing{{background:linear-gradient(135deg,#fff3e0,#ffe0b2);border-left:5px solid #ff9800}}
</style></head><body>
<div class="header">
  <h1>📊 Workflow Execution Report</h1>
  <p><strong>Period:</strong> {data['start_time']} → {data['end_time']}</p>
  <p><strong>Generated:</strong> {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
</div>

<div class="section">
  <h2>Executive Summary</h2>
  <div class="grid">
    <div class="card">
      <h3>Total Sessions</h3><div class="val">{summary.get('total_sessions',0)}</div>
      <div class="sub">
        <span class="ok">✓ {summary.get('successful_sessions',0)} successful</span><br>
        <span class="bad">✗ {summary.get('failed_sessions',0)} failed</span>
      </div>
    </div>
    <div class="card">
      <h3>Session Success Rate</h3><div class="val">{summary.get('session_success_rate','0%')}</div>
      <div class="bar"><div class="fill" style="width:{pct('session_success_rate')}%">{summary.get('session_success_rate','0%')}</div></div>
    </div>
    <div class="card">
      <h3>Total Workflows</h3><div class="val">{summary.get('total_workflows',0)}</div>
      <div class="sub">
        <span class="ok">✓ {summary.get('successful_workflows',0)} successful</span><br>
        <span class="bad">✗ {summary.get('failed_workflows',0)} failed</span>
      </div>
    </div>
    <div class="card">
      <h3>Workflow Success Rate</h3><div class="val">{summary.get('workflow_success_rate','0%')}</div>
      <div class="bar"><div class="fill" style="width:{pct('workflow_success_rate')}%">{summary.get('workflow_success_rate','0%')}</div></div>
    </div>
  </div>
</div>

<div class="section">
  <h2>📊 PostgreSQL Success / Failure Tracking</h2>
  <div class="grid">
    <div class="card pg"><h3>Links Executed</h3><div class="val">{summary.get('postgres_links_executed',0)}</div></div>
    <div class="card pg"><h3>✅ Success</h3><div class="val" style="color:#28a745">{summary.get('postgres_success_count',0)}</div></div>
    <div class="card pg"><h3>❌ Failure</h3><div class="val" style="color:#dc3545">{summary.get('postgres_failure_count',0)}</div></div>
    <div class="card pg">
      <h3>📈 Success Rate</h3><div class="val">{summary.get('postgres_success_rate','0%')}</div>
      <div class="bar"><div class="fill" style="width:{pct('postgres_success_rate')}%">{summary.get('postgres_success_rate','0%')}</div></div>
    </div>
  </div>"""

        if summary.get('postgres_both_true_error', 0) > 0:
            html += f"""
  <div class="card" style="background:linear-gradient(135deg,#fff3cd,#ffeaa7);border-left:5px solid #ffc107;margin-top:15px">
    <h3>⚠️ Data Validation Issue</h3>
    <div class="val" style="color:#856404">{summary['postgres_both_true_error']}</div>
    <div class="sub">Links with both success AND failure = TRUE — check DB consistency</div>
  </div>"""

        html += "\n</div>"

        if summary.get('sessions_with_timing', 0) > 0:
            html += f"""
<div class="section">
  <h2>⏱ Orchestrator Timing Metrics</h2>
  <div class="grid">
    <div class="card timing"><h3>Sessions with Timing</h3><div class="val">{summary.get('sessions_with_timing',0)}</div></div>
    <div class="card timing"><h3>Avg Estimated</h3><div class="val">{summary.get('avg_estimated_s',0)}s</div></div>
    <div class="card timing"><h3>Avg Margin</h3><div class="val">{summary.get('avg_margin_s',0)}s</div></div>
    <div class="card timing"><h3>Avg Configured</h3><div class="val">{summary.get('avg_configured_s',0)}s</div></div>
    <div class="card timing"><h3>Avg Actual</h3><div class="val">{summary.get('avg_actual_s',0)}s</div></div>
  </div>
</div>"""

        if 'performance' in metrics:
            p = metrics['performance']
            html += f"""
<div class="section"><h2>Performance</h2><div class="grid">
  <div class="card"><h3>Avg Duration</h3><div class="val">{p.get('avg_duration_seconds',0):.1f}s</div></div>
  <div class="card"><h3>Total Duration</h3><div class="val">{p.get('total_duration_hours',0):.2f}h</div></div>
  <div class="card"><h3>Min / Max</h3><div class="val">{p.get('min_duration_seconds',0):.0f}s / {p.get('max_duration_seconds',0):.0f}s</div></div>
</div></div>"""

        if metrics.get('by_account'):
            html += """<div class="section"><h2>Success by Account</h2>
<table><tr><th>Account</th><th>Total</th><th>✅ Success</th><th>❌ Failure</th><th>Rate</th></tr>"""
            for acc, s in metrics['by_account'].items():
                html += f"<tr><td>{acc}</td><td>{s['total']}</td><td class='ok'>{s.get('success',0)}</td><td class='bad'>{s.get('failure',0)}</td><td>{s.get('success_rate','0%')}</td></tr>"
            html += "</table></div>"

        if metrics.get('by_workflow_type'):
            html += """<div class="section"><h2>Success by Workflow Type</h2>
<table><tr><th>Type</th><th>Total</th><th>✅ Success</th><th>❌ Failure</th><th>Rate</th></tr>"""
            for wft, s in metrics['by_workflow_type'].items():
                html += f"<tr><td>{wft}</td><td>{s['total']}</td><td class='ok'>{s.get('success',0)}</td><td class='bad'>{s.get('failure',0)}</td><td>{s.get('success_rate','0%')}</td></tr>"
            html += "</table></div>"

        html += f"""
<div class="section"><h2>Additional Metrics</h2><div class="grid">
  <div class="card"><h3>Video Recordings</h3><div class="val">{summary.get('total_videos',0)}</div></div>
  <div class="card"><h3>Screenshots</h3><div class="val">{summary.get('total_screenshots',0)}</div></div>
  <div class="card"><h3>Automa Logs</h3><div class="val">{summary.get('total_automa_logs',0)}</div>
    <div class="sub">Error Logs: {summary.get('total_automa_errors',0)}<br>Rate: {summary.get('automa_error_rate','0%')}</div>
  </div>
  <div class="card"><h3>Orchestrators</h3><div class="val">{summary.get('total_orchestrators',0)}</div></div>
</div></div>
</body></html>"""

        kwargs['ti'].xcom_push(key='html_report', value=html)
        logger.info("✅ HTML report generated")
        return html

    except Exception as e:
        logger.error(f"❌ Error generating HTML report: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


# ============================================================================
# EMAIL, STORAGE, CLEANUP
# ============================================================================

def send_report_email_task(**kwargs):
    try:
        ti          = kwargs['ti']
        data        = ti.xcom_pull(key='execution_data', task_ids='collect_data')
        text_report = ti.xcom_pull(key='text_report',    task_ids='generate_text_report')
        html_report = ti.xcom_pull(key='html_report',    task_ids='generate_html_report')

        if not text_report or not html_report:
            logger.warning("⚠️ No reports found — skipping email")
            return False

        end_time  = datetime.fromisoformat(data['end_time'])
        timestamp = end_time.strftime('%Y-%m-%d %H:%M UTC')
        rate      = data.get('success_stats', {}).get('success_rate', 0)
        subject   = f"📊 Workflow Execution Report — {timestamp} — Success: {rate}%"

        return send_email_report(subject, text_report, html_report)

    except Exception as e:
        logger.error(f"❌ Error sending report: {e}")
        raise


def store_report_metadata(**kwargs):
    try:
        ti           = kwargs['ti']
        data         = ti.xcom_pull(key='execution_data', task_ids='collect_data')
        metrics      = ti.xcom_pull(key='metrics',        task_ids='analyze_metrics')
        email_sent   = ti.xcom_pull(task_ids='send_report_email')
        mongo_db     = get_mongo_connection()

        report_doc = {
            'report_type':            'workflow_execution',
            'generated_at':           datetime.now(timezone.utc),
            'period_start':           data['start_time'],
            'period_end':             data['end_time'],
            'dag_run_id':             kwargs.get('dag_run').run_id if kwargs.get('dag_run') else None,
            'email_sent':             email_sent,
            'summary':                metrics.get('summary', {}),
            'postgres_success_stats': metrics.get('postgres_success_stats', {}),
            'timing_stats':           metrics.get('timing_stats', {}),
            'total_sessions':         len(data.get('sessions', [])),
            'total_workflows':        len(data.get('workflows', [])),
            'total_orchestrators':    len(data.get('orchestrators', [])),
            'total_links':            len(data.get('pg_links', [])),
            'total_success':          data.get('success_stats', {}).get('total_success', 0),
            'total_failure':          data.get('success_stats', {}).get('total_failure', 0),
            'success_rate':           data.get('success_stats', {}).get('success_rate', 0),
            'created_by':             'airflow_reporting_dag',
        }

        result = mongo_db.execution_reports.insert_one(report_doc)
        logger.info(f"✅ Report metadata stored: {result.insertedId}")
        return str(result.insertedId)

    except Exception as e:
        logger.error(f"❌ Error storing report metadata: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def cleanup_old_reports(**kwargs):
    try:
        mongo_db    = get_mongo_connection()
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)
        result      = mongo_db.execution_reports.delete_many({'generated_at': {'$lt': cutoff_date}})
        logger.info(f"✅ Cleaned up {result.deleted_count} old reports")
        return result.deleted_count
    except Exception as e:
        logger.error(f"❌ Error cleaning up old reports: {e}")
        return 0


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    'execution_report',
    default_args=DEFAULT_ARGS,
    description='Generate and send comprehensive workflow execution reports with success/failure tracking',
    schedule_interval=None,   # triggered by local_executor
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['reporting', 'workflow', 'email', 'analytics', 'success-tracking', 'orchestrator']
) as dag:

    start_task = DummyOperator(task_id='start')

    collect_data_task = PythonOperator(
        task_id='collect_data',
        python_callable=collect_execution_data,
        provide_context=True,
    )

    analyze_metrics_task = PythonOperator(
        task_id='analyze_metrics',
        python_callable=analyze_execution_metrics,
        provide_context=True,
    )

    generate_text_task = PythonOperator(
        task_id='generate_text_report',
        python_callable=generate_text_report,
        provide_context=True,
    )

    generate_html_task = PythonOperator(
        task_id='generate_html_report',
        python_callable=generate_html_report,
        provide_context=True,
    )

    send_email_task = PythonOperator(
        task_id='send_report_email',
        python_callable=send_report_email_task,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    store_metadata_task = PythonOperator(
        task_id='store_report_metadata',
        python_callable=store_report_metadata,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    cleanup_old_reports_task = PythonOperator(
        task_id='cleanup_old_reports',
        python_callable=cleanup_old_reports,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end_task = DummyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Dependencies
    start_task >> collect_data_task >> analyze_metrics_task
    analyze_metrics_task >> [generate_text_task, generate_html_task]
    [generate_text_task, generate_html_task] >> send_email_task
    send_email_task >> store_metadata_task >> cleanup_old_reports_task >> end_task
