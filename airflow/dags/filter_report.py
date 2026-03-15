"""
SIMPLIFIED Filter Links Report DAG
Sends concise email with:
- Day and time of filtering
- Number of workflows updated with links
- List of links and their tweeted times
UPDATED: Now includes success/failure statistics
"""

import sys
import os
import logging
from datetime import datetime, timedelta, timezone

sys.path.append('/opt/airflow/src')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from pymongo import MongoClient
from core.database.postgres.connection import get_postgres_connection
from streamlit.ui.settings.settings_manager import get_system_setting

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://app_user:app_password@mongodb:27017/messages_db')

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
    """Get MongoDB connection"""
    client = MongoClient(MONGODB_URI)
    return client['messages_db']


def send_email_report(subject, text_body, html_body=None):
    """Send email with both text and HTML versions"""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    try:
        email_host = os.getenv('EMAIL_HOST', 'smtp.gmail.com')
        email_port = int(os.getenv('EMAIL_PORT', '587'))
        email_username = os.getenv('EMAIL_USERNAME')
        email_password = os.getenv('EMAIL_PASSWORD')
        email_from = os.getenv('EMAIL_FROM', email_username)
        email_to = os.getenv('EMAIL_TO', email_username)

        if not email_username or not email_password:
            logger.warning("⚠️ Email credentials not configured")
            return False

        msg = MIMEMultipart('alternative')
        msg['From'] = email_from
        msg['To'] = email_to
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
        import traceback
        logger.error(traceback.format_exc())
        return False


def get_weekly_workflow_config():
    """Get weekly workflow configuration"""
    try:
        current_day = datetime.now().strftime('%A').lower()
        weekly_settings = get_system_setting('weekly_workflow_settings', {})
        day_config = weekly_settings.get(current_day, {})
        filtering_config = day_config.get('filtering_config', {})

        if not filtering_config:
            return {
                'enabled': False,
                'hours_limit': 24,
                'day': current_day
            }

        config = {
            'enabled': filtering_config.get('enabled', False),
            'hours_limit': filtering_config.get('hours_limit', 24),
            'day': current_day,
        }

        return config

    except Exception as e:
        logger.error(f"Error loading weekly config: {e}")
        return {
            'enabled': False,
            'hours_limit': 24,
            'day': datetime.now().strftime('%A').lower()
        }


# ============================================================================
# DATA COLLECTION (UPDATED)
# ============================================================================

def collect_simple_filter_data(**kwargs):
    """Collect simple filtering data: day, time, workflows updated, and links with success/failure"""
    try:
        config = get_weekly_workflow_config()
        hours_limit = config.get('hours_limit', 24)
        current_day = config.get('day', datetime.now().strftime('%A').lower())

        # Calculate time range based on hours_limit
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=hours_limit)

        logger.info(f"📊 Collecting simple filter data from {start_time} to {end_time}")

        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                # ✅ UPDATED: Count workflows (links) with success/failure stats
                cur.execute("""
                    SELECT
                        COUNT(*) as total_count,
                        SUM(CASE WHEN success = TRUE THEN 1 ELSE 0 END) as success_count,
                        SUM(CASE WHEN failure = TRUE THEN 1 ELSE 0 END) as failure_count,
                        SUM(CASE WHEN success = TRUE AND failure = TRUE THEN 1 ELSE 0 END) as both_true_count
                    FROM links
                    WHERE processed_by_workflow = TRUE
                      AND workflow_processed_time >= %s
                      AND workflow_processed_time <= %s
                      AND connected_content_id IS NOT NULL
                """, (start_time, end_time))
                result = cur.fetchone()
                try:
                    total_count = result['total_count'] if result else 0
                    success_count = result['success_count'] if result else 0
                    failure_count = result['failure_count'] if result else 0
                    both_true_count = result['both_true_count'] if result else 0
                except (KeyError, TypeError):
                    total_count = result[0] if result else 0
                    success_count = result[1] if result and len(result) > 1 else 0
                    failure_count = result[2] if result and len(result) > 2 else 0
                    both_true_count = result[3] if result and len(result) > 3 else 0

                # Calculate success rate
                success_rate = (success_count / total_count * 100) if total_count > 0 else 0

                # ✅ UPDATED: Get all links with success/failure status
                cur.execute("""
                    SELECT
                        l.links_id,
                        l.link,
                        l.tweeted_time,
                        l.tweeted_date,
                        l.workflow_type,
                        l.workflow_status,
                        l.connected_content_id,
                        l.connected_via_workflow,
                        l.workflow_processed_time,
                        l.executed,
                        l.success,
                        l.failure,
                        a.username as account_username
                    FROM links l
                    LEFT JOIN accounts a ON l.account_id = a.account_id
                    WHERE l.processed_by_workflow = TRUE
                      AND l.workflow_processed_time >= %s
                      AND l.workflow_processed_time <= %s
                    ORDER BY l.workflow_processed_time DESC
                """, (start_time, end_time))

                links_data = []
                if cur.rowcount > 0:
                    for row in cur.fetchall():
                        try:
                            # Try dict access
                            success_flag = row['success']
                            failure_flag = row['failure']
                            status = '✅ Success' if success_flag else '❌ Failure' if failure_flag else '⚪ Pending'

                            links_data.append({
                                'link_id': row['links_id'],
                                'url': row['link'],
                                'tweeted_time': row['tweeted_time'].strftime('%Y-%m-%d %H:%M:%S UTC') if row['tweeted_time'] else 'N/A',
                                'tweeted_date': row['tweeted_date'].strftime('%Y-%m-%d') if row['tweeted_date'] else 'N/A',
                                'workflow_type': row['workflow_type'] or 'N/A',
                                'workflow_status': row['workflow_status'] or 'pending',
                                'execution_status': row['executed'],
                                'success': success_flag,
                                'failure': failure_flag,
                                'status_badge': status,
                                'content_id': row['connected_content_id'],
                                'workflow_name': row['connected_via_workflow'] or 'N/A',
                                'assigned_time': row['workflow_processed_time'].strftime('%Y-%m-%d %H:%M:%S UTC') if row['workflow_processed_time'] else 'N/A',
                                'account': row['account_username'] or 'Unknown'
                            })
                        except (KeyError, TypeError):
                            # Fall back to tuple access
                            success_flag = row[10] if len(row) > 10 else False
                            failure_flag = row[11] if len(row) > 11 else False
                            status = '✅ Success' if success_flag else '❌ Failure' if failure_flag else '⚪ Pending'

                            links_data.append({
                                'link_id': row[0],
                                'url': row[1],
                                'tweeted_time': row[2].strftime('%Y-%m-%d %H:%M:%S UTC') if row[2] else 'N/A',
                                'tweeted_date': row[3].strftime('%Y-%m-%d') if row[3] else 'N/A',
                                'workflow_type': row[4] or 'N/A',
                                'workflow_status': row[5] or 'pending',
                                'execution_status': row[9] if len(row) > 9 else False,
                                'success': success_flag,
                                'failure': failure_flag,
                                'status_badge': status,
                                'content_id': row[6],
                                'workflow_name': row[7] or 'N/A',
                                'assigned_time': row[8].strftime('%Y-%m-%d %H:%M:%S UTC') if row[8] else 'N/A',
                                'account': row[12] if len(row) > 12 else 'Unknown'
                            })

        data = {
            'day': current_day.title(),
            'report_time': end_time.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'time_range': {
                'start': start_time.strftime('%Y-%m-%d %H:%M:%S UTC'),
                'end': end_time.strftime('%Y-%m-%d %H:%M:%S UTC'),
                'hours': hours_limit
            },
            'workflows_updated_count': total_count,
            'success_stats': {
                'success_count': success_count,
                'failure_count': failure_count,
                'both_true_count': both_true_count,
                'success_rate': round(success_rate, 1),
                'success_rate_formatted': f"{success_rate:.1f}%"
            },
            'total_links_assigned': len(links_data),
            'links': links_data
        }

        logger.info(f"✅ Collected data:")
        logger.info(f"   Day: {data['day']}")
        logger.info(f"   Workflows updated: {total_count}")
        logger.info(f"   ✅ Success: {success_count}")
        logger.info(f"   ❌ Failure: {failure_count}")
        logger.info(f"   📈 Success Rate: {success_rate:.1f}%")
        logger.info(f"   Total links assigned: {len(links_data)}")

        kwargs['ti'].xcom_push(key='filter_data', value=data)
        return data

    except Exception as e:
        logger.error(f"❌ Error collecting filter data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


# ============================================================================
# REPORT GENERATION (UPDATED)
# ============================================================================

def generate_simple_text_report(**kwargs):
    """Generate simple plain text report with success/failure"""
    try:
        ti = kwargs['ti']
        data = ti.xcom_pull(key='filter_data', task_ids='collect_simple_data')

        if not data:
            raise Exception("No filter data found")

        success_stats = data.get('success_stats', {})

        # Safely extract values with None handling
        success_count = success_stats.get('success_count', 0) or 0
        failure_count = success_stats.get('failure_count', 0) or 0
        both_true_count = success_stats.get('both_true_count', 0) or 0
        success_rate_formatted = success_stats.get('success_rate_formatted', '0%')

        lines = []
        lines.append("=" * 80)
        lines.append("LINK FILTERING REPORT")
        lines.append("=" * 80)
        lines.append("")
        lines.append(f"Day:                     {data.get('day', 'Unknown')}")
        lines.append(f"Report Generated:        {data.get('report_time', 'Unknown')}")
        lines.append(f"Period:                  Last {data.get('time_range', {}).get('hours', 24)} hours")
        lines.append(f"                         {data.get('time_range', {}).get('start', 'N/A')} to {data.get('time_range', {}).get('end', 'N/A')}")
        lines.append("")
        lines.append("=" * 80)
        lines.append("SUMMARY")
        lines.append("=" * 80)
        lines.append(f"Workflows Updated:       {data.get('workflows_updated_count', 0)} workflows connected to content")
        lines.append(f"✅ Success:              {success_count}")
        lines.append(f"❌ Failure:              {failure_count}")
        lines.append(f"📈 Success Rate:         {success_rate_formatted}")
        if both_true_count > 0:
            lines.append(f"⚠️  Data Error:           {both_true_count} links with both success and failure TRUE")
        lines.append(f"Links Assigned:          {data.get('total_links_assigned', 0)} links")
        lines.append("")

        if data.get('links'):
            lines.append("=" * 80)
            lines.append(f"LINKS ASSIGNED ({len(data['links'])} total)")
            lines.append("=" * 80)
            lines.append("")

            for idx, link in enumerate(data['links'], 1):
                lines.append(f"[{idx}] Link ID: {link.get('link_id', 'N/A')} - {link.get('status_badge', 'Unknown')}")
                lines.append(f"    URL:              {link.get('url', 'N/A')}")
                lines.append(f"    Tweeted Time:     {link.get('tweeted_time', 'N/A')}")
                lines.append(f"    Assigned Time:    {link.get('assigned_time', 'N/A')}")
                lines.append(f"    Workflow:         {link.get('workflow_name', 'N/A')}")
                lines.append(f"    Type:             {link.get('workflow_type', 'N/A')}")
                lines.append(f"    Status:           {link.get('workflow_status', 'N/A')}")
                lines.append(f"    Execution:        {'Executed' if link.get('execution_status') else 'Not Executed'}")
                lines.append(f"    Success:          {'✅ Yes' if link.get('success') else '❌ No'}")
                lines.append(f"    Failure:          {'✅ Yes' if link.get('failure') else '❌ No'}")
                if link.get('content_id'):
                    lines.append(f"    Connected To:     Content ID {link['content_id']}")
                lines.append(f"    Account:          {link.get('account', 'Unknown')}")
                lines.append("")
        else:
            lines.append("=" * 80)
            lines.append("No links were assigned during this period.")
            lines.append("=" * 80)

        lines.append("")
        lines.append("=" * 80)
        lines.append("END OF REPORT")
        lines.append("=" * 80)

        text_report = "\n".join(lines)
        kwargs['ti'].xcom_push(key='text_report', value=text_report)

        logger.info("✅ Text report generated with success/failure")
        return text_report

    except Exception as e:
        logger.error(f"❌ Error generating text report: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

def generate_simple_html_report(**kwargs):
    """Generate simple HTML report with success/failure"""
    try:
        ti = kwargs['ti']
        data = ti.xcom_pull(key='filter_data', task_ids='collect_simple_data')

        if not data:
            raise Exception("No filter data found")

        success_stats = data.get('success_stats', {})
        success_rate = success_stats.get('success_rate_formatted', '0%')
        success_rate_value = success_stats.get('success_rate', 0)

        html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; max-width: 1000px; margin: 0 auto; padding: 20px; background: #f5f7fa; }}
.header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 25px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
.header h1 {{ margin: 0 0 10px 0; font-size: 28px; }}
.header p {{ margin: 5px 0; opacity: 0.95; font-size: 14px; }}
.summary {{ background: white; padding: 25px; margin-bottom: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.08); }}
.summary h2 {{ color: #667eea; margin-top: 0; border-bottom: 2px solid #667eea; padding-bottom: 10px; font-size: 20px; }}
.metric-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0; }}
.metric {{ background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%); padding: 20px; border-radius: 8px; border-left: 4px solid #667eea; }}
.metric.success {{ border-left-color: #28a745; }}
.metric.failure {{ border-left-color: #dc3545; }}
.metric.rate {{ border-left-color: #007bff; }}
.metric-label {{ font-size: 12px; text-transform: uppercase; color: #6c757d; margin-bottom: 8px; font-weight: 600; letter-spacing: 0.5px; }}
.metric-value {{ font-size: 32px; font-weight: bold; color: #667eea; }}
.metric.success .metric-value {{ color: #28a745; }}
.metric.failure .metric-value {{ color: #dc3545; }}
.metric.rate .metric-value {{ color: #007bff; }}
.progress-bar {{ width: 100%; height: 20px; background: #e9ecef; border-radius: 10px; overflow: hidden; margin: 10px 0; }}
.progress-fill {{ height: 100%; background: linear-gradient(90deg, #28a745 0%, #20c997 100%); display: flex; align-items: center; justify-content: center; color: white; font-weight: bold; font-size: 11px; }}
.links-section {{ background: white; padding: 25px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.08); }}
.links-section h2 {{ color: #667eea; margin-top: 0; border-bottom: 2px solid #667eea; padding-bottom: 10px; font-size: 20px; }}
.link-card {{ background: #f8f9fa; padding: 18px; margin: 12px 0; border-radius: 8px; border-left: 4px solid #667eea; }}
.link-card.success {{ border-left-color: #28a745; }}
.link-card.failure {{ border-left-color: #dc3545; }}
.link-card .link-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }}
.link-card .link-number {{ font-size: 18px; font-weight: bold; color: #667eea; }}
.link-card.success .link-number {{ color: #28a745; }}
.link-card.failure .link-number {{ color: #dc3545; }}
.status-badge {{ display: inline-block; padding: 6px 12px; border-radius: 16px; font-size: 12px; font-weight: 600; text-transform: uppercase; }}
.status-success {{ background: #d4edda; color: #155724; }}
.status-failure {{ background: #f8d7da; color: #721c24; }}
.status-pending {{ background: #fff3cd; color: #856404; }}
.link-card .link-url {{ color: #495057; font-size: 13px; word-break: break-all; margin: 8px 0; background: white; padding: 8px; border-radius: 4px; }}
.link-card .link-details {{ display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 10px; font-size: 13px; }}
.link-card .detail-item {{ padding: 6px; background: white; border-radius: 4px; }}
.link-card .detail-label {{ font-weight: 600; color: #6c757d; }}
.link-card .detail-value {{ color: #495057; }}
.no-links {{ text-align: center; padding: 40px; color: #6c757d; font-size: 16px; }}
.data-error {{ background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%); border-left: 4px solid #ffc107; padding: 15px; margin: 15px 0; border-radius: 8px; }}
</style>
</head>
<body>

<div class="header">
<h1>🔗 Link Filtering Report</h1>
<p><strong>Day:</strong> {data['day']}</p>
<p><strong>Report Generated:</strong> {data['report_time']}</p>
<p><strong>Period:</strong> Last {data['time_range']['hours']} hours ({data['time_range']['start']} to {data['time_range']['end']})</p>
</div>

<div class="summary">
<h2>Summary</h2>
<div class="metric-grid">
<div class="metric">
<div class="metric-label">Workflows Updated</div>
<div class="metric-value">{data['workflows_updated_count']}</div>
</div>
<div class="metric success">
<div class="metric-label">✅ Success</div>
<div class="metric-value">{success_stats.get('success_count', 0)}</div>
</div>
<div class="metric failure">
<div class="metric-label">❌ Failure</div>
<div class="metric-value">{success_stats.get('failure_count', 0)}</div>
</div>
<div class="metric rate">
<div class="metric-label">📈 Success Rate</div>
<div class="metric-value">{success_rate}</div>
</div>
</div>
<div class="progress-bar">
<div class="progress-fill" style="width: {success_rate_value}%">{success_rate}</div>
</div>
<p>Links Assigned: <strong>{data['total_links_assigned']}</strong> links</p>
"""

        if success_stats.get('both_true_count', 0) > 0:
            html += f"""
<div class="data-error">
<strong>⚠️ Data Validation Issue:</strong> {success_stats.get('both_true_count', 0)} links have both success and failure = TRUE.
Please check database consistency.
</div>
"""

        html += """
</div>

<div class="links-section">
<h2>Links Assigned ({len(data['links'])} total)</h2>
"""

        if data['links']:
            for idx, link in enumerate(data['links'], 1):
                # Determine card class and status
                if link.get('success'):
                    card_class = 'success'
                    status_class = 'status-success'
                    status_text = '✅ Success'
                elif link.get('failure'):
                    card_class = 'failure'
                    status_class = 'status-failure'
                    status_text = '❌ Failure'
                else:
                    card_class = ''
                    status_class = 'status-pending'
                    status_text = '⚪ Pending'

                html += f"""
<div class="link-card {card_class}">
<div class="link-header">
<div class="link-number">#{idx} - Link ID: {link['link_id']}</div>
<div class="status-badge {status_class}">{status_text}</div>
</div>
<div class="link-url">🔗 {link['url']}</div>
<div class="link-details">
<div class="detail-item">
<span class="detail-label">Tweeted:</span><br>
<span class="detail-value">{link['tweeted_time']}</span>
</div>
<div class="detail-item">
<span class="detail-label">Assigned:</span><br>
<span class="detail-value">{link['assigned_time']}</span>
</div>
<div class="detail-item">
<span class="detail-label">Workflow:</span><br>
<span class="detail-value">{link['workflow_name']}</span>
</div>
<div class="detail-item">
<span class="detail-label">Type:</span><br>
<span class="detail-value">{link['workflow_type']}</span>
</div>
<div class="detail-item">
<span class="detail-label">Workflow Status:</span><br>
<span class="detail-value">{link['workflow_status']}</span>
</div>
<div class="detail-item">
<span class="detail-label">Executed:</span><br>
<span class="detail-value">{'✅ Yes' if link.get('execution_status') else '❌ No'}</span>
</div>
<div class="detail-item">
<span class="detail-label">Account:</span><br>
<span class="detail-value">{link['account']}</span>
</div>
"""
                if link.get('content_id'):
                    html += f"""
<div class="detail-item">
<span class="detail-label">Connected To:</span><br>
<span class="detail-value">Content ID {link['content_id']}</span>
</div>
"""
                html += """
</div>
</div>
"""
        else:
            html += """
<div class="no-links">
<p>No links were assigned during this period.</p>
</div>
"""

        html += """
</div>

</body>
</html>
"""

        kwargs['ti'].xcom_push(key='html_report', value=html)
        logger.info("✅ HTML report generated with success/failure")
        return html

    except Exception as e:
        logger.error(f"❌ Error generating HTML report: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


def send_simple_report_email(**kwargs):
    """Send the simple filtering report via email with success/failure"""
    try:
        ti = kwargs['ti']
        data = ti.xcom_pull(key='filter_data', task_ids='collect_simple_data')
        text_report = ti.xcom_pull(key='text_report', task_ids='generate_simple_text_report')
        html_report = ti.xcom_pull(key='html_report', task_ids='generate_simple_html_report')

        if not text_report or not html_report:
            logger.warning("⚠️ No reports found to send")
            return False

        success_stats = data.get('success_stats', {})
        success_rate = success_stats.get('success_rate_formatted', '0%')

        subject = f"🔗 Links Report - {data['day']} - {data['workflows_updated_count']} Workflows - Success: {success_rate}"

        success = send_email_report(subject, text_report, html_report)

        if success:
            logger.info("✅ Simple filtering report email sent successfully with success/failure")
        else:
            logger.warning("⚠️ Failed to send simple filtering report email")

        return success

    except Exception as e:
        logger.error(f"❌ Error sending filtering report: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


def store_simple_report_metadata(**kwargs):
    """Store simple report metadata in MongoDB with success/failure"""
    try:
        ti = kwargs['ti']
        data = ti.xcom_pull(key='filter_data', task_ids='collect_simple_data')
        email_sent = ti.xcom_pull(task_ids='send_simple_report_email')

        mongo_db = get_mongo_connection()

        report_doc = {
            'report_type': 'simple_link_filtering',
            'generated_at': datetime.now(timezone.utc),
            'day': data['day'],
            'time_range': data['time_range'],
            'workflows_updated_count': data['workflows_updated_count'],
            'success_stats': data.get('success_stats', {}),
            'total_links_assigned': data['total_links_assigned'],
            'email_sent': email_sent,
            'dag_run_id': kwargs.get('dag_run').run_id if kwargs.get('dag_run') else None,
            'created_by': 'filtering_report'  # ← Change from 'filter_links_report_simplified'
        }

        result = mongo_db.filter_reports.insert_one(report_doc)
        logger.info(f"✅ Simple report metadata stored with success/failure: {result.inserted_id}")

        return str(result.inserted_id)

    except Exception as e:
        logger.error(f"❌ Error storing report metadata: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


# ============================================================================
# DAG DEFINITION
# ============================================================================


with DAG(
    'filtering_report',  # ← Change from 'filter_links_report_simplified'
    default_args=DEFAULT_ARGS,
    description='Generate and send simplified link filtering reports with success/failure tracking',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['reporting', 'filtering', 'email', 'links', 'simplified', 'success-tracking']
) as dag:

    start_task = DummyOperator(
        task_id='start',
        dag=dag
    )

    # Task 1: Collect simple filtering data with success/failure
    collect_data_task = PythonOperator(
        task_id='collect_simple_data',
        python_callable=collect_simple_filter_data,
        provide_context=True,
        dag=dag
    )

    # Task 2: Generate simple text report with success/failure
    generate_text_task = PythonOperator(
        task_id='generate_simple_text_report',
        python_callable=generate_simple_text_report,
        provide_context=True,
        dag=dag
    )

    # Task 3: Generate simple HTML report with success/failure
    generate_html_task = PythonOperator(
        task_id='generate_simple_html_report',
        python_callable=generate_simple_html_report,
        provide_context=True,
        dag=dag
    )

    # Task 4: Send report via email with success/failure
    send_email_task = PythonOperator(
        task_id='send_simple_report_email',
        python_callable=send_simple_report_email,
        provide_context=True,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Task 5: Store report metadata with success/failure
    store_metadata_task = PythonOperator(
        task_id='store_simple_report_metadata',
        python_callable=store_simple_report_metadata,
        provide_context=True,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )

    end_task = DummyOperator(
        task_id='end',
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Define task dependencies
    start_task >> collect_data_task >> [generate_text_task, generate_html_task]
    [generate_text_task, generate_html_task] >> send_email_task >> store_metadata_task >> end_task
