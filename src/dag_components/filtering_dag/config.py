import os
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB URI
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://app_user:app_password@mongodb:27017/messages_db')

# Default arguments for the DAG
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}