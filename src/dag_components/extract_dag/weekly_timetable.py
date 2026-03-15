"""
Custom Weekly Workflow Timetable for Airflow
Path: /opt/airflow/src/dag_components/extract_dag/weekly_timetable.py
"""

import os
import logging
from datetime import datetime, timedelta
import pytz

from airflow.timetables.base import Timetable, DataInterval, DagRunInfo
from airflow.plugins_manager import AirflowPlugin

logger = logging.getLogger(__name__)


# ================================================================
# Helper function: get_system_setting
# ================================================================
def get_system_setting(setting_name: str, default=None):
    """
    Retrieve a system setting from environment variables or default.

    This allows you to store configuration for weekly timetables,
    such as the default run day or timezone.
    """
    value = os.getenv(setting_name, default)
    logger.info(f"System setting fetched: {setting_name} = {value}")
    return value


# ================================================================
# Custom Weekly Workflow Timetable
# ================================================================
class WeeklyWorkflowTimetable(Timetable):
    """
    A custom timetable that triggers a DAG once per week on a specified weekday and time.
    """

    def __init__(self, week_day: int = 0, hour: int = 0, minute: int = 0, timezone: str = "UTC"):
        """
        :param week_day: 0 = Monday, 6 = Sunday
        :param hour: Hour of day to run
        :param minute: Minute of hour to run
        :param timezone: Timezone string (e.g. "UTC" or "Africa/Nairobi")
        """
        self.week_day = week_day
        self.hour = hour
        self.minute = minute
        self.timezone = pytz.timezone(timezone)

    def infer_manual_data_interval(self, run_after: datetime):
        """Define how manual DAG runs infer their data interval."""
        start = run_after - timedelta(days=7)
        end = run_after
        return DataInterval(start=start, end=end)

    def next_dagrun_info(self, *, last_automated_data_interval, restriction):
        """Compute the next DAG run time."""
        now = datetime.now(self.timezone)
        next_run = (now + timedelta(days=(self.week_day - now.weekday()) % 7)).replace(
            hour=self.hour, minute=self.minute, second=0, microsecond=0
        )
        if next_run <= now:
            next_run += timedelta(days=7)

        logger.info(f"Next weekly DAG run scheduled for: {next_run.isoformat()}")
        return DagRunInfo.interval(start=next_run, end=next_run + timedelta(days=7))


# ================================================================
# Plugin Registration
# ================================================================
class WeeklyTimetablePlugin(AirflowPlugin):
    name = "weekly_workflow_timetable"
    timetables = [WeeklyWorkflowTimetable]