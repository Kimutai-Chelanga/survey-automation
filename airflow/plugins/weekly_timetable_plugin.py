from airflow.plugins_manager import AirflowPlugin
from dag_components.extract_dag.weekly_timetable import WeeklyWorkflowTimetable

class WeeklyTimetablePlugin(AirflowPlugin):
    name = "weekly_timetable_plugin"
    timetables = [WeeklyWorkflowTimetable]

