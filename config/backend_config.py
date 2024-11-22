# Table paths for all Delta tables
#/config/backend_config.py
import os
import sys

# Add the root of the project directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(project_root)

from config.app import app_details 

app_details_spark_catalog = app_details['spark_catalog']

table_paths = {
    "departments": f"{app_details_spark_catalog}/delta/departments",
    "features": f"{app_details_spark_catalog}/delta/features",
    "controls": f"{app_details_spark_catalog}/delta/controls",
    "control_runs": f"{app_details_spark_catalog}/delta/control_runs",
    "signal_detection": f"{app_details_spark_catalog}/delta/signal_detection",
    "notification_details": f"{app_details_spark_catalog}/delta/notification_details",
    "notification_runs": f"{app_details_spark_catalog}/delta/notification_runs"
}

# Table paths for examples
example_paths = {
    "high_precission": f"{app_details_spark_catalog}/example/delta/high_precission",
    "medium_precission": f"{app_details_spark_catalog}/example/delta/medium_precission",
    "low_precission": f"{app_details_spark_catalog}/example/delta/low_precission",
}
