#/config/backend.py
import os
import sys

# Add the root of the project directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(project_root)

from config.app import app_details 

table_details = {
    "migration_log_fact": {
        "name": f'{app_details["app_schema"]}.opssig_migration_log_fact',
        "path": f'{app_details["spark_catalog"]}{app_details["app_root"]}/opssig_migration_log_fact'
    },
    "departments_fact": {
        "name": f'{app_details["app_schema"]}.opssig_departments_fact',
        "path": f'{app_details["spark_catalog"]}{app_details["app_root"]}/opssig_departments_fact'
    },
    "features_fact": {
        "name": f'{app_details["app_schema"]}.opssig_features_fact',
        "path": f'{app_details["spark_catalog"]}{app_details["app_root"]}/opssig_features_fact'
    },
    "controls_fact": {
        "name": f'{app_details["app_schema"]}.opssig_controls_fact',
        "path": f'{app_details["spark_catalog"]}{app_details["app_root"]}/opssig_controls_fact'
    },
    "control_runs_fact": {
        "name": f'{app_details["app_schema"]}.opssig_control_runs_fact',
        "path": f'{app_details["spark_catalog"]}{app_details["app_root"]}/opssig_control_runs_fact'
    },
    "notification_details_fact": {
        "name": f'{app_details["app_schema"]}.opssig_notifications_fact',
        "path": f'{app_details["spark_catalog"]}{app_details["app_root"]}/opssig_notifications_fact'
    },
    "notification_runs_fact": {
        "name": f'{app_details["app_schema"]}.opssig_notification_runs_fact',
        "path": f'{app_details["spark_catalog"]}{app_details["app_root"]}/opssig_notification_runs_fact'
    },
}

if __name__ == "__main__":
# Iterate over the dictionary and print the name and path
    print("List Backend Resources & Paths")
    for key, value in table_details.items():
        # Unpack name and path from the inner dictionary
        name, path = value.values()
        print(f"\n{key}:\n\t{name}\n\t{path}")
        
