#/config/examples.py
import os
import sys

# Add the root of the project directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(project_root)

from config.app import app_details 

examples_details = {
    "high_precission": {
        "name": f'{app_details["app_examples_schema"]}.temperature_data_high',
        "path": f'{app_details["spark_catalog"]}{app_details["app_root"]}/examples/migration_log_fact'
    },
    "medium_precission": {
        "name": f'{app_details["app_examples_schema"]}.temperature_data_medium',
        "path": f'{app_details["spark_catalog"]}{app_details["app_root"]}/examples/departments_fact'
    },
    "low_precission": {
        "name": f'{app_details["app_examples_schema"]}.temperature_data_low',
        "path": f'{app_details["spark_catalog"]}{app_details["app_root"]}/examples/features_fact'
    },
}

if __name__ == "__main__":
# Iterate over the dictionary and print the name and path
    print("List Backend Resources & Paths")
    for key, value in examples_details.items():
        # Unpack name and path from the inner dictionary
        name, path = value.values()
        print(f"\n{key}:\n\t{name}\n\t{path}")
        
