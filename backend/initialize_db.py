import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql.utils import AnalysisException
from config.spark_setup import get_spark_session
from config.backend_config import table_paths

from backend.tables.departments import create_departments_table
from backend.tables.features import create_features_table
from backend.tables.controls import create_controls_table
from backend.tables.control_runs import create_control_run_table
from backend.tables.notification_details import create_notification_details_table
from backend.tables.notification_runs import create_notification_runs_table

# Initialize Spark session
spark = get_spark_session()

# Create table dict:
create_table_dict = {
    "departments": create_departments_table,
    "features": create_features_table,
    "controls": create_controls_table,
    "control_runs": create_control_run_table,
    "notification_details": create_notification_details_table,
    "notification_runs": create_notification_runs_table,
}

def initialize_database():
    print('\n####### Delta Tables #######\n')

    #Check if tables exist and print their locations
    print("\nDelta Tables: checking table existence and paths...\n")

    for table_name, table_path in table_paths.items():
        try:
            # Check if the Delta table exists
            df = spark.read.format("delta").load(table_path)
            print(f"\t{table_name} exists at {table_path}")

        except AnalysisException as e:
            print(f"\t{table_name} does not exist, atempt to create now")

            # Look up table name in create_table_dict and execute corresponding function
            create_function = create_table_dict.get(table_name)
            if create_function:
                create_function()  # Execute the table creation function
                print(f"\t{table_name} table created successfully")
            else:
                print(f"\t***No creation function found for {table_name}***")

       
        except Exception as e:
            print(f"\t{table_name} does not exist or could not be loaded: {e}")

if __name__ == "__main__":
    initialize_database()
    print("Delta Tables: initialized successfully!")

