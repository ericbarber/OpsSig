import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.spark_setup import get_spark_session
from config.backend_config import table_paths

from backend.tables.departments import create_departments_table
from backend.tables.features import create_features_table
from backend.tables.controls import create_controls_table

# Initialize Spark session
spark = get_spark_session()

def initialize_database():
    print('\n####### Delta Tables #######\n')
    # Create necessary tables
    create_departments_table()
    create_features_table()
    create_controls_table()
    # Add more table creation functions as needed
    
    #Check if tables exist and print their locations
    check_tables()
    
def check_tables():
    print("\nDelta Tables: checking table existence and paths...\n")

    for table_name, table_path in table_paths.items():
        try:
            # Check if the Delta table exists
            df = spark.read.format("delta").load(table_path)
            print(f"\t{table_name} exists at {table_path}")
        except Exception as e:
            print(f"\t{table_name} does not exist or could not be loaded: {e}")

if __name__ == "__main__":
    initialize_database()
    print("Delta Tables: initialized successfully!")

