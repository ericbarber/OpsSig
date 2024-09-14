from backend.tables.departments import create_departments_table
from backend.tables.features import create_features_table
from backend.tables.controls import create_controls_table
from config.spark_setup import get_spark_session

# Initialize Spark session
spark = get_spark_session()

def initialize_database():
    # Create necessary tables
    create_departments_table()
    create_features_table()
    create_controls_table()
    # Add more table creation functions as needed

if __name__ == "__main__":
    initialize_database()
    print("Database initialized successfully!")
