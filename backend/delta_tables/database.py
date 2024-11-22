#/backend/delta_tables/database.py
from pyspark.sql import SparkSession

def switch_to_schema(spark: SparkSession, schema_name: str):
    # Get the list of available schemas using dot notation
    schemas = [row.name for row in spark.catalog.listDatabases()]
    
    # Check if the specified schema is in the list of available schemas
    if schema_name in schemas:
        # Switch to the specified schema
        spark.sql(f"USE {schema_name}")
        print(f"Switched to schema: {schema_name}")
    else:
        # Raise an error if access to the schema is not allowed
        raise ValueError("Schema access not allowed")

def create_schema_if_not_exists(spark: SparkSession, schema_name: str):
    # Get the list of available schemas using dot notation
    schemas = [row.name for row in spark.catalog.listDatabases()]

    # Check if the specified schema is in the list of available schemas
    if schema_name not in schemas:
        # Create the schema if it does not exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
        print(f"Schema '{schema_name}' created successfully.")
        # Switch to the specified schema
    else:
        print(f"Schema '{schema_name}' already exists.")

def get_table_path_if_exists(spark: SparkSession, table_name: str):
    try:
        # Check if the table is already in the catalog
        tables = spark.sql(f"SHOW TABLES LIKE '{table_name}'").collect()
        if tables:
            # If the table is found, get the database and table name
            database = tables[0]['database']
            table = tables[0]['tableName']
            
            # Get the table details to retrieve the location
            table_details = spark.sql(f"DESCRIBE DETAIL {database}.{table}").collect()
            if table_details:
                # Return the table path (location)
                return table_details[0]['location']
        return None
    except Exception as e:
        print(f"Error checking table existence: {e}")
        return None

if __name__ == '__main__':
# Example usage
    spark = SparkSession.builder \
        .appName("Schema Access Check") \
        .getOrCreate()

    try:
        # Attempt to create the specified schema
        create_schema_if_not_exists(spark, "cdo")
        # Attempt to switch to the specified schema
        switch_to_schema(spark, "cdo")
    except ValueError as e:
        print(e)
