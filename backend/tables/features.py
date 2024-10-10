# backend/tables/stage/features.py

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from config.backend_config import table_paths
from config.spark_setup import get_spark_session

# Initialize Spark session
spark = get_spark_session()

# Create the features table with the production_status column
def create_features_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{table_paths['features']}`
        (
            department_id STRING,
            feature_id STRING,
            feature_name STRING,
            feature_version STRING,
            feature_query_id STRING,
            feature_logic STRING,
            triage_team ARRAY<STRING>,
            production_status BOOLEAN,
            created_timestamp TIMESTAMP,
            modified_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (department_id);
    """)
    print("Features table created successfully with production_status column.")

def insert_feature_data(feature_data):
    # Convert list of data to DataFrame, assuming production_status is either provided or set to False
    df = spark.createDataFrame(
        feature_data,
        ["department_id", "feature_id", "feature_name", "feature_version", "feature_query_id", "feature_logic", "triage_team", "production_status", "created_timestamp", "modified_timestamp"]
    )

    # Insert into features table
    df.withColumn("created_timestamp", F.to_timestamp("created_timestamp")) \
      .withColumn("modified_timestamp", F.to_timestamp("modified_timestamp")) \
      .write.format("delta") \
      .mode("append") \
      .save(table_paths['features'])

    print("Feature data inserted successfully.")

def merge_feature_data(feature_data):
    # Retrieve the schema from the Delta table
    table_schema = spark.read.format("delta").load(table_paths['features']).schema
    table_schema = table_schema[:-2]

    # Convert the list of feature data to a Row object
    department_row = Row(
        department_id=feature_data[0],
        feature_id=feature_data[1],
        feature_name=feature_data[2],
        feature_version=feature_data[3],
        feature_query_id=feature_data[4],
        feature_logic=feature_data[5],
        triage_team=feature_data[6],
        production_status=feature_data[7] 
    )

    # Convert list of data to DataFrame
    df = spark.createDataFrame([department_row], schema=table_schema)

    # Merge new data into the table
    df.createOrReplaceTempView("source_table")
    
    spark.sql(f"""
        MERGE INTO delta.`{table_paths['features']}` AS target
        USING source_table AS source
        ON target.feature_id = source.feature_id
        WHEN MATCHED THEN
          UPDATE SET target.feature_name = source.feature_name,
                     target.feature_version = source.feature_version,
                     target.feature_query_id = source.feature_query_id,
                     target.feature_logic = source.feature_logic,
                     target.triage_team = source.triage_team,
                     target.production_status = source.production_status,
                     target.modified_timestamp = current_timestamp()
        WHEN NOT MATCHED THEN
          INSERT (department_id, feature_id, feature_name, feature_version, feature_query_id, feature_logic, triage_team, production_status, created_timestamp, modified_timestamp)
          VALUES (source.department_id, source.feature_id, source.feature_name, source.feature_version, source.feature_query_id, source.feature_logic, source.triage_team, source.production_status, current_timestamp(), current_timestamp());
    """)
    
    print("Feature data merged (upserted) successfully.")

def update_feature_data(feature_id, new_feature_name, new_feature_version):
    spark.sql(f"""
        MERGE INTO delta.`{table_paths['features']}` AS target
        USING (SELECT '{feature_id}' AS feature_id, '{new_feature_name}' AS feature_name, '{new_feature_version}' AS feature_version) AS source
        ON target.feature_id = source.feature_id
        WHEN MATCHED THEN
          UPDATE SET target.feature_name = source.feature_name, 
                     target.feature_version = source.feature_version, 
                     target.modified_timestamp = current_timestamp();
    """)
    
    print(f"Feature {feature_id} updated successfully.")

def insert_new_feature_version(feature_id, department_id, feature_name, new_feature_version, feature_query_id, feature_logic, triage_team, production_status=False):
    # Convert list of data to DataFrame
    df = spark.createDataFrame(
        [(department_id, feature_id, feature_name, new_feature_version, feature_query_id, feature_logic, triage_team, production_status, None, None)],
        ["department_id", "feature_id", "feature_name", "feature_version", "feature_query_id", "feature_logic", "triage_team", "production_status", "created_timestamp", "modified_timestamp"]
    )
    
    # Add timestamps
    df = df.withColumn("created_timestamp", F.current_timestamp()) \
           .withColumn("modified_timestamp", F.current_timestamp())
    
    # Insert into features table
    df.write.format("delta") \
      .mode("append") \
      .save(table_paths['features'])

    print(f"New version {new_feature_version} for feature {feature_id} inserted successfully.")

def get_feature_by_id_and_version(feature_id, feature_version):
    return spark.sql(f"""
        SELECT * FROM delta.`{table_paths['features']}`
        WHERE feature_id = '{feature_id}' AND feature_version = '{feature_version}'
    """)

def get_production_feature(feature_id):
    return spark.sql(f"""
        SELECT * FROM delta.`{table_paths['features']}`
        WHERE feature_id = '{feature_id}' AND production_status = true
    """)

def update_production_status(feature_id, feature_version):
    # Set all versions to non-production for the feature
    spark.sql(f"""
        UPDATE delta.`{table_paths['features']}`
        SET production_status = false
        WHERE feature_id = '{feature_id}'
    """)
    
    # Set the specified version as production
    spark.sql(f"""
        UPDATE delta.`{table_paths['features']}`
        SET production_status = true
        WHERE feature_id = '{feature_id}' AND feature_version = '{feature_version}'
    """)
    
    print(f"Production status updated for feature {feature_id}, version {feature_version}.")


# Delete data from the features table
def delete_feature(feature_id):
    # Delete from features table
    spark.sql(f"""
        DELETE FROM delta.`{table_paths['features']}`
        WHERE feature_id = '{feature_id}'
    """)
    
    print(f"Feature {feature_id} deleted successfully.")


# Select details departments table
def get_feature_by_id(feature_id):
    return spark.sql(f"""
        SELECT * FROM delta.`{table_paths['features']}`
        WHERE feature_id = '{feature_id}'
        AND production_status = true
    """)
