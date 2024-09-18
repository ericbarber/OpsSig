from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.backend_config import table_paths
from config.spark_setup import get_spark_session

# Initialize Spark session
spark = get_spark_session()

# Create the features table
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
            created_timestamp TIMESTAMP,
            modified_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (department_id);
    """)
    print("Features table created successfully.")

# Select details departments table
def get_feature_by_id(feature_id):
    return spark.sql(f"""
        SELECT * FROM `{table_paths['features']}`
        WHERE feature_id = '{feature_id}'
    """)

# Insert data into the features table
def insert_feature_data(feature_data):
    # Convert list of data to DataFrame
    df = spark.createDataFrame(feature_data, ["department_id", "feature_id", "feature_name", "feature_version", "feature_query_id", "feature_logic", "triage_team", "created_timestamp", "modified_timestamp"])

    # Insert into features table
    df.withColumn("created_timestamp", F.to_timestamp("created_timestamp")) \
      .withColumn("modified_timestamp", F.to_timestamp("modified_timestamp")) \
      .write.format("delta") \
      .mode("append") \
      .save(table_paths['features'])

    print("Feature data inserted successfully.")

# Update data in the features table
def update_feature_data(feature_id, new_feature_name, new_feature_version):
    # Define the update SQL
    spark.sql(f"""
        MERGE INTO delta.`{table_paths['features']}` AS target
        USING (SELECT '{feature_id}' AS feature_id, '{new_feature_name}' AS feature_name, '{new_feature_version}' AS feature_version) AS source
        ON target.feature_id = source.feature_id
        WHEN MATCHED THEN
          UPDATE SET target.feature_name = source.feature_name, target.feature_version = source.feature_version, target.modified_timestamp = current_timestamp();
    """)
    
    print(f"Feature {feature_id} updated successfully.")

# Delete data from the features table
def delete_feature(feature_id):
    # Delete from features table
    spark.sql(f"""
        DELETE FROM delta.`{table_paths['features']}`
        WHERE feature_id = '{feature_id}'
    """)
    
    print(f"Feature {feature_id} deleted successfully.")

# Merge (upsert) data into the features table
def merge_feature_data(feature_data):
    df = spark.createDataFrame(feature_data, ["department_id", "feature_id", "feature_name", "feature_version", "feature_query_id", "feature_logic", "triage_team", "created_timestamp", "modified_timestamp"])
    
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
                     target.modified_timestamp = current_timestamp()
        WHEN NOT MATCHED THEN
          INSERT (department_id, feature_id, feature_name, feature_version, feature_query_id, feature_logic, triage_team, created_timestamp, modified_timestamp)
          VALUES (source.department_id, source.feature_id, source.feature_name, source.feature_version, source.feature_query_id, source.feature_logic, source.triage_team, current_timestamp(), current_timestamp());
    """)
    
    print("Feature data merged (upserted) successfully.")

# Example usage
if __name__ == "__main__":
    # 1. Create the features table
    create_features_table()

    # 2. Insert sample feature data
    data = [
        ("Dept_001", "Feature_001", "Feature A", "v1.0", "query_001", "notebook_001", ["alice@example.com", "bob@example.com"], "2024-09-10 10:00:00", "2024-09-10 10:00:00")
    ]
    insert_feature_data(data)

    # 3. Update feature data
    update_feature_data("Feature_001", "Feature A Updated", "v1.1")

    # 4. Merge (upsert) feature data
    merge_data = [
        ("Dept_002", "Feature_002", "Feature B", "v2.0", "query_002", "notebook_002", ["eve@example.com"], "2024-09-11 12:00:00", "2024-09-11 12:00:00")
    ]
    merge_feature_data(merge_data)

    # 5. Delete feature
    delete_feature("Feature_001")

