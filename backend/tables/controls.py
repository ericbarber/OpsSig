from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.backend_config import table_paths
from config.spark_setup import get_spark_session

# Initialize Spark session
spark = get_spark_session()

# 1. Create the controls table
def create_controls_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{table_paths['controls']}`
        (
            department_id STRING,
            feature_id STRING,
            control_id STRING,
            control_group STRING,
            control_version STRING,
            control_notebook_id STRING,
            control_notebook_url STRING,
            created_timestamp TIMESTAMP,
            modified_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (department_id, control_group);
    """)
    print("Controls table created successfully.")

# 2. Insert data into the controls table
def insert_control_data(control_data):
    # Convert list of data to DataFrame
    df = spark.createDataFrame(control_data, ["department_id", "feature_id", "control_id", "control_group", "control_version", "control_notebook_id", "control_notebook_url", "created_timestamp", "modified_timestamp"])

    # Insert into controls table
    df.withColumn("created_timestamp", F.to_timestamp("created_timestamp")) \
      .withColumn("modified_timestamp", F.to_timestamp("modified_timestamp")) \
      .write.format("delta") \
      .mode("append") \
      .save(table_paths['controls'])

    print("Control data inserted successfully.")

# 3. Update data in the controls table
def update_control_data(control_id, control_version, new_control_notebook_url):
    # Define the update SQL
    spark.sql(f"""
        MERGE INTO delta.`{table_paths['controls']}` AS target
        USING (
            SELECT
                '{control_id}' AS control_id,
                '{control_version}' AS control_version,
                '{new_control_notebook_url}' AS control_notebook_url
        ) AS source
        ON target.control_id = source.control_id AND target.control_version = source.control_version
        WHEN MATCHED THEN
          UPDATE SET target.control_notebook_url = source.control_notebook_url, target.modified_timestamp = current_timestamp();
    """)
    
    print(f"Control {control_id} control_version {control_version} updated successfully.")

# 4. Delete data from the controls table
def delete_control(control_id, control_version):
    # Delete from controls table
    spark.sql(f"""
        DELETE FROM delta.`{table_paths['controls']}`
        WHERE control_id = '{control_id}' AND control_version = '{control_version}'
    """)
    
    print(f"Control {control_id} control_version {control_version} deleted successfully.")

# 5. Merge (upsert) data into the controls table
def merge_control_data(control_data):
    df = spark.createDataFrame(control_data, ["department_id", "feature_id", "control_id", "control_group", "control_version", "control_notebook_id", "control_notebook_url", "created_timestamp", "modified_timestamp"])
    
    # Merge new data into the table
    df.createOrReplaceTempView("source_table")

    spark.sql(f"""
        MERGE INTO delta.`{table_paths['controls']}` AS target
        USING source_table AS source
        ON target.control_id = source.control_id AND target.control_version = source.control_version
        WHEN MATCHED THEN
          UPDATE SET target.control_notebook_url = source.control_notebook_url,
                     target.modified_timestamp = current_timestamp()
        WHEN NOT MATCHED THEN
          INSERT (department_id, feature_id, control_id, control_group, control_version, control_notebook_id, control_notebook_url, created_timestamp, modified_timestamp)
          VALUES (source.department_id, source.feature_id, source.control_id, source.control_group, source.control_version, source.control_notebook_id, source.control_notebook_url, current_timestamp(), current_timestamp());
    """)
    
    print("Control data merged (upserted) successfully.")

# Example usage
if __name__ == "__main__":
    # 1. Create the controls table
    create_controls_table()

    # 2. Insert sample control data
    data = [
        ("Dept_001", "Feature_001", "Group_001", "Control_001", "v1.0", "notebook_001", "url_001", "2024-09-10 10:00:00", "2024-09-10 10:00:00")
    ]
    insert_control_data(data)

    # 3. Update control data
    update_control_data("Control_001", "v1.0", "updated_url_001")

    # 4. Merge (upsert) control data
    merge_data = [
        ("Dept_002", "Feature_002", "Group_002", "Control_002", "v2.0", "notebook_002", "url_002", "2024-09-11 12:00:00", "2024-09-11 12:00:00")
    ]
    merge_control_data(merge_data)

    # 5. Delete control
    delete_control("Control_001", "v1.0")
