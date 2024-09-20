from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.backend_config import table_paths
from config.spark_setup import get_spark_session

# Initialize Spark session
spark = get_spark_session()

# 1. Create the new control run tracking table
def create_control_run_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{table_paths['control_runs']}`
        (
            department_id STRING,
            feature_id STRING,
            feature_version STRING,
            control_id STRING,
            control_version STRING,
            control_signal_count STRING,
            run_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (department_id, feature_id, control_id);
    """)
    print("Control run tracking table created successfully.")

# 2. Insert data into the control run table
def insert_control_run_data(run_data):
    # Convert list of data to DataFrame
    df = spark.createDataFrame(run_data, ["department_id", "feature_id", "feature_version", "control_id", "control_version", "control_signal_count", "run_timestamp"])

    # Insert into control run table
    df.withColumn("run_timestamp", F.to_timestamp("run_timestamp")) \
      .write.format("delta") \
      .mode("append") \
      .save(table_paths['control_runs'])

    print("Control run data inserted successfully.")

# 3. Update data in the control run table
def update_control_run_data(control_id, control_version, new_signal_count):
    # Define the update SQL
    spark.sql(f"""
        MERGE INTO delta.`{table_paths['control_runs']}` AS target
        USING (
            SELECT
                '{control_id}' AS control_id,
                '{control_version}' AS control_version,
                '{new_signal_count}' AS control_signal_count
        ) AS source
        ON target.control_id = source.control_id AND target.control_version = source.control_version
        WHEN MATCHED THEN
          UPDATE SET target.control_signal_count = source.control_signal_count, target.run_timestamp = current_timestamp();
    """)
    
    print(f"Control {control_id} control_version {control_version} updated successfully.")

# 4. Delete data from the control run table
def delete_control_run(control_id, control_version):
    # Delete from control run table
    spark.sql(f"""
        DELETE FROM delta.`{table_paths['control_runs']}`
        WHERE control_id = '{control_id}' AND control_version = '{control_version}'
    """)
    
    print(f"Control {control_id} control_version {control_version} deleted successfully.")

# 5. Merge (upsert) data into the control run table
def merge_control_run_data(run_data):
    df = spark.createDataFrame(run_data, ["department_id", "feature_id", "feature_version", "control_id", "control_version", "control_signal_count", "run_timestamp"])
    
    # Merge new data into the table
    df.createOrReplaceTempView("source_table")

    spark.sql(f"""
        MERGE INTO delta.`{table_paths['control_runs']}` AS target
        USING source_table AS source
        ON target.control_id = source.control_id AND target.control_version = source.control_version
        WHEN MATCHED THEN
          UPDATE SET target.control_signal_count = source.control_signal_count,
                     target.run_timestamp = current_timestamp()
        WHEN NOT MATCHED THEN
          INSERT (department_id, feature_id, feature_version, control_id, control_version, control_signal_count, run_timestamp)
          VALUES (source.department_id, source.feature_id, source.feature_version, source.control_id, source.control_version, source.control_signal_count, current_timestamp());
    """)
    
    print("Control run data merged (upserted) successfully.")

# Example usage
if __name__ == "__main__":
    # 1. Create the control run table
    create_control_run_table()

    # 2. Insert sample control run data
    run_data = [
        ("Dept_001", "Feature_001", "v1.0", "Control_001", "v1.0", "10", "2024-09-10 10:00:00")
    ]
    insert_control_run_data(run_data)

    # 3. Update control run data
    update_control_run_data("Control_001", "v1.0", "15")

    # 4. Merge (upsert) control run data
    merge_data = [
        ("Dept_002", "Feature_002", "v2.0", "Control_002", "v2.0", "20", "2024-09-11 12:00:00")
    ]
    merge_control_run_data(merge_data)

    # 5. Delete control run
    delete_control_run("Control_001", "v1.0")
