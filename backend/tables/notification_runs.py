from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from config.backend_config import table_paths
from config.spark_setup import get_spark_session

# Initialize Spark session
spark = get_spark_session()

# Create the notification runs table
def create_notification_runs_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{table_paths['notification_runs']}`
        (
            control_id STRING,
            notification_run_id STRING,
            notification_id STRING,
            status STRING,
            recipient_details STRING,
            response_code STRING,
            retry_count INT,
            error_message STRING,
            run_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (control_id);
    """)
    print("Notification runs table created successfully.")

# Insert data into the notification runs table
def insert_notification_run_data(run_data):
    table_schema = spark.read.format("delta").load(table_paths['notification_runs']).schema

    notification_run_row = Row(*table_schema.fieldNames())(*run_data)
    df = spark.createDataFrame([notification_run_row], schema=table_schema)

    df.withColumn("run_timestamp", F.to_timestamp("run_timestamp")) \
      .write.format("delta") \
      .mode("append") \
      .save(table_paths['notification_runs'])

    print("Notification run data inserted successfully.")

# Update data in the notification runs table
def update_notification_run_data(notification_run_id, new_status, new_response_code):
    spark.sql(f"""
        MERGE INTO delta.`{table_paths['notification_runs']}` AS target
        USING (SELECT '{notification_run_id}' AS notification_run_id, '{new_status}' AS status, '{new_response_code}' AS response_code) AS source
        ON target.notification_run_id = source.notification_run_id
        WHEN MATCHED THEN
          UPDATE SET target.status = source.status, target.response_code = source.response_code, target.run_timestamp = current_timestamp();
    """)
    
    print(f"Notification run {notification_run_id} updated successfully.")

# Delete data from the notification runs table
def delete_notification_run(notification_run_id):
    spark.sql(f"""
        DELETE FROM delta.`{table_paths['notification_runs']}`
        WHERE notification_run_id = '{notification_run_id}'
    """)
    
    print(f"Notification run {notification_run_id} deleted successfully.")

# Merge (upsert) data into the notification runs table
def merge_notification_run_data(run_data):
    table_schema = spark.read.format("delta").load(table_paths['notification_runs']).schema
    table_schema = table_schema[:-1]
    
    notification_run_row = Row(
        notification_run_id=run_data[0],
        notification_id=run_data[1],
        control_id=run_data[2],
        status=run_data[3],
        recipient_details=run_data[4],
        response_code=run_data[5],
        retry_count=run_data[6],
        error_message=run_data[7],
    )

    df = spark.createDataFrame([notification_run_row], schema=table_schema)
    df.createOrReplaceTempView("source_table")

    spark.sql(f"""
        MERGE INTO delta.`{table_paths['notification_runs']}` AS target
        USING source_table AS source
        ON target.notification_run_id = source.notification_run_id
        WHEN MATCHED THEN
          UPDATE SET target.status = source.status,
                     target.response_code = source.response_code,
                     target.retry_count = source.retry_count,
                     target.error_message = source.error_message,
                     target.run_timestamp = current_timestamp()
        WHEN NOT MATCHED THEN
          INSERT (control_id, notification_run_id, notification_id, status, recipient_details, response_code, retry_count, error_message, run_timestamp)
          VALUES (source.control_id, source.notification_run_id, source.notification_id, source.status, source.recipient_details, source.response_code, source.retry_count, source.error_message, current_timestamp());
    """)
    
    print("Notification run data merged (upserted) successfully.")
