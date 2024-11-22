from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from config.backend import table_details

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

notification_runs_table_name = str(table_details['notification_runs_fact']['name'])

# Create the notification runs table
def create_notification_runs_table():
    notification_runs_schema = StructType([
        StructField("control_id", StringType(), False),
        StructField("notification_run_id", StringType(), False),
        StructField("notification_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("recipient_details", StringType(), True),
        StructField("response_code", StringType(), True),
        StructField("retry_count", IntegerType(), True),
        StructField("error_message", StringType(), True),
        StructField("run_timestamp", TimestampType(), True),
    ])
    
    DeltaTable.createIfNotExists(spark) \
        .tableName(notification_runs_table_name) \
        .addColumns(notification_runs_schema) \
        .partitionedBy("control_id") \
        .execute()
    
    print("Notification runs table created successfully.")

# Insert data into the notification runs table
def insert_notification_run_data(run_data):
    schema = spark.table(notification_runs_table_name).schema
    df = spark.createDataFrame([Row(*run_data)], schema=schema)
    df = df.withColumn("run_timestamp", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(notification_runs_table_name)
    print("Notification run data inserted successfully.")

# Merge (upsert) data into the notification runs table
def merge_notification_run_data(run_data):
    schema = spark.table(notification_runs_table_name).schema
    df = spark.createDataFrame([Row(*run_data)], schema=schema)
    df = df.withColumn("run_timestamp", F.current_timestamp())

    delta_table = DeltaTable.forName(spark, notification_runs_table_name)
    delta_table.alias("target").merge(
        df.alias("source"),
        "target.notification_run_id = source.notification_run_id"
    ).whenMatchedUpdate(
        set={
            "status": "source.status",
            "response_code": "source.response_code",
            "retry_count": "source.retry_count",
            "error_message": "source.error_message",
            "run_timestamp": F.current_timestamp(),
        }
    ).whenNotMatchedInsert(
        values={
            "control_id": "source.control_id",
            "notification_run_id": "source.notification_run_id",
            "notification_id": "source.notification_id",
            "status": "source.status",
            "recipient_details": "source.recipient_details",
            "response_code": "source.response_code",
            "retry_count": "source.retry_count",
            "error_message": "source.error_message",
            "run_timestamp": F.current_timestamp(),
        }
    ).execute()
    print("Notification run data merged (upserted) successfully.")

# Update data in the notification runs table
def update_notification_run_data(notification_run_id, new_status, new_response_code):
    delta_table = DeltaTable.forName(spark, notification_runs_table_name)
    delta_table.update(
        condition=F.col("notification_run_id") == notification_run_id,
        set={
            "status": F.lit(new_status),
            "response_code": F.lit(new_response_code),
            "run_timestamp": F.current_timestamp(),
        }
    )
    print(f"Notification run {notification_run_id} updated successfully.")

# Delete data from the notification runs table
def delete_notification_run(notification_run_id):
    delta_table = DeltaTable.forName(spark, notification_runs_table_name)
    delta_table.delete(
        F.col("notification_run_id") == notification_run_id
    )
    print(f"Notification run {notification_run_id} deleted successfully.")
