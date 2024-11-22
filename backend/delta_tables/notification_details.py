from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from config.backend import table_details

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

notifications_table_name = str(table_details['notification_details_fact']['name'])

# Create the notification details table
def create_notification_details_table():
    notifications_schema = StructType([
        StructField("notification_id", StringType(), False),
        StructField("notification_version", StringType(), False),
        StructField("notification_type", StringType(), True),
        StructField("notification_priority", StringType(), True),
        StructField("control_id", StringType(), True),
        StructField("control_version", StringType(), True),
        StructField("notification_template_path", StringType(), True),
        StructField("notification_template_dir", StringType(), True),
        StructField("notification_template_file", StringType(), True),
        StructField("in_production", BooleanType(), True),
        StructField("created_timestamp", TimestampType(), True),
        StructField("modified_timestamp", TimestampType(), True),
    ])
    
    DeltaTable.createIfNotExists(spark) \
        .tableName(notifications_table_name) \
        .addColumns(notifications_schema) \
        .partitionedBy("notification_id") \
        .execute()
    
    print("Notification details table created successfully.")

# Insert data into the notification details table
def insert_notification_details(notification_data):
    schema = spark.table(notifications_table_name).schema
    df = spark.createDataFrame([Row(*notification_data)], schema=schema)
    df = df.withColumn("created_timestamp", F.current_timestamp()) \
           .withColumn("modified_timestamp", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(notifications_table_name)
    print("Notification details inserted successfully.")

# Merge (upsert) data into the notification details table
def merge_notification_details(notification_data):
    schema = spark.table(notifications_table_name).schema[:-2]
    df = spark.createDataFrame([Row(*notification_data)], schema=schema)
    df = df.withColumn("created_timestamp", F.current_timestamp()) \
           .withColumn("modified_timestamp", F.current_timestamp())
    
    delta_table = DeltaTable.forName(spark, notifications_table_name)
    delta_table.alias("target").merge(
        df.alias("source"),
        "target.notification_id = source.notification_id AND target.notification_version = source.notification_version"
    ).whenMatchedUpdate(
        set={
            "notification_type": "source.notification_type",
            "notification_priority": "source.notification_priority",
            "notification_template_path": "source.notification_template_path",
            "notification_template_dir": "source.notification_template_dir",
            "notification_template_file": "source.notification_template_file",
            "in_production": "source.in_production",
            "modified_timestamp": F.current_timestamp(),
        }
    ).whenNotMatchedInsert(
        values={
            "notification_id": "source.notification_id",
            "notification_version": "source.notification_version",
            "notification_type": "source.notification_type",
            "notification_priority": "source.notification_priority",
            "control_id": "source.control_id",
            "control_version": "source.control_version",
            "notification_template_path": "source.notification_template_path",
            "notification_template_dir": "source.notification_template_dir",
            "notification_template_file": "source.notification_template_file",
            "in_production": "source.in_production",
            "created_timestamp": F.current_timestamp(),
            "modified_timestamp": F.current_timestamp(),
        }
    ).execute()
    print("Notification details merged (upserted) successfully.")

# Update data in the notification details table
def update_notification_details(notification_id, new_data):
    delta_table = DeltaTable.forName(spark, notifications_table_name)
    update_data = {
        "notification_type": new_data.get("notification_type"),
        "notification_version": new_data.get("notification_version"),
        "notification_priority": new_data.get("notification_priority"),
        "notification_template_path": new_data.get("notification_template_path"),
        "notification_template_dir": new_data.get("notification_template_dir"),
        "notification_template_file": new_data.get("notification_template_file"),
        "in_production": new_data.get("in_production"),
        "modified_timestamp": F.current_timestamp(),
    }
    delta_table.update(
        condition=F.col("notification_id") == notification_id,
        set=update_data
    )
    print(f"Notification {notification_id} updated successfully.")

# Delete data from the notification details table
def delete_notification_details(notification_id):
    delta_table = DeltaTable.forName(spark, notifications_table_name)
    delta_table.delete(F.col("notification_id") == notification_id)
    print(f"Notification {notification_id} deleted successfully.")

# Retrieve production notification by control ID
def get_production_notification_by_control_id(control_id):
    return spark.table(notifications_table_name).filter(
        (F.col("control_id") == control_id) & (F.col("in_production") == True)
    )
