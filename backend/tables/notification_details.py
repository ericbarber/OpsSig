from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from config.backend_config import table_paths
from config.spark_setup import get_spark_session

# Initialize Spark session
spark = get_spark_session()

# Create the notification details table
def create_notification_details_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{table_paths['notification_details']}`
        (
            notification_id STRING,
            notification_version STRING,
            notification_type STRING,
            notification_priority STRING,
            control_id STRING,
            control_version STRING,
            notification_template_path STRING,
            notification_template_dir STRING,
            notification_template_file STRING,
            in_production BOOLEAN,
            created_timestamp TIMESTAMP,
            modified_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (notification_id);
    """)
    print("Notification details table created successfully.")

# Insert data into the notification details table
def insert_notification_details(notification_data):
    # Retrieve the schema from the Delta table
    table_schema = spark.read.format("delta").load(table_paths['notification_details']).schema

    # Convert list of data to DataFrame
    notification_row = Row(*table_schema.fieldNames())(*notification_data)
    df = spark.createDataFrame([notification_row], schema=table_schema)

    # Insert into notification details table
    df.withColumn("created_timestamp", F.to_timestamp("created_timestamp")) \
      .withColumn("modified_timestamp", F.to_timestamp("modified_timestamp")) \
      .write.format("delta") \
      .mode("append") \
      .save(table_paths['notification_details'])

    print("Notification details inserted successfully.")

# Update data in the notification details table
def update_notification_details(notification_id, new_template_path, new_details):
    spark.sql(f"""
        MERGE INTO delta.`{table_paths['notification_details']}` AS target
        USING (SELECT '{notification_id}' AS notification_id, '{new_template_path}' AS notification_template_path, '{new_details}' AS notification_details) AS source
        ON target.notification_id = source.notification_id
        WHEN MATCHED THEN
          UPDATE SET target.notification_template_path = source.notification_template_path,
                     target.notification_details = source.notification_details,
                     target.modified_timestamp = current_timestamp();
    """)
    
    print(f"Notification {notification_id} updated successfully.")

# Delete data from the notification details table
def delete_notification_details(notification_id):
    spark.sql(f"""
        DELETE FROM delta.`{table_paths['notification_details']}`
        WHERE notification_id = '{notification_id}'
    """)
    
    print(f"Notification {notification_id} deleted successfully.")

# Merge (upsert) data into the notification details table
def merge_notification_details(notification_data):
    table_schema = spark.read.format("delta").load(table_paths['notification_details']).schema
    table_schema = table_schema[:-2]

    notification_row = Row(
        notification_id=notification_data[0],
        notification_version=notification_data[1],
        notification_type=notification_data[2],
        notification_priority=notification_data[3],
        control_id=notification_data[4],
        control_version=notification_data[5],
        notification_template_path=notification_data[6],
        notification_template_dir=notification_data[7],
        notification_template_file=notification_data[8],
        in_production=notification_data[9],
    )

    df = spark.createDataFrame([notification_row], schema=table_schema)
    df.createOrReplaceTempView("source_table")

    spark.sql(f"""
        MERGE INTO delta.`{table_paths['notification_details']}` AS target
        USING source_table AS source
        ON target.notification_id = source.notification_id AND target.notification_version = source.notification_version
        WHEN MATCHED THEN
          UPDATE SET 
             target.notification_template_path = source.notification_template_path,
             target.notification_template_dir = source.notification_template_dir,
             target.notification_template_file = source.notification_template_file,
             target.notification_type = source.notification_type,
             target.notification_priority = source.notification_priority,
             target.modified_timestamp = current_timestamp()
        WHEN NOT MATCHED THEN
            INSERT (
                notification_id,
                notification_version,
                notification_type,
                notification_priority,
                control_id,
                control_version,
                notification_template_path,
                notification_template_dir,
                notification_template_file,
                in_production,
                created_timestamp,
                modified_timestamp
            )
              VALUES (
                source.notification_id,
                source.notification_version,
                source.notification_type,
                source.notification_priority,
                source.control_id,
                source.control_version,
                source.notification_template_path,
                source.notification_template_dir,
                source.notification_template_file,
                source.in_production,
                current_timestamp(),
                current_timestamp()
            );
    """)
    
    print("Notification details merged (upserted) successfully.")

def get_production_notification_by_control_id(control_id):
    return spark.sql(f"""
        SELECT * FROM delta.`{table_paths['notification_details']}`
        WHERE control_id = '{control_id}' 
        AND in_production = true
    """)
