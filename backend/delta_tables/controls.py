from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from config.backend import table_details

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

controls_table_name = str(table_details['controls_fact']['name'])

# Create the controls table
def create_controls_table():
    controls_schema = StructType([
        StructField("department_id", StringType(), False),
        StructField("feature_id", StringType(), False),
        StructField("control_id", StringType(), False),
        StructField("control_group", StringType(), True),
        StructField("control_version", StringType(), True),
        StructField("control_notebook_id", StringType(), True),
        StructField("control_dashbaord_url", StringType(), True),
        StructField("created_timestamp", TimestampType(), True),
        StructField("modified_timestamp", TimestampType(), True),
    ])
    
    DeltaTable.createIfNotExists(spark) \
        .tableName(controls_table_name) \
        .addColumns(controls_schema) \
        .partitionedBy("department_id", "control_group") \
        .execute()
    
    print("Controls table created successfully.")

# Insert data into the controls table
def insert_control_data(control_data):
    schema = spark.table(controls_table_name).schema
    df = spark.createDataFrame(control_data, schema=schema)
    df = df.withColumn("created_timestamp", F.current_timestamp()) \
           .withColumn("modified_timestamp", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(controls_table_name)
    print("Control data inserted successfully.")

# Merge (upsert) data into the controls table
def merge_control_data(control_data):
    schema = spark.table(controls_table_name).schema
    df = spark.createDataFrame([Row(*control_data)], schema=schema)
    df = df.withColumn("created_timestamp", F.current_timestamp()) \
           .withColumn("modified_timestamp", F.current_timestamp())
    
    delta_table = DeltaTable.forName(spark, controls_table_name)
    delta_table.alias("target").merge(
        df.alias("source"),
        "target.control_id = source.control_id AND target.control_version = source.control_version"
    ).whenMatchedUpdate(
        set={
            "control_dashbaord_url": "source.control_dashbaord_url",
            "modified_timestamp": F.current_timestamp()
        }
    ).whenNotMatchedInsert(
        values={
            "department_id": "source.department_id",
            "feature_id": "source.feature_id",
            "control_id": "source.control_id",
            "control_group": "source.control_group",
            "control_version": "source.control_version",
            "control_notebook_id": "source.control_notebook_id",
            "control_dashbaord_url": "source.control_dashbaord_url",
            "created_timestamp": F.current_timestamp(),
            "modified_timestamp": F.current_timestamp()
        }
    ).execute()
    print("Control data merged (upserted) successfully.")

# Update data in the controls table
def update_control_data(control_id, control_version, control_dashbaord_url):
    delta_table = DeltaTable.forName(spark, controls_table_name)
    delta_table.update(
        condition=(F.col("control_id") == control_id) & (F.col("control_version") == control_version),
        set={
            "control_dashbaord_url": F.lit(control_dashbaord_url),
            "modified_timestamp": F.current_timestamp()
        }
    )
    print(f"Control {control_id} control_version {control_version} updated successfully.")

# Delete data from the controls table
def delete_control(control_id, control_version):
    delta_table = DeltaTable.forName(spark, controls_table_name)
    delta_table.delete(
        (F.col("control_id") == control_id) & (F.col("control_version") == control_version)
    )
    print(f"Control {control_id} control_version {control_version} deleted successfully.")
