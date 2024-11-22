import uuid
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, ArrayType, TimestampType
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from config.backend import table_details

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

control_runs_table_name = str(table_details['control_runs_fact']['name'])

# Define schemas for feature_data and control_signal_data
feature_data_schema = ArrayType(
    StructType([
        StructField("Index", StringType(), True),
        StructField("Feature", DoubleType(), True)
    ])
)

control_signal_data_schema = ArrayType(
    StructType([
        StructField("Index", StringType(), True),
        StructField("Feature", DoubleType(), True),
        StructField("moving_range", DoubleType(), True),
        StructField("UCL", DoubleType(), True),
        StructField("LCL", DoubleType(), True),
        StructField("CL", DoubleType(), True),
        StructField("out_of_control", BooleanType(), True)
    ])
)

# Create the control runs table
def create_control_runs_table():
    control_runs_schema = StructType([
        StructField("department_id", StringType(), False),
        StructField("feature_id", StringType(), False),
        StructField("feature_version", StringType(), True),
        StructField("feature_data", feature_data_schema, True),
        StructField("control_id", StringType(), True),
        StructField("control_version", StringType(), True),
        StructField("control_signal_count", StringType(), True),
        StructField("control_signal_data", control_signal_data_schema, True),
        StructField("signal_detected", BooleanType(), True),
        StructField("control_run_id", StringType(), False),
        StructField("control_run_timestamp", TimestampType(), True),
    ])
    
    DeltaTable.createIfNotExists(spark) \
        .tableName(control_runs_table_name) \
        .addColumns(control_runs_schema) \
        .partitionedBy("department_id", "feature_id") \
        .execute()
    
    print("Control run tracking table created successfully.")

# Insert data into the control run table
def insert_control_run_data(run_data):
    schema = spark.table(control_runs_table_name).schema
    structured_data = [
        Row(*data, str(uuid.uuid4()))  # Add control_run_id
        for data in run_data
    ]
    df = spark.createDataFrame(structured_data, schema=schema)
    df = df.withColumn("control_run_timestamp", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(control_runs_table_name)
    print("Control run data inserted successfully.")

# Merge (upsert) data into the control run table
def merge_control_run_data(run_data):
    schema = spark.table(control_runs_table_name).schema[:-1]
    run_data_with_id = list(run_data) + [str(uuid.uuid4())]
    df = spark.createDataFrame([Row(*run_data_with_id)], schema=schema)
    df = df.withColumn("control_run_timestamp", F.current_timestamp())

    delta_table = DeltaTable.forName(spark, control_runs_table_name)
    delta_table.alias("target").merge(
        df.alias("source"),
        "target.control_run_id = source.control_run_id"
    ).whenMatchedUpdate(
        set={
            "control_signal_data": "source.control_signal_data",
            "signal_detected": "source.signal_detected",
            "control_signal_count": "source.control_signal_count",
            "control_run_timestamp": F.current_timestamp(),
        }
    ).whenNotMatchedInsert(
        values={
            "department_id": "source.department_id",
            "feature_id": "source.feature_id",
            "feature_version": "source.feature_version",
            "feature_data": "source.feature_data",
            "control_id": "source.control_id",
            "control_version": "source.control_version",
            "control_signal_count": "source.control_signal_count",
            "control_signal_data": "source.control_signal_data",
            "signal_detected": "source.signal_detected",
            "control_run_id": "source.control_run_id",
            "control_run_timestamp": F.current_timestamp(),
        }
    ).execute()
    print("Control run data merged (upserted) successfully.")

# Update data in the control run table
def update_control_run_data(control_run_id, new_signal_data, new_signal_detected):
    delta_table = DeltaTable.forName(spark, control_runs_table_name)
    delta_table.update(
        condition=F.col("control_run_id") == control_run_id,
        set={
            "control_signal_data": F.lit(new_signal_data),
            "signal_detected": F.lit(new_signal_detected),
            "control_run_timestamp": F.current_timestamp(),
        }
    )
    print(f"Control run {control_run_id} updated successfully.")

# Delete data from the control run table
def delete_control_run(control_run_id):
    delta_table = DeltaTable.forName(spark, control_runs_table_name)
    delta_table.delete(F.col("control_run_id") == control_run_id)
    print(f"Control run {control_run_id} deleted successfully.")


def delete_control_run_by_id_version(control_id, control_version):
    delta_table = DeltaTable.forName(spark, control_runs_table_name)
    delta_table.delete(
        (F.col("control_id") == control_id) & (F.col("control_version") == control_version)
    )
    print(f"Control {control_id} control_version {control_version} deleted successfully.")
