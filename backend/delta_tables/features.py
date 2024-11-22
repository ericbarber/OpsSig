from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, TimestampType
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from config.backend import table_details

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

features_table_name = str(table_details['features_fact']['name'])

# Create the features table with the production_status column
def create_features_table():
    features_schema = StructType([
        StructField("department_id", StringType(), False),
        StructField("feature_id", StringType(), False),
        StructField("feature_name", StringType(), True),
        StructField("feature_version", StringType(), True),
        StructField("feature_query_id", StringType(), True),
        StructField("feature_logic", StringType(), True),
        StructField("triage_team", ArrayType(StringType()), True),
        StructField("production_status", BooleanType(), True),
        StructField("created_timestamp", TimestampType(), True),
        StructField("modified_timestamp", TimestampType(), True),
    ])
    
    DeltaTable.createIfNotExists(spark) \
        .tableName(features_table_name) \
        .addColumns(features_schema) \
        .partitionedBy("department_id") \
        .execute()
    
    print("Features table created successfully with production_status column.")

# Insert data into the features table
def insert_feature_data(feature_data):
    schema = spark.table(features_table_name).schema
    df = spark.createDataFrame([Row(*feature_data)], schema=schema)
    df = df.withColumn("created_timestamp", F.current_timestamp()) \
           .withColumn("modified_timestamp", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(features_table_name)
    print("Feature data inserted successfully.")

# Merge (upsert) data into the features table
def merge_feature_data(feature_data):
    delta_table = DeltaTable.forName(spark, features_table_name)
    schema = spark.table(features_table_name).schema
    df = spark.createDataFrame([Row(*feature_data)], schema=schema[:-2])
    df = df.withColumn("modified_timestamp", F.current_timestamp()) \
           .withColumn("created_timestamp", F.current_timestamp())

    delta_table.alias("target").merge(
        df.alias("source"),
        "target.feature_id = source.feature_id AND target.feature_version = source.feature_version"
    ).whenMatchedUpdate(
        set={
            "feature_name": "source.feature_name",
            "feature_query_id": "source.feature_query_id",
            "feature_logic": "source.feature_logic",
            "triage_team": "source.triage_team",
            "production_status": "source.production_status",
            "modified_timestamp": F.current_timestamp(),
        }
    ).whenNotMatchedInsert(
        values={
            "department_id": "source.department_id",
            "feature_id": "source.feature_id",
            "feature_name": "source.feature_name",
            "feature_version": "source.feature_version",
            "feature_query_id": "source.feature_query_id",
            "feature_logic": "source.feature_logic",
            "triage_team": "source.triage_team",
            "production_status": "source.production_status",
            "created_timestamp": F.current_timestamp(),
            "modified_timestamp": F.current_timestamp(),
        }
    ).execute()
    print("Feature data merged (upserted) successfully.")

# Update a feature's name and version
def update_feature_data(feature_id, new_feature_name, new_feature_version):
    delta_table = DeltaTable.forName(spark, features_table_name)
    update_df = spark.createDataFrame(
        [Row(feature_id=feature_id, feature_name=new_feature_name, feature_version=new_feature_version)],
        schema=spark.table(features_table_name).schema
    )
    delta_table.alias("target").merge(
        update_df.alias("source"),
        "target.feature_id = source.feature_id"
    ).whenMatchedUpdate(
        set={
            "feature_name": "source.feature_name",
            "feature_version": "source.feature_version",
            "modified_timestamp": F.current_timestamp(),
        }
    ).execute()
    print(f"Feature {feature_id} updated successfully.")

# Update production status for a feature version
def update_production_status(feature_id, feature_version):
    delta_table = DeltaTable.forName(spark, features_table_name)
    delta_table.update(
        condition=F.col("feature_id") == feature_id,
        set={"production_status": F.lit(False)}
    )
    delta_table.update(
        condition=(F.col("feature_id") == feature_id) & (F.col("feature_version") == feature_version),
        set={"production_status": F.lit(True)}
    )
    print(f"Production status updated for feature {feature_id}, version {feature_version}.")

# Delete a feature
def delete_feature(feature_id):
    delta_table = DeltaTable.forName(spark, features_table_name)
    delta_table.delete(F.col("feature_id") == feature_id)
    print(f"Feature {feature_id} deleted successfully.")

# Retrieve a feature by ID and version
def get_feature_by_id_and_version(feature_id, feature_version):
    return spark.table(features_table_name).filter(
        (F.col("feature_id") == feature_id) & (F.col("feature_version") == feature_version)
    )

# Retrieve the production version of a feature
def get_production_feature(feature_id):
    return spark.table(features_table_name).filter(
        (F.col("feature_id") == feature_id) & (F.col("production_status") == True)
    )
