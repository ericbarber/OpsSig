from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from config.spark_setup import get_spark_session
from signal_watch.control.control_chart import ControlChart
from config.backend_config import table_paths, example_paths
from backend.tables.departments import insert_department_data, get_department_by_id
from backend.tables.features import insert_feature_data, get_feature_by_id

# create a SparkSession
# spark = SparkSession.builder.appName('ControlAnalysisJobExample').getOrCreate()
app_name = 'ControlAnalysisJobExample'
spark = get_spark_session(app_name)

###############################################################################
## EXAMPLE SET UP 
###############################################################################
# Mocked data
# Read the Parquet file into a Spark DataFrame
# df = spark.read.parquet('./scripts/data/parquet/temperature_data_high.parquet')
df = spark.read.parquet('./signal_watch/examples/special_cause_variation_notification/scripts/data/parquet/temperature_data_high.parquet')
df = df.orderBy('Index', ascending=False).limit(180)
# modify limit number for more or less data

# Write the DataFrame to a Delta table
exampel_delta_path = example_paths["high_precission"]
# exampel_delta_path = "/mnt/delta/temperature_data_high"
df.write.format("delta").mode("overwrite").save(exampel_delta_path)

# Department
department_id = 'SigOpsDevTeam'
department_details_insert = [
    department_id,
    "SigOps Development Team",
    "Eric Barber",
    "eric.barber@sigops.com",
    "Eric Barber",
    "eric.barber@sigops.com",
    F.current_timestamp(),
    F.current_timestamp()
]
insert_department_data(department_data)

# Feature
feature_id = 'HighPrecissionTempSystem'

feature_logic = f"""
    SELECT * FROM 'delta.{exampel_delta_path}'
"""

feature_details = [
    department_id,
    feature_id,
    "DailyHighPrecissionTempSystem",
    "1",
    "feature_query_id-12345",
    feature_logic,
    ['eric.barber@sigops.com'],
    F.current_timestamp(),
    F.current_timestamp()
]
insert_feature_data(feature_details)

###############################################################################
## BUSINESS ENTITY

    # department_id STRING,
    # department_name STRING,
    # lead_name STRING,
    # lead_email STRING,
    # point_of_contact_name STRING,
    # point_of_contact_email STRING,
    # created_timestamp TIMESTAMP,
    # modified_timestamp TIMESTAMP
###############################################################################
department_details = get_depertment_by_id(department_id) 
department_details = department_details.collect()

###############################################################################
## REGISTER FEATURE QUERY

    # department_id STRING,
    # feature_id STRING,
    # feature_name STRING,
    # feature_version STRING,
    # feature_query_id STRING,
    # feature_query_notebook STRING,
    # triage_team ARRAY<STRING>,
    # created_timestamp TIMESTAMP,
    # modified_timestamp TIMESTAMP
###############################################################################
feature_details = get_feature_by_id(feature_id)
feature_details = feature_details.collect()

if feature_details:
    feature_logic = feature_details[0]['feature_logic']
else:
    feature_logic = None  # Handle case when no data is returned


###############################################################################
## CONTROL DEFINITION
###############################################################################

