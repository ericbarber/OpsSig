#/backend/delta_tables/migrations/migration_v001.py
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException

from delta import *
from delta.tables import DeltaTable

from config.app import app_details
from config.backend import table_details

from backend.delta_tables.database import create_schema_if_not_exists, get_table_path_if_exists

from backend.delta_tables.departments import create_departments_table
from backend.delta_tables.features import create_features_table
from backend.delta_tables.controls import create_controls_table
from backend.delta_tables.control_runs import create_control_runs_table
from backend.delta_tables.notification_details import create_notification_details_table
from backend.delta_tables.notification_runs import create_notification_runs_table

# Initialize Spark session
import pyspark
print("PySpark version:", pyspark.__version__)

# Create table dict:
create_table_dict = {
    "migration_log_fact": None,
    "departments_fact": create_departments_table,
    "features_fact": create_features_table,
    "controls_fact": create_controls_table,
    "control_runs_fact": create_control_runs_table,
    "notification_details_fact": create_notification_details_table,
    "notification_runs_fact": create_notification_runs_table,
}

schema_set = {
    "app": app_details['app_schema'] 
}

def migration_v001(spark: SparkSession):
    migration_version = 'v0.0.1'
    migration_details = 'Create cdo schema and cdo.migration_log_fact table'

    try:
        schema_name = "cdo"
        spark.sql("SHOW SCHEMAS").show()
        spark.sql("CREATE SCHEMA IF NOT EXISTS cdo")
        spark.sql(f"SHOW TABLES IN {schema_name}").show()
    
    except Exception as err:
        print(err)
    
    migration_log_name = 'cdo.opssig_migration_log_fact'
    migration_log_schema = StructType([
        StructField('version', StringType(), False),
        StructField('details', StringType(), True),
        StructField('updated_timestamp', TimestampType(), True),
    ])
    created_log = DeltaTable.createIfNotExists(spark) \
        .tableName(migration_log_name) \
        .addColumns(migration_log_schema) \
        .execute()
    
    insert_data = [Row(version=migration_version, details=migration_details)]
    insert_df = spark.createDataFrame(insert_data)
    delta_table = DeltaTable.forName(spark, migration_log_name)
    
    delta_table.alias("target").merge(
        insert_df.alias("source"),
        "target.version = source.version") \
        .whenMatchedUpdate(
            set={
                'details': 'source.details',
                'updated_timestamp': F.current_timestamp()
            }
        ) \
        .whenNotMatchedInsert(
            values={
                'version': 'source.version',
                'details': 'source.details',
                'updated_timestamp': F.current_timestamp()
            }
        ).execute()

def migration_v002(spark: SparkSession):
    print('\n####### Delta Tables #######\n')
    #Check if tables exist and print their locations

    for table_id, table in table_details.items():
        print(table_id, table['name'])
        print("\nDelta Tables: checking table existence and paths...\n")
        
        try:
            # Check if the Delta table exists
            table_name = table['name']
            table_path = get_table_path_if_exists(spark, table_name)
            if table_path:
                print(f"\t{table_id} exists as {table_name}")
                continue
            else:
                print(f"\t{table_name} does not exist, atempt to create now")
                # Look up table name in create_table_dict and execute corresponding function
                create_function = create_table_dict.get(table_id)
                if create_function:
                    create_function()  # Execute the table creation function
                    print(f"\t{table_id} table created successfully")
                else:
                    print(f"\t***No creation function found for {table_name}***")

        except AnalysisException as e:
            print(f"\t{table_name} AnalysisException")
            raise e

        except Exception as e:
            print(f"\t{table_name} does not exist or could not be loaded: {e}")

    migration_version = 'v0.0.2'
    migration_details = 'Create create backend tables and functions'
    insert_data = [Row(version=migration_version, details=migration_details)]
    insert_df = spark.createDataFrame(insert_data)
    migration_log_name = 'cdo.opssig_migration_log_fact'
    delta_table = DeltaTable.forName(spark, migration_log_name)
    
    delta_table.alias("target").merge(
        insert_df.alias("source"),
        "target.version = source.version") \
        .whenMatchedUpdate(
            set={
                'details': 'source.details',
                'updated_timestamp': F.current_timestamp()
            }
        ) \
        .whenNotMatchedInsert(
            values={
                'version': 'source.version',
                'details': 'source.details',
                'updated_timestamp': F.current_timestamp()
            }
        ).execute()

    print('Migration 0.0.2 complete')
