#/backend/delta_tables/departments.py
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from config.backend import table_details

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

department_table_name = str(table_details['departments_fact']['name'])

# Create the departments table
def create_departments_table():
    department_schema = StructType([
        StructField("department_id", StringType(), False),
        StructField("department_name", StringType(), True),
        StructField("lead_name", StringType(), True),
        StructField("lead_email", StringType(), True),
        StructField("point_of_contact_name", StringType(), True),
        StructField("point_of_contact_email", StringType(), True),
        StructField("created_timestamp", TimestampType(), True),
        StructField("modified_timestamp", TimestampType(), True),
    ])
    
    DeltaTable.createIfNotExists(spark) \
        .tableName(department_table_name) \
        .addColumns(department_schema) \
        .partitionedBy("department_id") \
        .execute()
    
    print("Departments table created successfully.")

# Select details from the departments table
def get_department_by_id(department_id):
    df = spark.table(department_table_name).filter(F.col("department_id") == department_id)
    return df

# Insert data into the departments table
def insert_department_data(department_data):
    insert_df = spark.createDataFrame([Row(*department_data)], schema=spark.table(department_table_name).schema)
    insert_df = insert_df.withColumn("created_timestamp", F.current_timestamp()) \
                         .withColumn("modified_timestamp", F.current_timestamp())
    insert_df.write.format("delta").mode("append").saveAsTable(department_table_name)
    print("Department data inserted successfully.")

# Update data in the departments table
def update_department_data(department_id, new_lead_name, new_lead_email):
    delta_table = DeltaTable.forName(spark, department_table_name)
    delta_table.alias("target").merge(
        spark.createDataFrame([Row(department_id=department_id, lead_name=new_lead_name, lead_email=new_lead_email)], schema=spark.table(department_table_name).schema).alias("source"),
        "target.department_id = source.department_id"
    ).whenMatchedUpdate(
        set={
            "lead_name": "source.lead_name",
            "lead_email": "source.lead_email",
            "modified_timestamp": F.current_timestamp()
        }
    ).execute()
    print(f"Department {department_id} updated successfully.")

# Delete data from the departments table
def delete_department(department_id):
    delta_table = DeltaTable.forName(spark, department_table_name)
    delta_table.delete(F.col("department_id") == department_id)
    print(f"Department {department_id} deleted successfully.")

# Merge (upsert) data into the departments table
def merge_department_data(department_data):
    delta_table = DeltaTable.forName(spark, department_table_name)
    department_df = spark.createDataFrame([Row(*department_data)], schema=spark.table(department_table_name).schema[:-2])
    department_df = department_df.withColumn("modified_timestamp", F.current_timestamp()) \
                                 .withColumn("created_timestamp", F.current_timestamp())

    delta_table.alias("target").merge(
        department_df.alias("source"),
        "target.department_id = source.department_id"
    ).whenMatchedUpdate(
        set={
            "department_name": "source.department_name",
            "lead_name": "source.lead_name",
            "lead_email": "source.lead_email",
            "point_of_contact_name": "source.point_of_contact_name",
            "point_of_contact_email": "source.point_of_contact_email",
            "modified_timestamp": F.current_timestamp()
        }
    ).whenNotMatchedInsert(
        values={
            "department_id": "source.department_id",
            "department_name": "source.department_name",
            "lead_name": "source.lead_name",
            "lead_email": "source.lead_email",
            "point_of_contact_name": "source.point_of_contact_name",
            "point_of_contact_email": "source.point_of_contact_email",
            "created_timestamp": F.current_timestamp(),
            "modified_timestamp": F.current_timestamp()
        }
    ).execute()
    print("Department data merged (upserted) successfully.")
