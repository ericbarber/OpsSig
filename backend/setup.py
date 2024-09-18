# from pyspark.sql import SparkSession
# from config import table_paths
#
# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("DeltaTableCreation") \
#     .getOrCreate()
#
# def create_tables():
#
#     # Create departments table
#     spark.sql(f"""
#         CREATE TABLE IF NOT EXISTS delta.`{table_paths['departments']}`
#         (
#             department_id STRING,
#             department_name STRING,
#             lead_name STRING,
#             lead_email STRING,
#             point_of_contact_name STRING,
#             point_of_contact_email STRING,
#             created_timestamp TIMESTAMP,
#             modified_timestamp TIMESTAMP
#         )
#         USING DELTA
#         PARTITIONED BY (department_id);
#     """)
#
#     # Create features table
#     spark.sql(f"""
#         CREATE TABLE IF NOT EXISTS delta.`{table_paths['features']}`
#         (
#             department_id STRING,
#             feature_id STRING,
#             feature_name STRING,
#             feature_version STRING,
#             feature_query_id STRING,
#             feature_query_notebook STRING,
#             triage_team ARRAY<STRING>,
#             created_timestamp TIMESTAMP,
#             modified_timestamp TIMESTAMP
#         )
#         USING DELTA
#         PARTITIONED BY (department_id);
#     """)
#
#     # Create controls table
#     spark.sql(f"""
#         CREATE TABLE IF NOT EXISTS delta.`{table_paths['controls']}`
#         (
#             department_id STRING,
#             feature_id STRING,
#             control_group STRING,
#             control_id STRING,
#             control_version STRING,
#             control_notebook_id STRING,
#             control_notebook_url STRING,
#             analysis_date TIMESTAMP,
#             created_timestamp TIMESTAMP,
#             modified_timestamp TIMESTAMP
#         )
#         USING DELTA
#         PARTITIONED BY (department_id, control_id, version);
#     """)
#     
#     # TODO: Create control_runs table
#     # I have the following script, that creates a delta table and provide several functions for interacting with that table. I need you to do the same thing but for the table defined here:
#     spark.sql(f"""
#         CREATE TABLE IF NOT EXISTS delta.`{table_paths['control_runs']}`
#         (
#
#             department_id STRING,
#             control_id STRING,
#             control_version STRING,
#             created_timestamp TIMESTAMP,
#             modified_timestamp TIMESTAMP
#         )
#         USING DELTA
#         PARTITIONED BY (department_id, control_id);
#     """)
#
#     # # TODO: Create signal_detection table
#     # spark.sql(f"""
#     #     CREATE TABLE IF NOT EXISTS delta.`{table_paths['signal_detection']}`
#     #     (
#     #         signal_id STRING,
#     #         control_analysis_id STRING,
#     #         department_id STRING,
#     #         signal_type STRING,
#     #         signal_date TIMESTAMP,
#     #         severity STRING
#     #     )
#     #     USING DELTA
#     #     PARTITIONED BY (department_id, feature_id);
#     # """)
#     #
#     # # TODO: Create notifications table
#     # spark.sql(f"""
#     #     CREATE TABLE IF NOT EXISTS delta.`{table_paths['notifications']}`
#     #     (
#     #         department_id STRING,
#     #         feature_id STRING,
#     #         timestamp TIMESTAMP
#     #         created_timestamp TIMESTAMP,
#     #         modified_timestamp TIMESTAMP
#     #     )
#     #     USING DELTA
#     #     PARTITIONED BY (department_id, feature_id, control_id);
#     # """)
#     
#     # TODO: Create other tables similarly
#     print("Tables created successfully.")
#
# if __name__ == "__main__":
#     create_tables()
#
