from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.backend_config import table_paths
from config.spark_setup import get_spark_session

# Initialize Spark session
spark = get_spark_session()

# Create the departments table
def create_departments_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{table_paths['departments']}`
        (
            department_id STRING,
            department_name STRING,
            lead_name STRING,
            lead_email STRING,
            point_of_contact_name STRING,
            point_of_contact_email STRING,
            created_timestamp TIMESTAMP,
            modified_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (department_id);
    """)
    print("Departments table created successfully.")

# Select details departments table
def get_department_by_id(department_id):
    return spark.sql(f"""
        SELECT * FROM `{table_paths['departments']}`
        WHERE department_id = '{department_id}'
    """)

# Insert data into the departments table
def insert_department_data(department_data):
    # Retrieve the schema from the Delta table
    table_schema = spark.read.format("delta").load(table_paths['departments']).schema
    
    # Convert list of data to DataFrame
    df = spark.createDataFrame(department_data, schema=table_schema)

    # Insert into departments table
    df.withColumn("created_timestamp", F.to_timestamp("created_timestamp")) \
      .withColumn("modified_timestamp", F.to_timestamp("modified_timestamp")) \
      .write.format("delta") \
      .mode("append") \
      .save(table_paths['departments'])

    print("Department data inserted successfully.")

# Update data in the departments table
def update_department_data(department_id, new_lead_name, new_lead_email):
    # Define the update SQL
    spark.sql(f"""
        MERGE INTO delta.`{table_paths['departments']}` AS target
        USING (SELECT '{department_id}' AS department_id, '{new_lead_name}' AS lead_name, '{new_lead_email}' AS lead_email) AS source
        ON target.department_id = source.department_id
        WHEN MATCHED THEN
          UPDATE SET target.lead_name = source.lead_name, target.lead_email = source.lead_email, target.modified_timestamp = current_timestamp();
    """)
    
    print(f"Department {department_id} updated successfully.")

# Delete data from the departments table
def delete_department(department_id):
    # Delete from departments table
    spark.sql(f"""
        DELETE FROM delta.`{table_paths['departments']}`
        WHERE department_id = '{department_id}'
    """)
    
    print(f"Department {department_id} deleted successfully.")

# Merge (upsert) data into the departments table
def merge_department_data(department_data):
    df = spark.createDataFrame(department_data, ["department_id", "department_name", "lead_name", "lead_email", "point_of_contact_name", "point_of_contact_email", "created_timestamp", "modified_timestamp"])
    
    # Merge new data into the table
    df.createOrReplaceTempView("source_table")

    spark.sql(f"""
        MERGE INTO delta.`{table_paths['departments']}` AS target
        USING source_table AS source
        ON target.department_id = source.department_id
        WHEN MATCHED THEN
          UPDATE SET target.department_name = source.department_name,
                     target.lead_name = source.lead_name,
                     target.lead_email = source.lead_email,
                     target.point_of_contact_name = source.point_of_contact_name,
                     target.point_of_contact_email = source.point_of_contact_email,
                     target.modified_timestamp = current_timestamp()
        WHEN NOT MATCHED THEN
          INSERT (department_id, department_name, lead_name, lead_email, point_of_contact_name, point_of_contact_email, created_timestamp, modified_timestamp)
          VALUES (source.department_id, source.department_name, source.lead_name, source.lead_email, source.point_of_contact_name, source.point_of_contact_email, current_timestamp(), current_timestamp());
    """)
    
    print("Department data merged (upserted) successfully.")

# Example usage
if __name__ == "__main__":
    # 1. Create the departments table
    create_departments_table()

    # 2. Insert sample department data
    data = [
        ("Dept_001", "Engineering", "Alice", "alice@example.com", "Bob", "bob@example.com", "2024-09-10 10:00:00", "2024-09-10 10:00:00"),
        ("Dept_002", "Sales", "Carol", "carol@example.com", "Dave", "dave@example.com", "2024-09-10 11:00:00", "2024-09-10 11:00:00")
    ]
    insert_department_data(data)

    # 3. Update department data
    update_department_data("Dept_001", "Alice Johnson", "alice.johnson@example.com")

    # 4. Merge (upsert) department data
    merge_data = [
        ("Dept_003", "Marketing", "Eve", "eve@example.com", "Frank", "frank@example.com", "2024-09-11 12:00:00", "2024-09-11 12:00:00")
    ]
    merge_department_data(merge_data)

    # 5. Delete department
    delete_department("Dept_002")

