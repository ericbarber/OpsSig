import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch, MagicMock

from backend.tables.departments import create_departments_table, insert_department_data, update_department_data, delete_department, merge_department_data, get_department_by_id


def normalize_sql(sql):
    """Helper function to normalize SQL by removing newlines and extra spaces."""
    return " ".join(sql.replace('\n', '').split())

# Fixture to initialize and teardown a Spark session
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.appName("pytest-spark-depertments").master("local").getOrCreate()
    yield spark
    spark.stop()

# Mocking the table paths (assuming it's in the config)
@patch('backend.tables.departments.table_paths', {'departments': '/mnt/delta/departments'})
def test_create_departments_table(spark):
    with patch.object(spark, 'sql') as mock_sql:
        create_departments_table()
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        # Verify if the correct SQL statement was executed
        expected_sql = normalize_sql("""
            CREATE TABLE IF NOT EXISTS delta.`/mnt/delta/departments`
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
            """
        )
        # mock_sql.assert_called_once_with(expected_sql)
        assert expected_sql == actual_sql, "Expected: {}, but got: {}".format( expected_sql, actual_sql)

@patch('backend.tables.departments.table_paths', {'departments': '/mnt/delta/departments'})
def test_update_department_data(spark):
    department_id = "Dept_001"
    new_lead_name = "Alice Johnson"
    new_lead_email = "alice.johnson@example.com"
    
    with patch.object(spark, 'sql') as mock_sql:
        update_department_data(department_id, new_lead_name, new_lead_email)
        
        # Normalize the SQL strings
        expected_sql = normalize_sql(f"""
            MERGE INTO delta.`/mnt/delta/departments` AS target
            USING (SELECT '{department_id}' AS department_id, '{new_lead_name}' AS lead_name, '{new_lead_email}' AS lead_email) AS source
            ON target.department_id = source.department_id
            WHEN MATCHED THEN
              UPDATE SET target.lead_name = source.lead_name, target.lead_email = source.lead_email, target.modified_timestamp = current_timestamp();
        """)
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        # Assert the normalized SQL
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"


@patch('backend.tables.departments.table_paths', {'departments': '/mnt/delta/departments'})
def test_delete_department(spark):
    department_id = "Dept_001"
    
    with patch.object(spark, 'sql') as mock_sql:
        delete_department(department_id)
        
        # Normalize the SQL strings
        expected_sql = normalize_sql(f"""
            DELETE FROM delta.`/mnt/delta/departments`
            WHERE department_id = '{department_id}'
        """)
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        # Assert the normalized SQL
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"


@patch('backend.tables.departments.table_paths', {'departments': '/mnt/delta/departments'})
def test_merge_department_data(spark):
    department_data = [
        ("Dept_003", "Marketing", "Eve", "eve@example.com", "Frank", "frank@example.com", "2024-09-11 12:00:00", "2024-09-11 12:00:00")
    ]
    
    with patch.object(spark, 'createDataFrame') as mock_create_df, patch.object(spark, 'sql') as mock_sql:
        merge_department_data(department_data)
        
        # Check if createDataFrame was called with the correct schema
        expected_columns = ["department_id", "department_name", "lead_name", "lead_email", "point_of_contact_name", "point_of_contact_email", "created_timestamp", "modified_timestamp"]
        mock_create_df.assert_called_once_with(department_data, expected_columns)
        
        # Normalize the SQL strings
        expected_sql = normalize_sql("""
            MERGE INTO delta.`/mnt/delta/departments` AS target
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
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        # Assert the normalized SQL
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test function to get department by ID
@patch('backend.tables.departments.table_paths', {'departments': '/mnt/delta/departments'})
def test_get_department_by_id(spark):
    department_id = "Dept_001"
    
    # Mock the spark.sql call
    with patch.object(spark, 'sql') as mock_sql:
        # Call the function under test
        get_department_by_id(department_id)

        # Expected SQL query
        expected_sql = normalize_sql(f"""
            SELECT * FROM `/mnt/delta/departments`
            WHERE department_id = '{department_id}'
        """)

        # Get the actual SQL from the mock call
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        # Assert that the SQL matches the expected query
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

        # Optionally, mock the return value and verify the result
        mock_sql.return_value = "mocked result"
        result = get_department_by_id(department_id)
        assert result == "mocked result"
