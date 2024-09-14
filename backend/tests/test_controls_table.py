import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch
from backend.tables.controls import create_controls_table, insert_control_data, update_control_data, delete_control, merge_control_data

# Helper function to normalize SQL by removing newlines and extra spaces
def normalize_sql(sql):
    return " ".join(sql.replace('\n', '').split())

# Fixture to initialize and teardown a Spark session
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.appName("pytest-spark").master("local").getOrCreate()
    yield spark
    spark.stop()

# Test creating the controls table
@patch('backend.tables.controls.table_paths', {'controls': '/mnt/delta/controls'})
def test_create_controls_table(spark):
    with patch.object(spark, 'sql') as mock_sql:
        create_controls_table()
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        expected_sql = normalize_sql("""
            CREATE TABLE IF NOT EXISTS delta.`/mnt/delta/controls`
            (
                department_id STRING,
                feature_id STRING,
                control_id STRING,
                control_group STRING,
                control_version STRING,
                control_notebook_id STRING,
                control_notebook_url STRING,
                created_timestamp TIMESTAMP,
                modified_timestamp TIMESTAMP
            )
            USING DELTA
            PARTITIONED BY (department_id, control_group);
        """)
        
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test inserting control data
@patch('backend.tables.controls.table_paths', {'controls': '/mnt/delta/controls'})
def test_insert_control_data(spark):
    control_data = [
        ("Dept_001", "Feature_001", "Group_001", "Control_001", "v1.0", "notebook_001", "url_001", "2024-09-10 10:00:00", "2024-09-10 10:00:00")
    ]
    
    with patch.object(spark, 'createDataFrame') as mock_create_df:
        insert_control_data(control_data)
        expected_columns = ["department_id", "feature_id", "control_id", "control_group", "control_version", "control_notebook_id", "control_notebook_url", "created_timestamp", "modified_timestamp"]        
        mock_create_df.assert_called_once_with(control_data, expected_columns)

# Test updating control data
@patch('backend.tables.controls.table_paths', {'controls': '/mnt/delta/controls'})
def test_update_control_data(spark):
    control_id = "Control_001"
    control_version = "v1.0"
    new_control_notebook_url = "updated_url_001"
    
    with patch.object(spark, 'sql') as mock_sql:
        update_control_data(control_id, control_version, new_control_notebook_url)
        
        expected_sql = normalize_sql(f"""
            MERGE INTO delta.`/mnt/delta/controls` AS target
            USING (
                SELECT
                    '{control_id}' AS control_id,
                    '{control_version}' AS control_version,
                    '{new_control_notebook_url}' AS control_notebook_url
                ) AS source
            ON target.control_id = source.control_id AND target.control_version = source.control_version
            WHEN MATCHED THEN
              UPDATE SET target.control_notebook_url = source.control_notebook_url, target.modified_timestamp = current_timestamp();
        """)
        
        actual_sql = normalize_sql(mock_sql.call_args[0][0])
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test deleting control data
@patch('backend.tables.controls.table_paths', {'controls': '/mnt/delta/controls'})
def test_delete_control(spark):
    control_id = "Control_001"
    control_version = "v1.0"
    
    with patch.object(spark, 'sql') as mock_sql:
        delete_control(control_id, control_version)
        
        expected_sql = normalize_sql(f"""
            DELETE FROM delta.`/mnt/delta/controls`
            WHERE control_id = '{control_id}' AND control_version = '{control_version}'
        """)
        
        actual_sql = normalize_sql(mock_sql.call_args[0][0])
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test merging control data
@patch('backend.tables.controls.table_paths', {'controls': '/mnt/delta/controls'})
def test_merge_control_data(spark):
    control_data = [
        ("Dept_002", "Feature_002", "Control_002", "Group_002", "v2.0", "notebook_002", "url_002", "2024-09-11 12:00:00", "2024-09-11 12:00:00", "2024-09-11 12:00:00")
    ]
    
    with patch.object(spark, 'createDataFrame') as mock_create_df, patch.object(spark, 'sql') as mock_sql:
        merge_control_data(control_data)
        
        expected_columns = ["department_id", "feature_id", "control_id", "control_group", "control_version", "control_notebook_id", "control_notebook_url", "created_timestamp", "modified_timestamp"]
        mock_create_df.assert_called_once_with(control_data, expected_columns)
        
        expected_sql = normalize_sql("""
            MERGE INTO delta.`/mnt/delta/controls` AS target
            USING source_table AS source
            ON target.control_id = source.control_id AND target.control_version = source.control_version
            WHEN MATCHED THEN
              UPDATE SET target.control_notebook_url = source.control_notebook_url,
                         target.modified_timestamp = current_timestamp()
            WHEN NOT MATCHED THEN
              INSERT (department_id, feature_id, control_id, control_group, control_version, control_notebook_id, control_notebook_url, created_timestamp, modified_timestamp)
              VALUES (source.department_id, source.feature_id, source.control_id, source.control_group, source.control_version, source.control_notebook_id, source.control_notebook_url, current_timestamp(), current_timestamp());
        """)
        
        actual_sql = normalize_sql(mock_sql.call_args[0][0])
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"
