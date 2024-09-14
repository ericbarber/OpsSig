import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch
from backend.tables.features import create_features_table, insert_feature_data, update_feature_data, delete_feature, merge_feature_data

# Helper function to normalize SQL by removing newlines and extra spaces
def normalize_sql(sql):
    return " ".join(sql.replace('\n', '').split())

# Fixture to initialize and teardown a Spark session
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.appName("pytest-spark").master("local").getOrCreate()
    yield spark
    spark.stop()

# Test creating the features table
@patch('backend.tables.features.table_paths', {'features': '/mnt/delta/features'})
def test_create_features_table(spark):
    with patch.object(spark, 'sql') as mock_sql:
        create_features_table()
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        expected_sql = normalize_sql("""
            CREATE TABLE IF NOT EXISTS delta.`/mnt/delta/features`
            (
                department_id STRING,
                feature_id STRING,
                feature_name STRING,
                feature_version STRING,
                feature_query_id STRING,
                feature_query_notebook STRING,
                triage_team ARRAY<STRING>,
                created_timestamp TIMESTAMP,
                modified_timestamp TIMESTAMP
            )
            USING DELTA
            PARTITIONED BY (department_id);
        """)
        
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test inserting feature data
@patch('backend.tables.features.table_paths', {'features': '/mnt/delta/features'})
def test_insert_feature_data(spark):
    feature_data = [
        ("Dept_001", "Feature_001", "Feature A", "v1.0", "query_001", "notebook_001", ["alice@example.com", "bob@example.com"], "2024-09-10 10:00:00", "2024-09-10 10:00:00")
    ]
    
    with patch.object(spark, 'createDataFrame') as mock_create_df:
        insert_feature_data(feature_data)
        expected_columns = ["department_id", "feature_id", "feature_name", "feature_version", "feature_query_id", "feature_query_notebook", "triage_team", "created_timestamp", "modified_timestamp"]
        
        mock_create_df.assert_called_once_with(feature_data, expected_columns)

# Test updating feature data
@patch('backend.tables.features.table_paths', {'features': '/mnt/delta/features'})
def test_update_feature_data(spark):
    feature_id = "Feature_001"
    new_feature_name = "Feature A Updated"
    new_feature_version = "v1.1"
    
    with patch.object(spark, 'sql') as mock_sql:
        update_feature_data(feature_id, new_feature_name, new_feature_version)
        
        expected_sql = normalize_sql(f"""
            MERGE INTO delta.`/mnt/delta/features` AS target
            USING (SELECT '{feature_id}' AS feature_id, '{new_feature_name}' AS feature_name, '{new_feature_version}' AS feature_version) AS source
            ON target.feature_id = source.feature_id
            WHEN MATCHED THEN
              UPDATE SET target.feature_name = source.feature_name, target.feature_version = source.feature_version, target.modified_timestamp = current_timestamp();
        """)
        
        actual_sql = normalize_sql(mock_sql.call_args[0][0])
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test deleting feature data
@patch('backend.tables.features.table_paths', {'features': '/mnt/delta/features'})
def test_delete_feature(spark):
    feature_id = "Feature_001"
    
    with patch.object(spark, 'sql') as mock_sql:
        delete_feature(feature_id)
        
        expected_sql = normalize_sql(f"""
            DELETE FROM delta.`/mnt/delta/features`
            WHERE feature_id = '{feature_id}'
        """)
        
        actual_sql = normalize_sql(mock_sql.call_args[0][0])
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test merging feature data
@patch('backend.tables.features.table_paths', {'features': '/mnt/delta/features'})
def test_merge_feature_data(spark):
    feature_data = [
        ("Dept_002", "Feature_002", "Feature B", "v2.0", "query_002", "notebook_002", ["eve@example.com"], "2024-09-11 12:00:00", "2024-09-11 12:00:00")
    ]
    
    with patch.object(spark, 'createDataFrame') as mock_create_df, patch.object(spark, 'sql') as mock_sql:
        merge_feature_data(feature_data)
        
        expected_columns = ["department_id", "feature_id", "feature_name", "feature_version", "feature_query_id", "feature_query_notebook", "triage_team", "created_timestamp", "modified_timestamp"]
        mock_create_df.assert_called_once_with(feature_data, expected_columns)
        
        expected_sql = normalize_sql("""
            MERGE INTO delta.`/mnt/delta/features` AS target
            USING source_table AS source
            ON target.feature_id = source.feature_id
            WHEN MATCHED THEN
              UPDATE SET target.feature_name = source.feature_name,
                         target.feature_version = source.feature_version,
                         target.feature_query_id = source.feature_query_id,
                         target.feature_query_notebook = source.feature_query_notebook,
                         target.triage_team = source.triage_team,
                         target.modified_timestamp = current_timestamp()
            WHEN NOT MATCHED THEN
              INSERT (department_id, feature_id, feature_name, feature_version, feature_query_id, feature_query_notebook, triage_team, created_timestamp, modified_timestamp)
              VALUES (source.department_id, source.feature_id, source.feature_name, source.feature_version, source.feature_query_id, source.feature_query_notebook, source.triage_team, current_timestamp(), current_timestamp());
        """)
        
        actual_sql = normalize_sql(mock_sql.call_args[0][0])
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"
