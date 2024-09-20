import pytest
from unittest.mock import patch

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType
from config.backend_config import table_paths

from backend.tables.features import create_features_table, insert_feature_data, update_feature_data, delete_feature, merge_feature_data, get_feature_by_id

from backend.tables.departments import create_departments_table, insert_department_data, update_department_data, delete_department, merge_department_data, get_department_by_id

from backend.tables.controls import create_controls_table, insert_control_data, update_control_data, delete_control, merge_control_data


from backend.tables.control_runs import create_control_run_table, insert_control_run_data, update_control_run_data, delete_control_run, merge_control_run_data

def normalize_sql(sql):
    """Helper function to normalize SQL by removing newlines and extra spaces."""
    return " ".join(sql.replace('\n', '').split())

# Fixture to initialize and teardown a Spark session
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.appName("pytest-spark").master("local").getOrCreate()
    yield spark
    spark.stop()

# Test creating the controls table
@patch('backend.tables.controls.table_paths', {'controls': table_paths['controls']})
def test_create_controls_table(spark):
    with patch.object(spark, 'sql') as mock_sql:
        create_controls_table()
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        expected_sql = normalize_sql(f"""
            CREATE TABLE IF NOT EXISTS delta.`{table_paths['controls']}`
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
@patch('backend.tables.controls.table_paths', {'controls': table_paths['controls']})
def test_insert_control_data(spark):
    control_data = [
        ("Dept_001", "Feature_001", "Group_001", "Control_001", "v1.0", "notebook_001", "url_001", "2024-09-10 10:00:00", "2024-09-10 10:00:00")
    ]
    
    with patch.object(spark, 'createDataFrame') as mock_create_df:
        insert_control_data(control_data)
        expected_columns = ["department_id", "feature_id", "control_id", "control_group", "control_version", "control_notebook_id", "control_notebook_url", "created_timestamp", "modified_timestamp"]        
        mock_create_df.assert_called_once_with(control_data, expected_columns)

# Test updating control data
@patch('backend.tables.controls.table_paths', {'controls': table_paths['controls']})
def test_update_control_data(spark):
    control_id = "Control_001"
    control_version = "v1.0"
    new_control_notebook_url = "updated_url_001"
    
    with patch.object(spark, 'sql') as mock_sql:
        update_control_data(control_id, control_version, new_control_notebook_url)
        
        expected_sql = normalize_sql(f"""
            MERGE INTO delta.`{table_paths['controls']}` AS target
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
@patch('backend.tables.controls.table_paths', {'controls': table_paths['controls']})
def test_delete_control(spark):
    control_id = "Control_001"
    control_version = "v1.0"
    
    with patch.object(spark, 'sql') as mock_sql:
        delete_control(control_id, control_version)
        
        expected_sql = normalize_sql(f"""
            DELETE FROM delta.`{table_paths['controls']}`
            WHERE control_id = '{control_id}' AND control_version = '{control_version}'
        """)
        
        actual_sql = normalize_sql(mock_sql.call_args[0][0])
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test merging control data
@patch('backend.tables.controls.table_paths', {'controls': table_paths['controls']})
def test_merge_control_data(spark):
    control_data = [
        ("Dept_002", "Feature_002", "Control_002", "Group_002", "v2.0", "notebook_002", "url_002", "2024-09-11 12:00:00", "2024-09-11 12:00:00", "2024-09-11 12:00:00")
    ]
    
    with patch.object(spark, 'createDataFrame') as mock_create_df, patch.object(spark, 'sql') as mock_sql:
        merge_control_data(control_data)
        
        expected_columns = ["department_id", "feature_id", "control_id", "control_group", "control_version", "control_notebook_id", "control_notebook_url", "created_timestamp", "modified_timestamp"]
        mock_create_df.assert_called_once_with(control_data, expected_columns)
        
        expected_sql = normalize_sql(f"""
            MERGE INTO delta.`{table_paths['controls']}` AS target
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

# Mocking the table paths (assuming it's in the config)
@patch('backend.tables.departments.table_paths', {'departments': table_paths['departments']})
def test_create_departments_table(spark):
    with patch.object(spark, 'sql') as mock_sql:
        create_departments_table()
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        # Verify if the correct SQL statement was executed
        expected_sql = normalize_sql(f"""
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
            """
        )
        # mock_sql.assert_called_once_with(expected_sql)
        assert expected_sql == actual_sql, "Expected: {}, but got: {}".format( expected_sql, actual_sql)

@patch('backend.tables.departments.table_paths', {'departments': table_paths['departments']})
def test_update_department_data(spark):
    department_id = "Dept_001"
    new_lead_name = "Alice Johnson"
    new_lead_email = "alice.johnson@example.com"
    
    with patch.object(spark, 'sql') as mock_sql:
        update_department_data(department_id, new_lead_name, new_lead_email)
        
        # Normalize the SQL strings
        expected_sql = normalize_sql(f"""
            MERGE INTO delta.`{table_paths['departments']}` AS target
            USING (SELECT '{department_id}' AS department_id, '{new_lead_name}' AS lead_name, '{new_lead_email}' AS lead_email) AS source
            ON target.department_id = source.department_id
            WHEN MATCHED THEN
              UPDATE SET target.lead_name = source.lead_name, target.lead_email = source.lead_email, target.modified_timestamp = current_timestamp();
        """)
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        # Assert the normalized SQL
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"


@patch('backend.tables.departments.table_paths', {'departments': table_paths['departments']})
def test_delete_department(spark):
    department_id = "Dept_001"
    
    with patch.object(spark, 'sql') as mock_sql:
        delete_department(department_id)
        
        # Normalize the SQL strings
        expected_sql = normalize_sql(f"""
            DELETE FROM delta.`{table_paths['departments']}`
            WHERE department_id = '{department_id}'
        """)
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        # Assert the normalized SQL
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

@patch('backend.tables.departments.table_paths', {'departments': table_paths['departments']})
def test_merge_department_data(spark):
    # Prepare department data without timestamps
    department_data = [
        "Dept_003", "Marketing", "Eve", "eve@example.com", "Frank", "frank@example.com"
    ]
    
    # Expected schema (with timestamp fields)
    expected_schema = StructType([
        StructField("department_id", StringType(), True),
        StructField("department_name", StringType(), True),
        StructField("lead_name", StringType(), True),
        StructField("lead_email", StringType(), True),
        StructField("point_of_contact_name", StringType(), True),
        StructField("point_of_contact_email", StringType(), True),
    #     StructField("created_timestamp", TimestampType(), True),
    #     StructField("modified_timestamp", TimestampType(), True)
    ])
    
    # Mock the createDataFrame and sql calls
    with patch.object(spark, 'createDataFrame') as mock_create_df, patch.object(spark, 'sql') as mock_sql:
        # Call the function being tested
        merge_department_data(department_data)
        
        # Create the expected Row object (mapping individual fields, not as a tuple)
        expected_row = [Row(department_id="Dept_003", department_name="Marketing", lead_name="Eve", lead_email="eve@example.com", point_of_contact_name="Frank", point_of_contact_email="frank@example.com")]
        
        # Check if createDataFrame was called with the correct data and schema
        mock_create_df.assert_called_once_with(expected_row, schema=expected_schema)
        
        # Normalize the SQL strings for comparison
        expected_sql = normalize_sql(f"""
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
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        # Assert the normalized SQL
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test function to get department by ID
@patch('backend.tables.departments.table_paths', {'departments': table_paths['departments']})
def test_get_department_by_id(spark):
    department_id = "Dept_001"
    
    # Mock the spark.sql call
    with patch.object(spark, 'sql') as mock_sql:
        # Call the function under test
        get_department_by_id(department_id)

        # Expected SQL query
        expected_sql = normalize_sql(f"""
            SELECT * FROM delta.`{table_paths['departments']}`
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

# Test creating the features table
@patch('backend.tables.features.table_paths', {'features': table_paths['features']})
def test_create_features_table(spark):
    with patch.object(spark, 'sql') as mock_sql:
        create_features_table()
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        expected_sql = normalize_sql(f"""
            CREATE TABLE IF NOT EXISTS delta.`{table_paths['features']}`
            (
                department_id STRING,
                feature_id STRING,
                feature_name STRING,
                feature_version STRING,
                feature_query_id STRING,
                feature_logic STRING,
                triage_team ARRAY<STRING>,
                created_timestamp TIMESTAMP,
                modified_timestamp TIMESTAMP
            )
            USING DELTA
            PARTITIONED BY (department_id);
        """)
        
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test inserting feature data
@patch('backend.tables.features.table_paths', {'features': table_paths['features']})
def test_insert_feature_data(spark):
    feature_data = [
        ("Dept_001", "Feature_001", "Feature A", "v1.0", "query_001", "notebook_001", ["alice@example.com", "bob@example.com"], "2024-09-10 10:00:00", "2024-09-10 10:00:00")
    ]
    
    with patch.object(spark, 'createDataFrame') as mock_create_df:
        insert_feature_data(feature_data)
        expected_columns = ["department_id", "feature_id", "feature_name", "feature_version", "feature_query_id", "feature_logic", "triage_team", "created_timestamp", "modified_timestamp"]
        
        mock_create_df.assert_called_once_with(feature_data, expected_columns)

# Test updating feature data
@patch('backend.tables.features.table_paths', {'features': table_paths['features']})
def test_update_feature_data(spark):
    feature_id = "Feature_001"
    new_feature_name = "Feature A Updated"
    new_feature_version = "v1.1"
    
    with patch.object(spark, 'sql') as mock_sql:
        update_feature_data(feature_id, new_feature_name, new_feature_version)
        
        expected_sql = normalize_sql(f"""
            MERGE INTO delta.`{table_paths['features']}` AS target
            USING (SELECT '{feature_id}' AS feature_id, '{new_feature_name}' AS feature_name, '{new_feature_version}' AS feature_version) AS source
            ON target.feature_id = source.feature_id
            WHEN MATCHED THEN
              UPDATE SET target.feature_name = source.feature_name, target.feature_version = source.feature_version, target.modified_timestamp = current_timestamp();
        """)
        
        actual_sql = normalize_sql(mock_sql.call_args[0][0])
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test deleting feature data
@patch('backend.tables.features.table_paths', {'features': table_paths['features']})
def test_delete_feature(spark):
    feature_id = "Feature_001"
    
    with patch.object(spark, 'sql') as mock_sql:
        delete_feature(feature_id)
        
        expected_sql = normalize_sql(f"""
            DELETE FROM delta.`{table_paths['features']}`
            WHERE feature_id = '{feature_id}'
        """)
        
        actual_sql = normalize_sql(mock_sql.call_args[0][0])
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test merging feature data
@patch('backend.tables.features.table_paths', {'features': table_paths['features']})
def test_merge_feature_data(spark):
    feature_data = ["Dept_002", "Feature_002", "Feature B", "v2.0", "query_002", "notebook_002", ["eve@example.com"]]    
    # Expected schema (with timestamp fields)
    expected_schema = StructType([
        StructField("department_id", StringType(), True),
        StructField("feature_id", StringType(), True),
        StructField("feature_name", StringType(), True),
        StructField("feature_version", StringType(), True),
        StructField("feature_query_id", StringType(), True),
        StructField("feature_logic", StringType(), True),
        StructField("triage_team", ArrayType(StringType(), True), True),
    #     StructField("created_timestamp", TimestampType(), True),
    #     StructField("modified_timestamp", TimestampType(), True)
    ])

    with patch.object(spark, 'createDataFrame') as mock_create_df, patch.object(spark, 'sql') as mock_sql:
        merge_feature_data(feature_data)
        
        # Create the expected Row object (mapping individual fields, not as a tuple)
        expected_row = [Row(department_id="Dept_002", feature_id= "Feature_002", feature_name= "Feature B", feature_version= "v2.0", feature_query_id= "query_002", feature_logic= "notebook_002", triage_team= ["eve@example.com"])]

        mock_create_df.assert_called_once_with(expected_row, schema=expected_schema)
        
        expected_sql = normalize_sql(f"""
            MERGE INTO delta.`{table_paths['features']}` AS target
            USING source_table AS source
            ON target.feature_id = source.feature_id
            WHEN MATCHED THEN
              UPDATE SET target.feature_name = source.feature_name,
                         target.feature_version = source.feature_version,
                         target.feature_query_id = source.feature_query_id,
                         target.feature_logic = source.feature_logic,
                         target.triage_team = source.triage_team,
                         target.modified_timestamp = current_timestamp()
            WHEN NOT MATCHED THEN
              INSERT (department_id, feature_id, feature_name, feature_version, feature_query_id, feature_logic, triage_team, created_timestamp, modified_timestamp)
              VALUES (source.department_id, source.feature_id, source.feature_name, source.feature_version, source.feature_query_id, source.feature_logic, source.triage_team, current_timestamp(), current_timestamp());
        """)
        
        actual_sql = normalize_sql(mock_sql.call_args[0][0])
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test function to get feature by ID
@patch('backend.tables.features.table_paths', {'features': table_paths['features']})
def test_get_feature_by_id(spark):
    feature_id = "Dept_001"
    
    # Mock the spark.sql call
    with patch.object(spark, 'sql') as mock_sql:
        # Call the function under test
        get_feature_by_id(feature_id)

        # Expected SQL query
        expected_sql = normalize_sql(f"""
            SELECT * FROM delta.`{table_paths['features']}`
            WHERE feature_id = '{feature_id}'
        """)

        # Get the actual SQL from the mock call
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        # Assert that the SQL matches the expected query
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

        # Optionally, mock the return value and verify the result
        mock_sql.return_value = "mocked result"
        result = get_feature_by_id(feature_id)
        assert result == "mocked result"

@patch('backend.tables.control_runs.table_paths', {'control_runs': table_paths['control_runs']})
def test_create_control_run_table(spark):
    with patch.object(spark, 'sql') as mock_sql:
        create_control_run_table()
        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        expected_sql = normalize_sql(f"""
            CREATE TABLE IF NOT EXISTS delta.`{table_paths['control_runs']}`
            (
                department_id STRING,
                feature_id STRING,
                feature_version STRING,
                feature_data array<struct<Index:string,Feature:double>>,
                control_id STRING,
                control_version STRING,
                control_signal_count STRING,
                control_signal_data array<struct<Index:string,Feature:double,moving_range:double,UCL:double,LCL:double,CL:double,out_of_control:boolean>>,
                signal_detected BOOLEAN,
                control_run_timestamp TIMESTAMP
            )
            USING DELTA
            PARTITIONED BY (department_id, feature_id);
        """)

        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test inserting control run data
@patch('backend.tables.control_runs.table_paths', {'control_runs': table_paths['control_runs']})
def test_insert_control_run_data(spark):
    run_data = [
        ("Dept_001", "Feature_001", "v1.0", 
         [("180", 72.145), ("179", 72.023)], 
         "Control_001", "v1.0", "10", 
         [("157", 71.166, 0.847, 72.550, 71.349, 71.950, True)], 
         True, "2024-09-10 10:00:00")
    ]
    
    with patch.object(spark, 'createDataFrame') as mock_create_df:
        insert_control_run_data(run_data)
        expected_columns = ["department_id", "feature_id", "feature_version", "feature_data", 
                            "control_id", "control_version", "control_signal_count", 
                            "control_signal_data", "signal_detected", "control_run_timestamp"]        
        mock_create_df.assert_called_once_with(run_data, expected_columns)

@patch('backend.tables.control_runs.table_paths', {'control_runs': table_paths['control_runs']})
def test_update_control_run_data(spark):
    control_id = "Control_001"
    control_version = "v1.0"
    new_signal_data = "ARRAY(STRUCT(157, 71.166, 0.900, 72.600, 71.300, 71.950, False))"
    new_signal_detected = False

    with patch.object(spark, 'sql') as mock_sql:
        # Manually set control_signal_data in the SQL form (not a Python list)
        update_control_run_data(control_id, control_version, new_signal_data, new_signal_detected)

        # Build the expected SQL manually
        expected_sql = normalize_sql(f"""
            MERGE INTO delta.`{table_paths['control_runs']}` AS target
            USING (
                SELECT
                    '{control_id}' AS control_id,
                    '{control_version}' AS control_version,
                    '{new_signal_data}' AS control_signal_data,
                    '{new_signal_detected}' AS signal_detected
                ) AS source
            ON target.control_id = source.control_id AND target.control_version = source.control_version
            WHEN MATCHED THEN
              UPDATE SET target.control_signal_data = source.control_signal_data,
                         target.signal_detected = source.signal_detected,
                         target.control_run_timestamp = current_timestamp();
        """)

        actual_sql = normalize_sql(mock_sql.call_args[0][0])

        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test deleting control run data
@patch('backend.tables.control_runs.table_paths', {'control_runs': table_paths['control_runs']})
def test_delete_control_run(spark):
    control_id = "Control_001"
    control_version = "v1.0"
    
    with patch.object(spark, 'sql') as mock_sql:
        delete_control_run(control_id, control_version)
        
        expected_sql = normalize_sql(f"""
            DELETE FROM delta.`{table_paths['control_runs']}`
            WHERE control_id = '{control_id}' AND control_version = '{control_version}'
        """)
        
        actual_sql = normalize_sql(mock_sql.call_args[0][0])
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"

# Test merging (upserting) control run data
@patch('backend.tables.control_runs.table_paths', {'control_runs': table_paths['control_runs']})
def test_merge_control_run_data(spark):
    run_data = [
        ("Dept_002", "Feature_002", "v2.0", 
         [("178", 72.121), ("177", 71.977)], 
         "Control_002", "v2.0", "20", 
         [("157", 71.166, 0.847, 72.550, 71.349, 71.950, True)], 
         True, "2024-09-11 12:00:00")
    ]
    
    with patch.object(spark, 'createDataFrame') as mock_create_df, patch.object(spark, 'sql') as mock_sql:
        merge_control_run_data(run_data)
        
        expected_columns = ["department_id", "feature_id", "feature_version", "feature_data", 
                            "control_id", "control_version", "control_signal_count", 
                            "control_signal_data", "signal_detected", "control_run_timestamp"]
        mock_create_df.assert_called_once_with(run_data, expected_columns)
        
        expected_sql = normalize_sql(f"""
            MERGE INTO delta.`{table_paths['control_runs']}` AS target
            USING source_table AS source
            ON target.control_id = source.control_id AND target.control_version = source.control_version
            WHEN MATCHED THEN
              UPDATE SET target.control_signal_data = source.control_signal_data,
                         target.signal_detected = source.signal_detected,
                         target.control_signal_count = source.control_signal_count,
                         target.control_run_timestamp = current_timestamp()
            WHEN NOT MATCHED THEN
              INSERT (department_id, feature_id, feature_version, feature_data, 
                      control_id, control_version, control_signal_count, 
                      control_signal_data, signal_detected, control_run_timestamp)
              VALUES (source.department_id, source.feature_id, source.feature_version, source.feature_data, 
                      source.control_id, source.control_version, source.control_signal_count, 
                      source.control_signal_data, source.signal_detected, current_timestamp());
        """)
        
        actual_sql = normalize_sql(mock_sql.call_args[0][0])
        assert expected_sql == actual_sql, f"Expected: {expected_sql}, but got: {actual_sql}"
