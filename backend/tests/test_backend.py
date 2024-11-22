from unittest.mock import patch, MagicMock, ANY
import pytest
import uuid

from delta.tables import DeltaTable

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, ArrayType, DoubleType 
import pyspark.sql.functions as F

from backend.delta_tables.controls import (
    create_controls_table,
    insert_control_data,
    update_control_data,
    delete_control,
    merge_control_data,
)

from backend.delta_tables.control_runs import (
    create_control_runs_table,
    insert_control_run_data,
    update_control_run_data,
    delete_control_run,
    merge_control_run_data,
)

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.appName("pytest-spark").master("local").getOrCreate()
    yield spark
    spark.stop()

@patch('backend.delta_tables.controls.DeltaTable')
@patch('backend.delta_tables.controls.table_details', {'controls_fact': {'name': "cdo.opssig_controls_fact"}})
def test_create_controls_table(mock_delta_table, spark):
    # Configure the mock to simulate chained calls
    create_mock = MagicMock()
    mock_delta_table.createIfNotExists.return_value = create_mock

    # Call the function being tested
    create_controls_table()

    # Assertions for the chained calls
    mock_delta_table.createIfNotExists.assert_called_once_with(spark)
    create_mock.tableName.assert_called_once_with("cdo.opssig_controls_fact")
    # create_mock.addColumns.assert_called_once()  # Ensure schema was added
    # create_mock.partitionedBy.assert_called_once_with("department_id", "control_group")
    # create_mock.execute.assert_called_once()

# Test inserting control data
@patch('backend.delta_tables.controls.DeltaTable')
@patch('backend.delta_tables.controls.table_details', {'controls_fact': {'name': "cdo.opssig_controls_fact"}})
def test_insert_control_data(mock_delta_table, spark):
    control_data = [
        ("Dept_001", "Feature_001", "Control_001", "Group_001", "v1.0", "notebook_001", "url_001")
    ]
    mock_delta_table.return_value = MagicMock()
    
    with patch.object(spark, 'createDataFrame') as mock_create_df:
        insert_control_data(control_data)
        mock_create_df.assert_called_once()

@patch('backend.delta_tables.controls.DeltaTable')
@patch('backend.delta_tables.controls.table_details', {'controls_fact': {'name': "cdo.opssig_controls_fact"}})
def test_insert_control_data(mock_delta_table, spark):
    control_data = [
        ("Dept_001", "Feature_001", "Control_001", "Group_001", "v1.0", "notebook_001", "url_001")
    ]

    # Mock the schema for the Delta table
    mocked_schema = StructType([
        StructField("department_id", StringType(), False),
        StructField("feature_id", StringType(), False),
        StructField("control_id", StringType(), False),
        StructField("control_group", StringType(), True),
        StructField("control_version", StringType(), True),
        StructField("control_notebook_id", StringType(), True),
        StructField("control_dashbaord_url", StringType(), True),
        StructField("created_timestamp", TimestampType(), True),
        StructField("modified_timestamp", TimestampType(), True),
    ])

    # Mock the spark.table call to return a DataFrame with the mocked schema
    with patch.object(spark, 'table') as mock_table, \
         patch.object(spark, 'createDataFrame') as mock_create_df:
        mock_table.return_value.schema = mocked_schema

        # Call the function being tested
        insert_control_data(control_data)

        # Assertions
        mock_table.assert_called_once_with("cdo.opssig_controls_fact")  # Ensure the correct table name is accessed
        mock_create_df.assert_called_once()  # Ensure createDataFrame was called

@patch('backend.delta_tables.controls.DeltaTable')
@patch('backend.delta_tables.controls.table_details', {'controls_fact': {'name': "cdo.opssig_controls_fact"}})
def test_update_control_data(mock_delta_table, spark):
    control_id = "Control_001"
    control_version = "v1.0"
    new_control_dashbaord_url = "updated_url_001"

    # Create a mock DeltaTable
    delta_table_mock = MagicMock()
    mock_delta_table.forName.return_value = delta_table_mock

    # Call the function to update control data
    update_control_data(control_id, control_version, new_control_dashbaord_url)

    # Capture arguments passed to the mock's `update` method
    call_args = delta_table_mock.update.call_args[1]

    # Validate the condition argument
    expected_condition = (F.col("control_id") == control_id) & (F.col("control_version") == control_version)
    actual_condition = call_args["condition"]
    assert str(expected_condition) == str(actual_condition), "Condition does not match the expected value"

    # Validate the `set` argument
    expected_set = {
        "control_dashbaord_url": F.lit(new_control_dashbaord_url),
        "modified_timestamp": F.current_timestamp(),
    }
    actual_set = call_args["set"]

    # Compare `set` argument values individually
    assert "control_dashbaord_url" in actual_set, "Missing 'control_dashbaord_url' in set values"
    assert str(expected_set["control_dashbaord_url"]) == str(actual_set["control_dashbaord_url"]), \
        "Value for 'control_dashbaord_url' does not match"
    assert str(expected_set["modified_timestamp"]) == str(actual_set["modified_timestamp"]), \
        "Value for 'modified_timestamp' does not match"

@patch('backend.delta_tables.controls.DeltaTable')
@patch('backend.delta_tables.controls.table_details', {'controls_fact': {'name': "cdo.opssig_controls_fact"}})
def test_delete_control(mock_delta_table, spark):
    control_id = "Control_001"
    control_version = "v1.0"

    # Mock DeltaTable
    delta_table_mock = MagicMock()
    mock_delta_table.forName.return_value = delta_table_mock

    # Call the function
    delete_control(control_id, control_version)

    # Capture the condition argument from the mock's `delete` call
    delete_condition = delta_table_mock.delete.call_args[0][0]

    # Define the expected condition
    expected_condition = (F.col("control_id") == control_id) & (F.col("control_version") == control_version)

    # Compare the string representations of the conditions
    assert str(expected_condition) == str(delete_condition), "Delete condition does not match"

@patch('backend.delta_tables.controls.DeltaTable')
@patch('backend.delta_tables.controls.table_details', {'controls_fact': {'name': "cdo.opssig_controls_fact"}})
def test_merge_control_data(mock_delta_table, spark):
    control_data = [
        ("Dept_002", "Feature_002", "Control_002", "Group_002", "v2.0", "notebook_002", "url_002")
    ]

    # Mock schema to simulate the Delta table schema
    mock_schema = StructType([
        StructField("department_id", StringType(), True),
        StructField("feature_id", StringType(), True),
        StructField("control_id", StringType(), True),
        StructField("control_group", StringType(), True),
        StructField("control_version", StringType(), True),
        StructField("control_notebook_id", StringType(), True),
        StructField("control_dashbaord_url", StringType(), True)
    ])

    # Mock `spark.table` to return a DataFrame with the mocked schema
    with patch.object(spark, 'table') as mock_table:
        mock_df = MagicMock()
        mock_df.schema = mock_schema
        mock_table.return_value = mock_df

        # Mock `createDataFrame`
        with patch.object(spark, 'createDataFrame') as mock_create_df:
            mock_created_df = MagicMock()
            mock_create_df.return_value = mock_created_df

            # Mock DeltaTable
            delta_table_mock = MagicMock()
            mock_delta_table.forName.return_value = delta_table_mock

            # Call the function
            merge_control_data(control_data)

            # Validate `createDataFrame` was called with the correct schema
            mock_create_df.assert_called_once_with([Row(*control_data)], schema=mock_schema)

            # Validate the `merge` method was called
            delta_table_mock.alias.return_value.merge.assert_called_once()

# @patch('backend.delta_tables.controls.DeltaTable')
# @patch('backend.delta_tables.controls.table_details', {'control_runs_fact': {'name': "cdo.opssig_control_runs_fact"}})
# def test_create_control_runs_table(mock_delta_table, spark):
#     with patch.object(DeltaTable, 'createIfNotExists') as mock_create:
#         # Mock the DeltaTable builder
#         builder_mock = MagicMock()
#         mock_create.return_value = builder_mock
#
#         # Call the function to create the control runs table
#         create_control_runs_table()
#
#         # Validate that the createIfNotExists method was called
#         builder_mock.tableName.assert_called_once_with("cdo.opssig_control_runs_fact")
#
#         # Validate that the addColumns method was called with the correct schema
#         expected_schema = StructType([
#             StructField("department_id", StringType(), False),
#             StructField("feature_id", StringType(), False),
#             StructField("feature_version", StringType(), True),
#             StructField("feature_data", ArrayType(StructType([
#                 StructField("Index", StringType(), True),
#                 StructField("Feature", DoubleType(), True)
#             ])), True),
#             StructField("control_id", StringType(), True),
#             StructField("control_version", StringType(), True),
#             StructField("control_signal_count", StringType(), True),
#             StructField("control_signal_data", ArrayType(StructType([
#                 StructField("Index", StringType(), True),
#                 StructField("Feature", DoubleType(), True),
#                 StructField("moving_range", DoubleType(), True),
#                 StructField("UCL", DoubleType(), True),
#                 StructField("LCL", DoubleType(), True),
#                 StructField("CL", DoubleType(), True),
#                 StructField("out_of_control", BooleanType(), True)
#             ])), True),
#             StructField("signal_detected", BooleanType(), True),
#             StructField("control_run_id", StringType(), False),
#             StructField("control_run_timestamp", TimestampType(), True),
#         ])
#         builder_mock.addColumns.assert_called_once_with(expected_schema)
#
#         # Validate that the partitionedBy and execute methods were called
#         builder_mock.partitionedBy.assert_called_once_with("department_id", "feature_id")
#         builder_mock.execute.assert_called_once()
#
# @patch('backend.delta_tables.controls.DeltaTable')
# @patch('backend.delta_tables.controls.table_details', {'control_runs_fact': {'name': "cdo.opssig_control_runs_fact"}})
# def test_insert_control_run_data(mock_delta_table, spark):
#     run_data = [
#         ("Dept_001", "Feature_001", "v1.0", [], "Control_001", "v1.0", "10", [], True)
#     ]
#
#     # Mock the Spark table schema
#     schema = StructType([
#         StructField("department_id", StringType(), False),
#         StructField("feature_id", StringType(), False),
#         StructField("feature_version", StringType(), True),
#         StructField("feature_data", ArrayType(StructType([
#             StructField("Index", StringType(), True),
#             StructField("Feature", DoubleType(), True)
#         ])), True),
#         StructField("control_id", StringType(), True),
#         StructField("control_version", StringType(), True),
#         StructField("control_signal_count", StringType(), True),
#         StructField("control_signal_data", ArrayType(StructType([
#             StructField("Index", StringType(), True),
#             StructField("Feature", DoubleType(), True),
#             StructField("moving_range", DoubleType(), True),
#             StructField("UCL", DoubleType(), True),
#             StructField("LCL", DoubleType(), True),
#             StructField("CL", DoubleType(), True),
#             StructField("out_of_control", BooleanType(), True)
#         ])), True),
#         StructField("signal_detected", BooleanType(), True),
#         StructField("control_run_id", StringType(), False),
#         StructField("control_run_timestamp", TimestampType(), True),
#     ])
#
#     with patch.object(spark, 'table') as mock_table:
#         mock_table.return_value.schema = schema
#         with patch.object(spark, 'createDataFrame') as mock_create_df:
#             df_mock = MagicMock()
#             mock_create_df.return_value = df_mock
#
#             insert_control_run_data(run_data)
#
#             # Validate DataFrame creation
#             mock_create_df.assert_called_once()
#             args, kwargs = df_mock.withColumn.call_args_list[0]
#             assert str(args[0]) == "control_run_timestamp", "Column name mismatch"
#             assert str(args[1]) == str(F.current_timestamp()), "Column value mismatch"
#
# @patch('backend.delta_tables.controls.DeltaTable')
# @patch('backend.delta_tables.controls.table_details', {'control_runs_fact': {'name': "cdo.opssig_control_runs_fact"}})
# def test_merge_control_run_data(mock_delta_table, spark):
#     run_data = ("Dept_001", "Feature_001", "v1.0", [], "Control_001", "v1.0", "10", [], True)
#
#     # Mock the Spark table schema
#     schema = StructType([
#         StructField("department_id", StringType(), False),
#         StructField("feature_id", StringType(), False),
#         StructField("feature_version", StringType(), True),
#         StructField("feature_data", ArrayType(StructType([
#             StructField("Index", StringType(), True),
#             StructField("Feature", DoubleType(), True)
#         ])), True),
#         StructField("control_id", StringType(), True),
#         StructField("control_version", StringType(), True),
#         StructField("control_signal_count", StringType(), True),
#         StructField("control_signal_data", ArrayType(StructType([
#             StructField("Index", StringType(), True),
#             StructField("Feature", DoubleType(), True),
#             StructField("moving_range", DoubleType(), True),
#             StructField("UCL", DoubleType(), True),
#             StructField("LCL", DoubleType(), True),
#             StructField("CL", DoubleType(), True),
#             StructField("out_of_control", BooleanType(), True)
#         ])), True),
#         StructField("signal_detected", BooleanType(), True),
#         StructField("control_run_id", StringType(), False),
#         StructField("control_run_timestamp", TimestampType(), True),
#     ])
#
#     with patch.object(spark, 'table') as mock_table:
#         mock_table.return_value.schema = schema
#         with patch.object(spark, 'createDataFrame') as mock_create_df:
#             df_mock = MagicMock()
#             mock_create_df.return_value = df_mock
#
#             delta_table_mock = MagicMock()
#             mock_delta_table.forName.return_value = delta_table_mock
#
#             merge_control_run_data(run_data)
#
#             # Validate the merge logic
#             delta_table_mock.alias.return_value.merge.assert_called_once()
#
# @patch('backend.delta_tables.controls.DeltaTable')
# @patch('backend.delta_tables.controls.table_details', {'control_runs_fact': {'name': "cdo.opssig_control_runs_fact"}})
# def test_update_control_run_data(mock_delta_table, spark):
#     control_run_id = str(uuid.uuid4())
#     new_signal_data = [{"Index": "1", "Feature": 2.5}]
#     new_signal_detected = True
#
#     delta_table_mock = MagicMock()
#     mock_delta_table.forName.return_value = delta_table_mock
#
#     update_control_run_data(control_run_id, new_signal_data, new_signal_detected)
#
#     # Validate the update logic
#     update_call_args = delta_table_mock.update.call_args[1]
#     expected_condition = F.col("control_run_id") == control_run_id
#     assert str(expected_condition) == str(update_call_args["condition"])
#
# @patch('backend.delta_tables.controls.DeltaTable')
# @patch('backend.delta_tables.controls.table_details', {'control_runs_fact': {'name': "cdo.opssig_control_runs_fact"}})
# def test_delete_control_run(mock_delta_table, spark):
#     control_run_id = str(uuid.uuid4())
#
#     delta_table_mock = MagicMock()
#     mock_delta_table.forName.return_value = delta_table_mock
#
#     delete_control_run(control_run_id)
#
#     # Validate the delete logic
#     delete_condition = delta_table_mock.delete.call_args[0][0]
#     expected_condition = F.col("control_run_id") == control_run_id
#     assert str(expected_condition) == str(delete_condition)
