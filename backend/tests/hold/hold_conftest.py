import pytest
import pyspark
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from unittest.mock import patch

# Fixture to initialize and teardown a Spark session
@pytest.fixture(scope="function")
def spark():
    delta_version = "3.2.0"  # Adjust this version as per your environment
    delta_package_version = f"delta-spark_2.12:{delta_version}"


    packages = [
        f"io.delta:${DELTA_PACKAGE_VERSION}"
    ]
    
    builder = (
        pyspark.sql.SparkSession.builder.appName("pytest-spark")
        .config("", "")
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instances", "1") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", f"io.delta:{delta_package_version}") \
        .config("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp -Dio.netty.tryReflectionSetAccessible=true") \
    )

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=my_packages
    ).getOrCreate()
    
    yield spark
    spark.stop()

# Fixture to set up the controls table
@pytest.fixture(scope="function", autouse=True)
def setup_controls_table(spark):
    """Creates the controls table before each test and drops it after."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/mnt/delta/controls` (
            department_id STRING,
            feature_id STRING,
            control_id STRING,
            control_group STRING,
            control_version STRING,
            control_notebook_id STRING,
            control_notebook_url STRING,
            created_timestamp TIMESTAMP,
            modified_timestamp TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (department_id, control_group);
    """)
    yield
    spark.sql("DROP TABLE IF EXISTS delta.`/mnt/delta/controls`")

# Fixture to set up the departments table
@pytest.fixture(scope="function", autouse=True)
def setup_departments_table(spark):
    """Creates the departments table before each test and drops it after."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/mnt/delta/departments` (
            department_id STRING,
            department_name STRING,
            lead_name STRING,
            lead_email STRING,
            point_of_contact_name STRING,
            point_of_contact_email STRING,
            created_timestamp TIMESTAMP,
            modified_timestamp TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (department_id);
    """)
    yield
    spark.sql("DROP TABLE IF EXISTS delta.`/mnt/delta/departments`")

# Fixture to set up the features table
@pytest.fixture(scope="function", autouse=True)
def setup_features_table(spark):
    """Creates the features table before each test and drops it after."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/mnt/delta/features` (
            department_id STRING,
            feature_id STRING,
            feature_name STRING,
            feature_version STRING,
            feature_query_id STRING,
            feature_logic STRING,
            triage_team ARRAY<STRING>,
            created_timestamp TIMESTAMP,
            modified_timestamp TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (department_id);
    """)
    yield
    spark.sql("DROP TABLE IF EXISTS delta.`/mnt/delta/features`")

# Fixture to mock the paths for tables
@pytest.fixture(scope="function", autouse=True)
def mock_table_paths():
    """Mocks the table paths for the controls, departments, and features."""
    with patch('backend.tables.controls.table_paths', {'controls': '/mnt/delta/controls'}), \
         patch('backend.tables.departments.table_paths', {'departments': '/mnt/delta/departments'}), \
         patch('backend.tables.features.table_paths', {'features': '/mnt/delta/features'}):
        yield
