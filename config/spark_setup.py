#/config/spark_setup.py
import os
from pyspark.sql import SparkSession
from delta import * # configure_spark_with_delta_pip

# Ensure that the environment variable is set
env = os.getenv("ENV", "dev")
warehouse_dir = os.getenv("SPARK_SQL_WAREHOUSE_DIR", 
                          f"/opt/spark/work-dir/spark-warehouse" # "/opt/spark/work-dir/spark-warehouse"
)

# Set the appropriate JDBC URL for your Hive metastore
hive_metastore_jdbc_url = "jdbc:derby:;databaseName=/opt/hive/metastore_db;create=true"

delta_package_version = os.getenv("DELTA_PACKAGE_VERSION", "")
if not delta_package_version:
    raise ValueError("Environment variable 'DELTA_PACKAGE_VERSION' is not set.")
app_name = 'OperationSignalProcessing'

def get_spark_session(app_name=app_name):
    """
    Initialize a Spark session. If running in Databricks, use the provided Spark session.
    Otherwise, configure a new Spark session locally.
    """
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        print("Running in Databricks, using the provided Spark session.")
        return SparkSession.builder.getOrCreate()  # Use Databricks-provided session

    elif 'spark' in globals():
        print(f"Existing Spark {spark.conf.get("spark.app.name")}")
        return SparkSession.builder.getOrCreate()  # Use Databricks-provided session

    else:  
        try:
            # Build the spark session        
            builder = SparkSession.builder \
                .appName(f"{app_name}") \
                .config("spark.sql.warehouse.dir", f"{warehouse_dir}") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config(
                    "spark.driver.extraJavaOptions",
                    "-Divy.cache.dir=/tmp -Divy.home=/tmp -Dio.netty.tryReflectionSetAccessible=true"
                )
                # .config("spark.jars.packages", f"io.delta:{delta_package_version}") \
                # .config("spark.sparkContext.setLogLevel", "DEBUG") \
                # .config("spark.sql.catalogImplementation", "hive") \
                # .config("spark.sql.catalog.spark_catalog.type", "hive") \
                # .config("spark.hadoop.hive.metastore.uris", "thrift://hive4:10000") \
                # .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"{hive_metastore_jdbc_url}") \
                # .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver") \
                # .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "APP") \
                # .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "") \
# Use the configure_spark_with_delta_pip function
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            return spark
        except Exception as e:
            print(f"Error initializing Spark session: {e}")
            return None
