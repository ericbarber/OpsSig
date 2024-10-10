import os
from pyspark.sql import SparkSession

# Ensure that the environment variable is set
delta_package_version = os.getenv('DELTA_PACKAGE_VERSION')
if not delta_package_version:
    raise ValueError("Environment variable 'DELTA_PACKAGE_VERSION' is not set.")

app_name = 'DeltaLakeInitialization'

def get_spark_session(app_name=app_name):
   
    try:
        # Ensure that the environment variable is set
        delta_package_version = os.getenv('DELTA_PACKAGE_VERSION')
        if not delta_package_version:
            raise ValueError("Environment variable 'DELTA_PACKAGE_VERSION' is not set.")
        
        # Build the spark session        
        spark = SparkSession.builder \
            .appName(f"{app_name}") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config(
                "spark.driver.extraJavaOptions",
                "-Divy.cache.dir=/tmp -Divy.home=/tmp -Dio.netty.tryReflectionSetAccessible=true"
            ) \
            .config("spark.jars.packages", f"io.delta:{delta_package_version}") \
            .getOrCreate()
        return spark
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        return None
