from pyspark.sql import SparkSession

app_name = 'DeltaLakeInitialization'

def get_spark_session():
    spark = SparkSession.builder \
        .appName(f"{app_name}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    return spark
