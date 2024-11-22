#!/bin/bash
#/docker/dev/docker_startup_test.sh

# Check if SPARK_HOME is set
if [ -z "$SPARK_HOME" ]; then
  echo "SPARK_HOME is not set. Please ensure Spark is installed."
  exit 1
fi

# Check Delta package installation
echo "Checking Delta package installation..."
DELTA_PACKAGE_CHECK=$($SPARK_HOME/bin/pyspark --packages io.delta:${DELTA_PACKAGE_VERSION} --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --version 2>&1)
if [[ "$DELTA_PACKAGE_CHECK" == *"io.delta"* ]]; then
  echo "Delta package is installed correctly."
else
  echo "Delta package installation failed or not included in the Spark session."
  exit 1
fi

# Check connection to mount location
MOUNT_DIR="/mnt/opssig/spark-warehouse"
echo "Checking mount location..."
if [ -d "$MOUNT_DIR" ]; then
  echo "Mount location '$MOUNT_DIR' exists."
else
  echo "Mount location '$MOUNT_DIR' does not exist. Please verify your Docker mount."
  exit 1
fi

# Check if Spark session can read/write from the mount
echo "Testing Spark session to verify catalog setup..."
SPARK_TEST=$($SPARK_HOME/bin/pyspark \
  --packages io.delta:${DELTA_PACKAGE_VERSION} \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.sql.warehouse.dir=$MOUNT_DIR" \
  --conf "spark.databricks.delta.retentionDurationCheck.enabled=false" \
  -e "from pyspark.sql import SparkSession; spark = SparkSession.builder.getOrCreate(); spark.sql('SHOW DATABASES').show()" 2>&1)

if [[ "$SPARK_TEST" == *"default"* ]]; then
  echo "Spark session and catalog setup are working properly."
else
  echo "Spark session or catalog setup failed. Check the configuration and try again."
  exit 1
fi

echo "All checks passed successfully!"
exit 0
