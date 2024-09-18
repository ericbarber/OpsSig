#!/bin/bash

# Source Rust environment
source "$HOME/.cargo/env"

# Set PySpark environment variables
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='lab --ip=0.0.0.0 --no-browser'
export DELTA_SPARK_VERSION='3.1.0'
export DELTA_PACKAGE_VERSION=delta-spark_2.12:${DELTA_SPARK_VERSION}

# Initialize delta tables
python3 backend/initialize_db.py

# Run tests before starting the environment
echo "Running pytests: backend"
pytest --disable-warnings backend/tests
pytest_result_1=$?

echo "Running pytests: signal_watch"
pytest --disable-warnings signal_watch/tests
pytest_result_2=$?

# Check if tests were successful
if [ $pytest_result_1 -eq 0 ] && [ $pytest_result_2 -eq 0 ]; then
  echo "Tests passed. Exiting container."
  # # Start PySpark with Delta configurations
  # $SPARK_HOME/bin/pyspark --packages io.delta:${DELTA_PACKAGE_VERSION} \
  #   --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp -Dio.netty.tryReflectionSetAccessible=true" \
  #   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  #   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  exit 0
else
  echo "Tests failed. Exiting container."
  exit 1
fi
