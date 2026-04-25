#!/bin/bash
# cluster_config.sh
# Configures the Spark cluster environment for execution

# Set driver and executor memory
export SPARK_DRIVER_MEMORY=2g
export SPARK_EXECUTOR_MEMORY=4g

# Set the number of executor cores
export SPARK_EXECUTOR_CORES=2

# Set the default parallelism to optimize data shuffling
export SPARK_DEFAULT_PARALLELISM=4

echo "Spark cluster configuration applied."
echo "Driver Memory: $SPARK_DRIVER_MEMORY"
echo "Executor Memory: $SPARK_EXECUTOR_MEMORY"
echo "Executor Cores: $SPARK_EXECUTOR_CORES"
echo "Default Parallelism: $SPARK_DEFAULT_PARALLELISM"