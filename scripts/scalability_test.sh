#!/bin/bash
# scalability_test.sh
# Tests Spark scalability by running the same job with different core counts

echo "Starting scalability tests..."

echo "Test 1: Running with 1 Core (local[1])"
spark-submit \
    --master local[1] \
    --driver-memory 2g \
    dataframe_api/df_queries.py > results/scalability_1core_metrics.txt
echo "Test 1 complete."

echo "Test 2: Running with 2 Cores (local[2])"
spark-submit \
    --master local[2] \
    --driver-memory 2g \
    dataframe_api/df_queries.py > results/scalability_2core_metrics.txt
echo "Test 2 complete."

echo "Test 3: Running with 4 Cores (local[4])"
spark-submit \
    --master local[4] \
    --driver-memory 2g \
    dataframe_api/df_queries.py > results/scalability_4core_metrics.txt
echo "Test 3 complete."

echo "All scalability tests finished. Review the text files in the results folder to compare execution times."