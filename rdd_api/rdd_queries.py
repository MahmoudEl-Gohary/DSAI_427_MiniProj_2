import time
import shutil
import os
from pyspark.sql import SparkSession

results_dir = "/home/hduser/mini_project_2/results/"
if os.path.exists(results_dir):
    for item in os.listdir(results_dir):
        item_path = os.path.join(results_dir, item)
        if item.startswith("rdd_q") and os.path.isdir(item_path):
            shutil.rmtree(item_path)

spark = SparkSession.builder \
    .appName("RDD_Queries") \
    .getOrCreate()

sc = spark.sparkContext

df = spark.read.csv("file:///home/hduser/mini_project_2/data/GUIDE_Test.csv", header=True, inferSchema=True)
rdd = df.rdd.filter(lambda row: row is not None)

print("Query 1: RDD API")
start = time.time()
q1 = rdd.filter(lambda row: row["Category"] == "Malware") \
        .map(lambda row: (row["IncidentGrade"], 1)) \
        .reduceByKey(lambda a, b: a + b)
q1.saveAsTextFile("file:///home/hduser/mini_project_2/results/rdd_q1")
print(f"Execution Time Q1: {time.time() - start:.4f} seconds")

print("Query 2: RDD API")
start = time.time()
q2 = rdd.map(lambda row: ((row["Category"], row["Usage"]), 1)) \
        .reduceByKey(lambda a, b: a + b)
q2.saveAsTextFile("file:///home/hduser/mini_project_2/results/rdd_q2")
print(f"Execution Time Q2: {time.time() - start:.4f} seconds")

print("Query 3: RDD API")
start = time.time()
q3 = rdd.filter(lambda row: row["Usage"] == "Public" and row["Category"] != "Test")
q3.saveAsTextFile("file:///home/hduser/mini_project_2/results/rdd_q3")
print(f"Execution Time Q3: {time.time() - start:.4f} seconds")

print("Query 4: RDD API")
start = time.time()
q4 = rdd.map(lambda row: (row["AlertTitle"], 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1], ascending=False)
q4.saveAsTextFile("file:///home/hduser/mini_project_2/results/rdd_q4")
print(f"Execution Time Q4: {time.time() - start:.4f} seconds")

print("Query 5: RDD API")
start = time.time()
q5 = rdd.map(lambda row: ((row["Category"], row["AlertTitle"]), 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
        .groupByKey() \
        .mapValues(lambda vals: sorted(list(vals), key=lambda x: x[1], reverse=True))
q5.saveAsTextFile("file:///home/hduser/mini_project_2/results/rdd_q5")
print(f"Execution Time Q5: {time.time() - start:.4f} seconds")

print("Query 6: RDD API")
start = time.time()
q6_counts = rdd.map(lambda row: (row["Timestamp"], 1)) \
               .reduceByKey(lambda a, b: a + b) \
               .sortByKey() \
               .collect()

cumulative = []
total = 0
for time_val, count in q6_counts:
    total += count
    cumulative.append((time_val, total))
sc.parallelize(cumulative).saveAsTextFile("file:///home/hduser/mini_project_2/results/rdd_q6")
print(f"Execution Time Q6: {time.time() - start:.4f} seconds")

print("Query 7: RDD API")
start = time.time()
top_grade = rdd.map(lambda row: (row["IncidentGrade"], 1)) \
               .reduceByKey(lambda a, b: a + b) \
               .sortBy(lambda x: x[1], ascending=False) \
               .first()[0]

q7 = rdd.filter(lambda row: row["IncidentGrade"] == top_grade) \
        .map(lambda row: row["AlertTitle"]) \
        .distinct()
q7.saveAsTextFile("file:///home/hduser/mini_project_2/results/rdd_q7")
print(f"Execution Time Q7: {time.time() - start:.4f} seconds")

print("Query 8: RDD API")
start = time.time()
cached_rdd = rdd.filter(lambda row: row["Usage"] == "Public").cache()
q8 = cached_rdd.map(lambda row: (row["Category"], 1)) \
               .reduceByKey(lambda a, b: a + b)
q8.saveAsTextFile("file:///home/hduser/mini_project_2/results/rdd_q8")
print(f"Execution Time Q8: {time.time() - start:.4f} seconds")

print("Query 9: RDD API")
start = time.time()
lookup_dict = {"TP": "True Positive", "FP": "False Positive"}
broadcast_lookup = sc.broadcast(lookup_dict)
q9 = rdd.map(lambda row: (str(row), broadcast_lookup.value.get(row["IncidentGrade"], "Unknown")))
q9.saveAsTextFile("file:///home/hduser/mini_project_2/results/rdd_q9")
print(f"Execution Time Q9: {time.time() - start:.4f} seconds")

print("Query 10: RDD API")
start = time.time()
rdd_subset1 = rdd.filter(lambda row: row["Category"] == "Malware").map(lambda row: (row["IncidentId"], str(row)))
rdd_subset2 = rdd.filter(lambda row: row["Usage"] == "Public").map(lambda row: (row["IncidentId"], str(row)))
q10 = rdd_subset1.join(rdd_subset2)
q10.saveAsTextFile("file:///home/hduser/mini_project_2/results/rdd_q10")
print(f"Execution Time Q10: {time.time() - start:.4f} seconds")

spark.stop()