import time
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RDD_Queries") \
    .getOrCreate()

sc = spark.sparkContext

def get_memory_mb():
    runtime = sc._jvm.java.lang.Runtime.getRuntime()
    return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)

df = spark.read.csv("file:///home/hduser/mini_project_2/data/GUIDE_Test.csv", header=True, inferSchema=True)
rdd = df.rdd.filter(lambda row: row is not None)

print("Query 1: RDD API")
start_time = time.time()
start_mem = get_memory_mb()
q1 = rdd.filter(lambda row: row["Category"] == "Malware") \
        .map(lambda row: (row["IncidentGrade"], 1)) \
        .reduceByKey(lambda a, b: a + b)
q1.count()
print(f"Execution Time Q1: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q1: {max(0, get_memory_mb() - start_mem):.2f} MB")

print("Query 2: RDD API")
start_time = time.time()
start_mem = get_memory_mb()
q2 = rdd.map(lambda row: ((row["Category"], row["Usage"]), 1)) \
        .reduceByKey(lambda a, b: a + b)
q2.count()
print(f"Execution Time Q2: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q2: {max(0, get_memory_mb() - start_mem):.2f} MB")

print("Query 3: RDD API")
start_time = time.time()
start_mem = get_memory_mb()
q3 = rdd.filter(lambda row: row["Usage"] == "Public" and row["Category"] != "Test")
q3.count()
print(f"Execution Time Q3: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q3: {max(0, get_memory_mb() - start_mem):.2f} MB")

print("Query 4: RDD API")
start_time = time.time()
start_mem = get_memory_mb()
q4 = rdd.map(lambda row: (row["AlertTitle"], 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1], ascending=False)
q4.count()
print(f"Execution Time Q4: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q4: {max(0, get_memory_mb() - start_mem):.2f} MB")

print("Query 5: RDD API")
start_time = time.time()
start_mem = get_memory_mb()
q5 = rdd.map(lambda row: ((row["Category"], row["AlertTitle"]), 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
        .groupByKey() \
        .mapValues(lambda vals: sorted(list(vals), key=lambda x: x[1], reverse=True))
q5.count()
print(f"Execution Time Q5: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q5: {max(0, get_memory_mb() - start_mem):.2f} MB")

print("Query 6: RDD API")
start_time = time.time()
start_mem = get_memory_mb()
q6_counts = rdd.map(lambda row: (row["Timestamp"], 1)) \
               .reduceByKey(lambda a, b: a + b) \
               .sortByKey() \
               .collect()
cumulative = []
total = 0
for time_val, count in q6_counts:
    total += count
    cumulative.append((time_val, total))
sc.parallelize(cumulative).count()
print(f"Execution Time Q6: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q6: {max(0, get_memory_mb() - start_mem):.2f} MB")

print("Query 7: RDD API")
start_time = time.time()
start_mem = get_memory_mb()
top_grade = rdd.map(lambda row: (row["IncidentGrade"], 1)) \
               .reduceByKey(lambda a, b: a + b) \
               .sortBy(lambda x: x[1], ascending=False) \
               .first()[0]
q7 = rdd.filter(lambda row: row["IncidentGrade"] == top_grade) \
        .map(lambda row: row["AlertTitle"]) \
        .distinct()
q7.count()
print(f"Execution Time Q7: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q7: {max(0, get_memory_mb() - start_mem):.2f} MB")

print("Query 8: RDD API")
start_time = time.time()
start_mem = get_memory_mb()
cached_rdd = rdd.filter(lambda row: row["Usage"] == "Public").cache()
q8 = cached_rdd.map(lambda row: (row["Category"], 1)) \
               .reduceByKey(lambda a, b: a + b)
q8.count()
print(f"Execution Time Q8: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q8: {max(0, get_memory_mb() - start_mem):.2f} MB")

print("Query 9: RDD API")
start_time = time.time()
start_mem = get_memory_mb()
lookup_dict = {"TP": "True Positive", "FP": "False Positive"}
broadcast_lookup = sc.broadcast(lookup_dict)
q9 = rdd.map(lambda row: (row["IncidentId"], broadcast_lookup.value.get(row["IncidentGrade"], "Unknown")))
q9.count()
print(f"Execution Time Q9: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q9: {max(0, get_memory_mb() - start_mem):.2f} MB")

print("Query 10: RDD API")
start_time = time.time()
start_mem = get_memory_mb()
rdd_subset1 = rdd.filter(lambda row: row["Category"] == "Malware").map(lambda row: (row["IncidentId"], row["Category"]))
rdd_subset2 = rdd.filter(lambda row: row["Usage"] == "Public").map(lambda row: (row["IncidentId"], row["Usage"]))
q10 = rdd_subset1.join(rdd_subset2)
q10.count()
print(f"Execution Time Q10: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q10: {max(0, get_memory_mb() - start_mem):.2f} MB")

spark.stop()