import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("DataFrame_Queries") \
    .config("spark.sql.join.preferSortMergeJoin", "true") \
    .getOrCreate()

def get_memory_mb():
    runtime = spark.sparkContext._jvm.java.lang.Runtime.getRuntime()
    return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)

df = spark.read.csv("file:///home/hduser/mini_project_2/data/GUIDE_Test.csv", header=True, inferSchema=True)

print("Query 1: DataFrame API")
start_time = time.time()
start_mem = get_memory_mb()
q1 = df.filter(df["Category"] == "Malware").groupBy("IncidentGrade").count()
q1.count()
print(f"Execution Time Q1: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q1: {max(0, get_memory_mb() - start_mem):.2f} MB")
q1.explain(True)

print("Query 2: DataFrame API")
start_time = time.time()
start_mem = get_memory_mb()
q2 = df.groupBy("Category", "Usage").agg(F.count("*").alias("TotalCount"))
q2.count()
print(f"Execution Time Q2: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q2: {max(0, get_memory_mb() - start_mem):.2f} MB")
q2.explain(True)

print("Query 3: DataFrame API")
start_time = time.time()
start_mem = get_memory_mb()
q3 = df.filter((df["Usage"] == "Public") & (df["Category"] != "Test"))
q3.count()
print(f"Execution Time Q3: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q3: {max(0, get_memory_mb() - start_mem):.2f} MB")
q3.explain(True)

print("Query 4: DataFrame API")
start_time = time.time()
start_mem = get_memory_mb()
q4 = df.groupBy("AlertTitle").count().orderBy(F.desc("count"))
q4.count()
print(f"Execution Time Q4: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q4: {max(0, get_memory_mb() - start_mem):.2f} MB")
q4.explain(True)

print("Query 5: DataFrame API")
start_time = time.time()
start_mem = get_memory_mb()
window_spec = Window.partitionBy("Category").orderBy(F.desc("count"))
q5_base = df.groupBy("Category", "AlertTitle").count()
q5 = q5_base.withColumn("Rank", F.rank().over(window_spec))
q5.count()
print(f"Execution Time Q5: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q5: {max(0, get_memory_mb() - start_mem):.2f} MB")
q5.explain(True)

print("Query 6: DataFrame API")
start_time = time.time()
start_mem = get_memory_mb()
window_cumulative = Window.orderBy("Timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
q6_base = df.groupBy("Timestamp").count()
q6 = q6_base.withColumn("CumulativeCount", F.sum("count").over(window_cumulative))
q6.count()
print(f"Execution Time Q6: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q6: {max(0, get_memory_mb() - start_mem):.2f} MB")
q6.explain(True)

print("Query 7: DataFrame API")
start_time = time.time()
start_mem = get_memory_mb()
inner_query = df.groupBy("IncidentGrade").count().orderBy(F.desc("count")).limit(1)
top_grade = inner_query.collect()[0]["IncidentGrade"]
q7 = df.filter(df["IncidentGrade"] == top_grade).select("AlertTitle").distinct()
q7.count()
print(f"Execution Time Q7: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q7: {max(0, get_memory_mb() - start_mem):.2f} MB")
q7.explain(True)

print("Query 8: DataFrame API")
start_time = time.time()
start_mem = get_memory_mb()
cached_df = df.filter(df["Usage"] == "Public").cache()
q8 = cached_df.groupBy("Category").count()
q8.count()
print(f"Execution Time Q8: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q8: {max(0, get_memory_mb() - start_mem):.2f} MB")
q8.explain(True)

print("Query 9: DataFrame API")
start_time = time.time()
start_mem = get_memory_mb()
lookup_data = [("TP", "True Positive"), ("FP", "False Positive")]
lookup_df = spark.createDataFrame(lookup_data, ["IncidentGrade", "GradeDescription"])
q9 = df.join(F.broadcast(lookup_df), "IncidentGrade")
q9.count()
print(f"Execution Time Q9: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q9: {max(0, get_memory_mb() - start_mem):.2f} MB")
q9.explain(True)

print("Query 10: DataFrame API")
start_time = time.time()
start_mem = get_memory_mb()
df_subset1 = df.filter(df["Category"] == "Malware").select("IncidentId", "Category")
df_subset2 = df.filter(df["Usage"] == "Public").select("IncidentId", "Usage")
q10 = df_subset1.join(df_subset2, "IncidentId")
q10.count()
print(f"Execution Time Q10: {time.time() - start_time:.4f} seconds")
print(f"Memory Impact Q10: {max(0, get_memory_mb() - start_mem):.2f} MB")
q10.explain(True)

spark.stop()