import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("DataFrame_Queries") \
    .config("spark.sql.join.preferSortMergeJoin", "true") \
    .getOrCreate()

df = spark.read.csv("file:///home/hduser/mini_project_2/data/GUIDE_Test.csv", header=True, inferSchema=True)

print("Query 1: DataFrame API")
start = time.time()
q1 = df.filter(df["Category"] == "Malware").groupBy("IncidentGrade").count()
q1.write.csv("file:///home/hduser/mini_project_2/results/df_q1", mode="overwrite", header=True)
print(f"Execution Time Q1: {time.time() - start:.4f} seconds")
q1.explain(True)

print("Query 2: DataFrame API")
start = time.time()
q2 = df.groupBy("Category", "Usage").agg(F.count("*").alias("TotalCount"))
q2.write.csv("file:///home/hduser/mini_project_2/results/df_q2", mode="overwrite", header=True)
print(f"Execution Time Q2: {time.time() - start:.4f} seconds")
q2.explain(True)

print("Query 3: DataFrame API")
start = time.time()
q3 = df.filter((df["Usage"] == "Public") & (df["Category"] != "Test"))
q3.write.csv("file:///home/hduser/mini_project_2/results/df_q3", mode="overwrite", header=True)
print(f"Execution Time Q3: {time.time() - start:.4f} seconds")
q3.explain(True)

print("Query 4: DataFrame API")
start = time.time()
q4 = df.groupBy("AlertTitle").count().orderBy(F.desc("count"))
q4.write.csv("file:///home/hduser/mini_project_2/results/df_q4", mode="overwrite", header=True)
print(f"Execution Time Q4: {time.time() - start:.4f} seconds")
q4.explain(True)

print("Query 5: DataFrame API")
start = time.time()
window_spec = Window.partitionBy("Category").orderBy(F.desc("count"))
q5_base = df.groupBy("Category", "AlertTitle").count()
q5 = q5_base.withColumn("Rank", F.rank().over(window_spec))
q5.write.csv("file:///home/hduser/mini_project_2/results/df_q5", mode="overwrite", header=True)
print(f"Execution Time Q5: {time.time() - start:.4f} seconds")
q5.explain(True)

print("Query 6: DataFrame API")
start = time.time()
window_cumulative = Window.orderBy("Timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
q6_base = df.groupBy("Timestamp").count()
q6 = q6_base.withColumn("CumulativeCount", F.sum("count").over(window_cumulative))
q6.write.csv("file:///home/hduser/mini_project_2/results/df_q6", mode="overwrite", header=True)
print(f"Execution Time Q6: {time.time() - start:.4f} seconds")
q6.explain(True)

print("Query 7: DataFrame API")
start = time.time()
inner_query = df.groupBy("IncidentGrade").count().orderBy(F.desc("count")).limit(1)
top_grade = inner_query.collect()[0]["IncidentGrade"]
q7 = df.filter(df["IncidentGrade"] == top_grade).select("AlertTitle").distinct()
q7.write.csv("file:///home/hduser/mini_project_2/results/df_q7", mode="overwrite", header=True)
print(f"Execution Time Q7: {time.time() - start:.4f} seconds")
q7.explain(True)

print("Query 8: DataFrame API")
start = time.time()
cached_df = df.filter(df["Usage"] == "Public").cache()
q8 = cached_df.groupBy("Category").count()
q8.write.csv("file:///home/hduser/mini_project_2/results/df_q8", mode="overwrite", header=True)
print(f"Execution Time Q8: {time.time() - start:.4f} seconds")
q8.explain(True)

print("Query 9: DataFrame API")
start = time.time()
lookup_data = [("TP", "True Positive"), ("FP", "False Positive")]
lookup_df = spark.createDataFrame(lookup_data, ["IncidentGrade", "GradeDescription"])
q9 = df.join(F.broadcast(lookup_df), "IncidentGrade")
q9.write.csv("file:///home/hduser/mini_project_2/results/df_q9", mode="overwrite", header=True)
print(f"Execution Time Q9: {time.time() - start:.4f} seconds")
q9.explain(True)

print("Query 10: DataFrame API")
start = time.time()
df_subset1 = df.filter(df["Category"] == "Malware")
df_subset2 = df.filter(df["Usage"] == "Public")
q10 = df_subset1.join(df_subset2, "IncidentId")
q10.write.csv("file:///home/hduser/mini_project_2/results/df_q10", mode="overwrite", header=True)
print(f"Execution Time Q10: {time.time() - start:.4f} seconds")
q10.explain(True)

spark.stop()