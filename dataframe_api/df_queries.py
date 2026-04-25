from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("DataFrame_Queries") \
    .config("spark.sql.join.preferSortMergeJoin", "true") \
    .getOrCreate()

df = spark.read.csv("file:///home/hduser/mini_project_2/data/GUIDE_Test.csv", header=True, inferSchema=True)

print("Query 1: DataFrame API")
q1 = df.filter(df["Category"] == "Malware").groupBy("IncidentGrade").count()
q1.show()
q1.explain(True)

print("Query 2: DataFrame API")
q2 = df.groupBy("Category", "Usage").agg(F.count("*").alias("TotalCount"))
q2.show()
q2.explain(True)

print("Query 3: DataFrame API")
q3 = df.filter((df["Usage"] == "Public") & (df["Category"] != "Test"))
q3.show()
q3.explain(True)

print("Query 4: DataFrame API")
q4 = df.groupBy("AlertTitle").count().orderBy(F.desc("count"))
q4.show()
q4.explain(True)

print("Query 5: DataFrame API")
window_spec = Window.partitionBy("Category").orderBy(F.desc("count"))
q5_base = df.groupBy("Category", "AlertTitle").count()
q5 = q5_base.withColumn("Rank", F.rank().over(window_spec))
q5.show()
q5.explain(True)

print("Query 6: DataFrame API")
window_cumulative = Window.orderBy("Timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
q6_base = df.groupBy("Timestamp").count()
q6 = q6_base.withColumn("CumulativeCount", F.sum("count").over(window_cumulative))
q6.show()
q6.explain(True)

print("Query 7: DataFrame API")
inner_query = df.groupBy("IncidentGrade").count().orderBy(F.desc("count")).limit(1)
top_grade = inner_query.collect()[0]["IncidentGrade"]
q7 = df.filter(df["IncidentGrade"] == top_grade).select("AlertTitle").distinct()
q7.show()
q7.explain(True)

print("Query 8: DataFrame API")
cached_df = df.filter(df["Usage"] == "Public").cache()
q8 = cached_df.groupBy("Category").count()
q8.show()
q8.explain(True)

print("Query 9: DataFrame API")
lookup_data = [("TP", "True Positive"), ("FP", "False Positive")]
lookup_df = spark.createDataFrame(lookup_data, ["IncidentGrade", "GradeDescription"])
q9 = df.join(F.broadcast(lookup_df), "IncidentGrade")
q9.show()
q9.explain(True)

print("Query 10: DataFrame API")
df_subset1 = df.filter(df["Category"] == "Malware")
df_subset2 = df.filter(df["Usage"] == "Public")
q10 = df_subset1.join(df_subset2, "IncidentId")
q10.show()
q10.explain(True)

spark.stop()