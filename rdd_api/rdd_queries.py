from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RDD_Queries") \
    .getOrCreate()

sc = spark.sparkContext

df = spark.read.csv("file:///home/hduser/mini_project_2/data/GUIDE_Test.csv", header=True, inferSchema=True)
rdd = df.rdd.filter(lambda row: row is not None)

print("Query 1: RDD API")
q1 = rdd.filter(lambda row: row["Category"] == "Malware") \
        .map(lambda row: (row["IncidentGrade"], 1)) \
        .reduceByKey(lambda a, b: a + b)
print(q1.take(5))

print("Query 2: RDD API")
q2 = rdd.map(lambda row: ((row["Category"], row["Usage"]), 1)) \
        .reduceByKey(lambda a, b: a + b)
print(q2.take(5))

print("Query 3: RDD API")
q3 = rdd.filter(lambda row: row["Usage"] == "Public" and row["Category"] != "Test")
print(q3.take(5))

print("Query 4: RDD API")
q4 = rdd.map(lambda row: (row["AlertTitle"], 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1], ascending=False)
print(q4.take(5))

print("Query 5: RDD API")
q5 = rdd.map(lambda row: ((row["Category"], row["AlertTitle"]), 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
        .groupByKey() \
        .mapValues(lambda vals: sorted(list(vals), key=lambda x: x[1], reverse=True))
print(q5.take(2))

print("Query 6: RDD API")
q6_counts = rdd.map(lambda row: (row["Timestamp"], 1)) \
               .reduceByKey(lambda a, b: a + b) \
               .sortByKey() \
               .collect()

cumulative = []
total = 0
for time, count in q6_counts[:10]:
    total += count
    cumulative.append((time, total))
print(cumulative)

print("Query 7: RDD API")
top_grade = rdd.map(lambda row: (row["IncidentGrade"], 1)) \
               .reduceByKey(lambda a, b: a + b) \
               .sortBy(lambda x: x[1], ascending=False) \
               .first()[0]

q7 = rdd.filter(lambda row: row["IncidentGrade"] == top_grade) \
        .map(lambda row: row["AlertTitle"]) \
        .distinct()
print(q7.take(5))

print("Query 8: RDD API")
cached_rdd = rdd.filter(lambda row: row["Usage"] == "Public").cache()
q8 = cached_rdd.map(lambda row: (row["Category"], 1)) \
               .reduceByKey(lambda a, b: a + b)
print(q8.take(5))

print("Query 9: RDD API")
lookup_dict = {"TP": "True Positive", "FP": "False Positive"}
broadcast_lookup = sc.broadcast(lookup_dict)

q9 = rdd.map(lambda row: (row, broadcast_lookup.value.get(row["IncidentGrade"], "Unknown")))
print(q9.take(2))

print("Query 10: RDD API")
rdd_subset1 = rdd.filter(lambda row: row["Category"] == "Malware").map(lambda row: (row["IncidentId"], row))
rdd_subset2 = rdd.filter(lambda row: row["Usage"] == "Public").map(lambda row: (row["IncidentId"], row))
q10 = rdd_subset1.join(rdd_subset2)
print(q10.take(1))

spark.stop()