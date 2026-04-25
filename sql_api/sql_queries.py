import time
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SQL_Queries") \
    .config("spark.sql.join.preferSortMergeJoin", "true") \
    .getOrCreate()

df = spark.read.csv("file:///home/hduser/mini_project_2/data/GUIDE_Test.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("incidents")
spark.sql("CREATE OR REPLACE TEMP VIEW lookup AS SELECT 'TP' as IncidentGrade, 'True Positive' as GradeDescription UNION SELECT 'FP', 'False Positive'")

print("Query 1: SQL API")
start = time.time()
q1 = spark.sql("SELECT IncidentGrade, COUNT(*) as count FROM incidents WHERE Category = 'Malware' GROUP BY IncidentGrade")
q1.write.csv("file:///home/hduser/mini_project_2/results/sql_q1", mode="overwrite", header=True)
print(f"Execution Time Q1: {time.time() - start:.4f} seconds")
q1.explain(True)

print("Query 2: SQL API")
start = time.time()
q2 = spark.sql("SELECT Category, Usage, COUNT(*) as TotalCount FROM incidents GROUP BY Category, Usage")
q2.write.csv("file:///home/hduser/mini_project_2/results/sql_q2", mode="overwrite", header=True)
print(f"Execution Time Q2: {time.time() - start:.4f} seconds")
q2.explain(True)

print("Query 3: SQL API")
start = time.time()
q3 = spark.sql("SELECT * FROM incidents WHERE Usage = 'Public' AND Category != 'Test'")
q3.write.csv("file:///home/hduser/mini_project_2/results/sql_q3", mode="overwrite", header=True)
print(f"Execution Time Q3: {time.time() - start:.4f} seconds")
q3.explain(True)

print("Query 4: SQL API")
start = time.time()
q4 = spark.sql("SELECT AlertTitle, COUNT(*) as count FROM incidents GROUP BY AlertTitle ORDER BY count DESC")
q4.write.csv("file:///home/hduser/mini_project_2/results/sql_q4", mode="overwrite", header=True)
print(f"Execution Time Q4: {time.time() - start:.4f} seconds")
q4.explain(True)

print("Query 5: SQL API")
start = time.time()
q5 = spark.sql("""
    SELECT Category, AlertTitle, count, 
           RANK() OVER (PARTITION BY Category ORDER BY count DESC) as Rank
    FROM (SELECT Category, AlertTitle, COUNT(*) as count FROM incidents GROUP BY Category, AlertTitle)
""")
q5.write.csv("file:///home/hduser/mini_project_2/results/sql_q5", mode="overwrite", header=True)
print(f"Execution Time Q5: {time.time() - start:.4f} seconds")
q5.explain(True)

print("Query 6: SQL API")
start = time.time()
q6 = spark.sql("""
    SELECT Timestamp, count, 
           SUM(count) OVER (ORDER BY Timestamp) as CumulativeCount
    FROM (SELECT Timestamp, COUNT(*) as count FROM incidents GROUP BY Timestamp)
""")
q6.write.csv("file:///home/hduser/mini_project_2/results/sql_q6", mode="overwrite", header=True)
print(f"Execution Time Q6: {time.time() - start:.4f} seconds")
q6.explain(True)

print("Query 7: SQL API")
start = time.time()
q7 = spark.sql("""
    SELECT DISTINCT AlertTitle FROM incidents 
    WHERE IncidentGrade = (
        SELECT IncidentGrade FROM incidents GROUP BY IncidentGrade ORDER BY COUNT(*) DESC LIMIT 1
    )
""")
q7.write.csv("file:///home/hduser/mini_project_2/results/sql_q7", mode="overwrite", header=True)
print(f"Execution Time Q7: {time.time() - start:.4f} seconds")
q7.explain(True)

print("Query 8: SQL API")
start = time.time()
spark.sql("CACHE TABLE public_usage AS SELECT * FROM incidents WHERE Usage = 'Public'")
q8 = spark.sql("SELECT Category, COUNT(*) FROM public_usage GROUP BY Category")
q8.write.csv("file:///home/hduser/mini_project_2/results/sql_q8", mode="overwrite", header=True)
print(f"Execution Time Q8: {time.time() - start:.4f} seconds")
q8.explain(True)

print("Query 9: SQL API")
start = time.time()
q9 = spark.sql("SELECT /*+ BROADCAST(lookup) */ i.*, l.GradeDescription FROM incidents i JOIN lookup l ON i.IncidentGrade = l.IncidentGrade")
q9.write.csv("file:///home/hduser/mini_project_2/results/sql_q9", mode="overwrite", header=True)
print(f"Execution Time Q9: {time.time() - start:.4f} seconds")
q9.explain(True)

print("Query 10: SQL API")
start = time.time()
q10 = spark.sql("""
    SELECT /*+ MERGE(i1, i2) */ i1.IncidentId, i1.Category, i2.Usage 
    FROM incidents i1 
    JOIN incidents i2 ON i1.IncidentId = i2.IncidentId 
    WHERE i1.Category = 'Malware' AND i2.Usage = 'Public'
""")
q10.write.csv("file:///home/hduser/mini_project_2/results/sql_q10", mode="overwrite", header=True)
print(f"Execution Time Q10: {time.time() - start:.4f} seconds")
q10.explain(True)

spark.stop()