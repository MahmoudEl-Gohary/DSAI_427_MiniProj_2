from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SQL_Queries") \
    .config("spark.sql.join.preferSortMergeJoin", "true") \
    .getOrCreate()

# Load Dataset and Register Temp View
df = spark.read.csv("data/GUIDE_Test.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("incidents")

# Create Lookup Table for Query 9
spark.sql("CREATE OR REPLACE TEMP VIEW lookup AS SELECT 'TP' as IncidentGrade, 'True Positive' as GradeDescription UNION SELECT 'FP', 'False Positive'")

# Query 1: Basic Filtering
print("Query 1: SQL API")
q1 = spark.sql("SELECT IncidentGrade, COUNT(*) as count FROM incidents WHERE Category = 'Malware' GROUP BY IncidentGrade")
q1.show()
q1.explain(True)

# Query 2: Multiple Grouping & Aggregations
print("Query 2: SQL API")
q2 = spark.sql("SELECT Category, Usage, COUNT(*) as TotalCount FROM incidents GROUP BY Category, Usage")
q2.show()
q2.explain(True)

# Query 3: Complex Filtering
print("Query 3: SQL API")
q3 = spark.sql("SELECT * FROM incidents WHERE Usage = 'Public' AND Category != 'Test'")
q3.show()
q3.explain(True)

# Query 4: Sorting and Ranking
print("Query 4: SQL API")
q4 = spark.sql("SELECT AlertTitle, COUNT(*) as count FROM incidents GROUP BY AlertTitle ORDER BY count DESC")
q4.show()
q4.explain(True)

# Query 5: Window Functions (Ranking)
print("Query 5: SQL API")
q5 = spark.sql("""
    SELECT Category, AlertTitle, count, 
           RANK() OVER (PARTITION BY Category ORDER BY count DESC) as Rank
    FROM (SELECT Category, AlertTitle, COUNT(*) as count FROM incidents GROUP BY Category, AlertTitle)
""")
q5.show()
q5.explain(True)

# Query 6: Window Functions (Cumulative Sum)
print("Query 6: SQL API")
q6 = spark.sql("""
    SELECT Timestamp, count, 
           SUM(count) OVER (ORDER BY Timestamp) as CumulativeCount
    FROM (SELECT Timestamp, COUNT(*) as count FROM incidents GROUP BY Timestamp)
""")
q6.show()
q6.explain(True)

# Query 7: Subquery
print("Query 7: SQL API")
q7 = spark.sql("""
    SELECT DISTINCT AlertTitle FROM incidents 
    WHERE IncidentGrade = (
        SELECT IncidentGrade FROM incidents GROUP BY IncidentGrade ORDER BY COUNT(*) DESC LIMIT 1
    )
""")
q7.show()
q7.explain(True)

# Query 8: Caching
print("Query 8: SQL API")
spark.sql("CACHE TABLE Public_Usage AS SELECT * FROM incidents WHERE Usage = 'Public'")
q8 = spark.sql("SELECT Category, COUNT(*) FROM Public_Usage GROUP BY Category")
q8.show()
q8.explain(True)

# Query 9: Broadcast Join
print("Query 9: SQL API")
q9 = spark.sql("SELECT /*+ BROADCAST(lookup) */ i.*, l.GradeDescription FROM incidents i JOIN lookup l ON i.IncidentGrade = l.IncidentGrade")
q9.show()
q9.explain(True)

# Query 10: Sort-Merge Join
print("Query 10: SQL API")
q10 = spark.sql("""
    SELECT /*+ MERGE(i1, i2) */ i1.IncidentId, i1.Category, i2.Usage 
    FROM incidents i1 
    JOIN incidents i2 ON i1.IncidentId = i2.IncidentId 
    WHERE i1.Category = 'Malware' AND i2.Usage = 'Public'
""")
q10.show()
q10.explain(True)

spark.stop()