# DSAI 427 Mini Project 2

Performance and scalability comparison of Apache Spark query implementations using:

- Spark DataFrame API
- Spark SQL API
- Spark RDD API

The project runs 10 equivalent analytics queries on the same cybersecurity incident dataset and compares execution time, memory impact, and query execution plans.

## Project Goals

- Implement identical analytical workloads with DataFrame, SQL, and RDD approaches.
- Compare performance and memory behavior across APIs.
- Validate optimization techniques such as caching, broadcast joins, and merge/sort-merge joins.
- Measure scalability under different core allocations.

## Repository Structure

```text
.
|-- dataframe_api/
|   `-- df_queries.py
|-- rdd_api/
|   `-- rdd_queries.py
|-- sql_api/
|   `-- sql_queries.py
|-- scripts/
|   |-- cluster_config.sh
|   `-- scalability_test.sh
|-- results/
|   |-- df_metrics.txt
|   |-- rdd_metrics.txt
|   `-- sql_metrics.txt
`-- README.md
```

## Query Workload (10 Queries)

1. Filter + GroupBy: malware incidents by incident grade.
2. Multi-column GroupBy: count by category and usage.
3. Conditional filter: public usage where category is not test.
4. Aggregation + sorting: most frequent alert titles.
5. Window function ranking: rank alert titles per category.
6. Cumulative analytics: running total by timestamp.
7. Nested query pattern: distinct titles for top incident grade.
8. Caching optimization: cache public subset then aggregate.
9. Broadcast join: join with a small lookup table.
10. Merge/sort-merge style join: filtered self-join on incident id.

## Tech Stack

- Python (PySpark)
- Apache Spark
- Spark SQL
- Bash scripts for cluster/scalability runs

## Prerequisites

- Java 8 or 11
- Apache Spark 3.x with `spark-submit` on `PATH`
- Python 3.8+
- Linux/WSL shell for `.sh` scripts

## Dataset Setup

The current scripts load the dataset from:

```text
file:///home/hduser/mini_project_2/data/GUIDE_Test.csv
```

Before running, choose one of these options:

1. Place `GUIDE_Test.csv` at the exact path above.
2. Edit the dataset path in these files:
	- `dataframe_api/df_queries.py`
	- `rdd_api/rdd_queries.py`
	- `sql_api/sql_queries.py`

## How To Run

From the project root:

### 1) DataFrame API

```bash
spark-submit dataframe_api/df_queries.py > results/df_metrics.txt
```

### 2) RDD API

```bash
spark-submit rdd_api/rdd_queries.py > results/rdd_metrics.txt
```

### 3) SQL API

```bash
spark-submit sql_api/sql_queries.py > results/sql_metrics.txt
```

### 4) Optional: Apply Spark config script

```bash
source scripts/cluster_config.sh
```

### 5) Optional: Scalability test (1, 2, 4 cores)

```bash
bash scripts/scalability_test.sh
```

This generates:

- `results/scalability_1core_metrics.txt`
- `results/scalability_2core_metrics.txt`
- `results/scalability_4core_metrics.txt`

## Current Results Snapshot

Based on the checked-in metrics files:

| API | Avg Query Time (Q1-Q10) | Fastest Query | Slowest Query |
|---|---:|---:|---:|
| DataFrame | ~7.27 s | Q3: 3.51 s | Q8: 24.96 s |
| SQL | ~7.36 s | Q9: 3.31 s | Q8: 25.52 s |
| RDD | ~52.26 s | Q3: 35.49 s | Q10: 118.57 s |

### Observations

- DataFrame and SQL are very close in average runtime.
- RDD implementation is substantially slower for this workload (about 7x slower on average).
- Query 8 is heavy in both DataFrame and SQL because first-time cache materialization is expensive.
- Query 10 is especially costly in RDD because join-heavy transformations amplify shuffle overhead.

## Optimization Features Included

- DataFrame + SQL execution plan inspection with `explain(True)`.
- Broadcast join for small lookup data (Q9).
- Merge/sort-merge join hints in SQL (Q10).
- Caching of frequently reused filtered subset (Q8).
- Basic memory impact reporting via JVM runtime counters.

## Notes and Limitations

- Memory impact values are approximate process-level deltas and may vary across runs.
- Runtime depends on hardware, Spark version, and local system load.
- Because path values are hardcoded, portability requires dataset path updates.

## Troubleshooting

- If `spark-submit` is not found, verify Spark installation and `PATH`.
- If dataset load fails, confirm the CSV path in all three query files.
- If running on Windows, use WSL/Git Bash for `.sh` scripts.
- If Java errors appear, verify `JAVA_HOME` and Java compatibility with your Spark version.

## Suggested Next Improvements

- Parameterize dataset path using command-line args or environment variables.
- Add automated parsing of metrics into a consolidated comparison table/plot.
- Add repeated runs and report mean/stddev for statistically stronger comparisons.
- Persist optimized/cached data in Parquet to benchmark I/O impact vs CSV.
