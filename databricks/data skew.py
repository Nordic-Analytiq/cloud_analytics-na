# Databricks notebook source
# MAGIC %md
# MAGIC _**Data skew**_ occurs when data is unevenly distributed across partitions or compute nodes during distributed processing, causing one or a few worker tasks to handle the vast majority of the data while other nodes sit idle.
# MAGIC
# MAGIC _**How Databricks Handles Skew Automatically**_
# MAGIC - Adaptive Query Execution (AQE): Enabled by default in modern Databricks Runtimes (spark.sql.adaptive.skewJoin.enabled = true). AQE automatically detects skewed partitions during joins at runtime and splits them into smaller sub-tasks without requiring manual salting code.
# MAGIC - Liquid Clustering (CLUSTER BY): When storing tables in Delta Lake or Managed Iceberg, Liquid Clustering avoids skew on disk by sizing Parquet files evenly regardless of how skewed the underlying column values are.

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Create a heavily skewed dataset (1,000,000 rows)
# 95% of rows belong to 'STOCKHOLM', while other cities have very few rows
df_skewed = (
    spark.range(0, 1_000_000)
    .withColumn(
        "city",
        F.when(F.col("id") < 950_000, F.lit("STOCKHOLM"))
         .when(F.col("id") < 980_000, F.lit("GOTHENBURG"))
         .otherwise(F.lit("MALMO"))
    )
    .withColumn("amount", (F.rand() * 100).cast("int"))
)
# 2. Repartition by 'city' to simulate a shuffle (like a groupBy or join)
df_partitioned = df_skewed.repartition(4, "city")

# 3. Inspect row counts per Spark partition
print("--- PARTITION DISTRIBUTION (SKEWED) ---")
df_partitioned.withColumn("partition_id", F.spark_partition_id()) \
    .groupBy("partition_id", "city") \
    .count() \
    .orderBy("partition_id") \
    .show()

# ---------------------------------------------------------
# 4. HOW TO FIX IT: Key Salting (For Joins / Aggregations)
# ---------------------------------------------------------
# Append a random salt (0 to 9) to split 'STOCKHOLM' across 10 sub-keys
SALT_BUCKETS = 10

df_salted = df_skewed.withColumn(
    "salted_city",
    F.concat(F.col("city"), F.lit("_"), (F.rand() * SALT_BUCKETS).cast("int"))
)
print("--- PARTITION DISTRIBUTION (AFTER SALTING) ---")
df_salted.repartition(4, "salted_city") \
    .withColumn("partition_id", F.spark_partition_id()) \
    .groupBy("partition_id") \
    .count() \
    .orderBy("partition_id") \
    .show()

df_skewed.show(5)
df_partitioned.show(5)
df_salted.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Solving Skew with Salting in PySpark

# COMMAND ----------

from pyspark.sql import functions as F

SALT_BUCKETS = 10

# 1. Create a large skewed Fact Table (1,000,000 rows, 95% 'STOCKHOLM')
df_orders = (
    spark.range(0, 1_000_000)
    .withColumn(
        "city",
        F.when(F.col("id") < 950_000, F.lit("STOCKHOLM"))
         .when(F.col("id") < 980_000, F.lit("GOTHENBURG"))
         .otherwise(F.lit("MALMO"))
    )
    .withColumn("amount", (F.rand() * 100).cast("int"))
)
print("df_orders")
df_orders.show(5)
# 2. Create a small Dimension Table (3 rows)
df_cities = spark.createDataFrame(
    [("STOCKHOLM", "SE-AB"), ("GOTHENBURG", "SE-O"), ("MALMO", "SE-M")],
    ["city", "region_code"]
)
print("df_cities")
df_cities.show(5)
# ---------------------------------------------------------
# A. TWO-STEP SALTED AGGREGATION (Without Dimension Join)
# ---------------------------------------------------------
# Step 1: Add random salt (0-9) and aggregate in parallel across 10 buckets
df_partial_agg = (
    df_orders
    .withColumn("salt", (F.rand() * SALT_BUCKETS).cast("int"))
    .groupBy("city", "salt")
    .agg(F.sum("amount").alias("partial_sum"), F.count("*").alias("partial_cnt"))
)
print("df_partial_agg")
df_partial_agg.show(5)
# Step 2: Remove the salt and sum the 10 pre-aggregated results per city
df_final_agg = (
    df_partial_agg
    .groupBy("city")
    .agg(F.sum("partial_sum").alias("total_amount"), F.sum("partial_cnt").alias("total_orders"))
)

print("--- TWO-STEP SALTED AGGREGATION RESULT ---")
df_final_agg.show()

# ---------------------------------------------------------
# B. SALTED JOIN (Fact Table + Exploded Dimension Table)
# ---------------------------------------------------------
# Step 1: Assign a random salt (0 to 9) to each row in the large skewed table
df_orders_salted = df_orders.withColumn(
    "salted_city",
    F.concat(F.col("city"), F.lit("_"), (F.rand() * SALT_BUCKETS).cast("int"))
)
print("df_orders_salted")
df_orders_salted.show(5)
# Step 2: Replicate (explode) each row in the small table across all 10 salt values
# Using a literal list [0, 1, ..., 9] creates all matching keys (e.g., STOCKHOLM_0 ... STOCKHOLM_9)
salt_array = F.array([F.lit(i) for i in range(SALT_BUCKETS)])

df_cities_exploded = (
    df_cities
    .withColumn("salt", F.explode(salt_array))
    .withColumn("salted_city", F.concat(F.col("city"), F.lit("_"), F.col("salt")))
    .drop("city", "salt")
)
print("df_cities_exploded")
df_cities_exploded.show()
# Step 3: Join on 'salted_city' — all 10 partitions now share the workload evenly
df_joined = df_orders_salted.join(df_cities_exploded, on="salted_city", how="inner")

print("--- SALTED JOIN SAMPLE OUTPUT ---")
df_joined.select("id", "city", "region_code", "amount").show(5)
