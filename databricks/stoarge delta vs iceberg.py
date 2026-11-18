# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS format_demo_catalog
# MAGIC COMMENT 'Catalog for testing Delta Lake and Apache Iceberg';

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Setup sample data
data = [(1, "2026-11-18", "Stockholm"), (2, "2026-11-18", "Gothenburg")]
df = spark.createDataFrame(data, ["id", "event_date", "city"]).withColumn(
    "event_date", F.to_date("event_date")
)

spark.sql("CREATE SCHEMA IF NOT EXISTS format_demo_catalog.format_demo")

# ---------------------------------------------------------
# 2. CREATE TABLES (Delta Lake vs. Managed Apache Iceberg)
# ---------------------------------------------------------

# Delta Lake: Default format in Databricks, supports Liquid Clustering
spark.sql("""
    CREATE OR REPLACE TABLE format_demo_catalog.format_demo.events_delta (
        id INT,
        event_date DATE,
        city STRING
    )
    USING DELTA
    CLUSTER BY (city)
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
""")

# Apache Iceberg: Native managed Iceberg table (partition transforms like
# months() aren't supported for managed Iceberg tables, so partition by column)
spark.sql("""
    CREATE OR REPLACE TABLE format_demo_catalog.format_demo.events_iceberg (
        id INT,
        event_date DATE,
        city STRING
    )
    USING ICEBERG
    PARTITIONED BY (event_date)
""")

df.write.mode("append").saveAsTable("format_demo_catalog.format_demo.events_delta")

# Managed Iceberg tables don't support DataFrame (Delta) writes - use SQL INSERT
df.createOrReplaceTempView("_tmp_events")
spark.sql("INSERT INTO format_demo_catalog.format_demo.events_iceberg SELECT * FROM _tmp_events")

# ---------------------------------------------------------
# 3. DIFFERENTIATE METADATA & STORAGE ARCHITECTURE
# ---------------------------------------------------------

# Inspect table properties and storage locations
delta_detail = spark.sql("DESCRIBE EXTENDED format_demo_catalog.format_demo.events_delta")
iceberg_detail = spark.sql("DESCRIBE EXTENDED format_demo_catalog.format_demo.events_iceberg")

print("--- DELTA TABLE DETAILS ---")
delta_detail.filter("col_name IN ('Provider', 'Location', 'Clustering Information')").show(truncate=False)

print("--- ICEBERG TABLE DETAILS ---")
iceberg_detail.filter("col_name IN ('Provider', 'Location', 'Partition Information')").show(truncate=False)

# ---------------------------------------------------------
# 4. DIFFERENTIATE VERSION HISTORY & TIME TRAVEL
# ---------------------------------------------------------

# Delta uses transaction log versions (VERSION AS OF)
print("--- DELTA HISTORY (Transaction Log Versions) ---")
spark.sql("DESCRIBE HISTORY format_demo_catalog.format_demo.events_delta").select(
    "version", "timestamp", "operation"
).show(truncate=False)

# Iceberg uses snapshot IDs (the .snapshots metadata view isn't available for
# managed Iceberg tables in Databricks; DESCRIBE HISTORY shows the snapshot commits)
print("--- ICEBERG HISTORY (Snapshot Commits) ---")
spark.sql("DESCRIBE HISTORY format_demo_catalog.format_demo.events_iceberg").select(
    "version", "timestamp", "operation"
).show(truncate=False)

# ---------------------------------------------------------
# 5. BRIDGE BOTH FORMATS WITH DELTA UNIFORM
# ---------------------------------------------------------
# Enable Iceberg reads on a Delta table without duplicating Parquet files
spark.sql("""
    ALTER TABLE format_demo_catalog.format_demo.events_delta SET TBLPROPERTIES (
        'delta.enableIcebergCompatV2' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.universalFormat.enabledFormats' = 'iceberg'
    )
""")


# COMMAND ----------

# MAGIC %md
# MAGIC |**Feature**|**Delta Lake (USING DELTA)**|**Apache Iceberg (USING ICEBERG)**|
# MAGIC |:---|:---|:---|
# MAGIC |**Metadata Layer**|Tracks state via sequential JSON commit files and Parquet checkpoints inside a _delta_log folder.|Tracks state via a hierarchical tree of metadata.json files pointing to Avro manifest lists and manifest files.|
# MAGIC |**Data Layout**|Uses Liquid Clustering (CLUSTER BY) to dynamically organize data without rigid directory structures.|Uses Hidden Partitioning (PARTITIONED BY col) so queries filter automatically without needing partition columns.
# MAGIC |**History Inspection**|Queried via DESCRIBE HISTORY table_name, returning incremental integer version numbers.	|Queried via metadata tables (table_name.snapshots), returning 64-bit snapshot_id values and manifest_list paths.|
# MAGIC |**Interoperability**|Native to Databricks; can expose Iceberg metadata asynchronously using Databricks UniForm.|Open specification managed by Unity Catalog, readable/writable directly by external engines via Iceberg REST Catalog.|
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Use your newly created catalog (or change to your active catalog name)
target_catalog = "format_demo_catalog"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.format_demo")

# 2. Prepare sample data
data = [
    (1, "2026-11-18", "Stockholm"),
    (2, "2026-11-18", "Gothenburg"),
    (3, "2026-12-01", "Malmo")
]
df = spark.createDataFrame(data, ["id", "event_date", "city"]).withColumn(
    "event_date", F.to_date("event_date")
)

# ---------------------------------------------------------
# 3. DELTA LAKE APPROACH: Liquid Clustering (No Partition Expressions)
# ---------------------------------------------------------
spark.sql(f"""
    CREATE OR REPLACE TABLE {target_catalog}.format_demo.events_delta (
        id INT,
        event_date DATE,
        city STRING
    )
    USING DELTA
    CLUSTER BY (city, event_date)
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
""")

df.write.mode("append").saveAsTable(f"{target_catalog}.format_demo.events_delta")

# ---------------------------------------------------------
# 4. GENERATE APACHE ICEBERG METADATA VIA UNIFORM
# ---------------------------------------------------------
# Instead of a separate Iceberg DDL, UniForm writes Iceberg metadata.json 
# and Avro manifests alongside the _delta_log for external Iceberg readers.
spark.sql(f"""
    ALTER TABLE {target_catalog}.format_demo.events_delta SET TBLPROPERTIES (
        'delta.enableIcebergCompatV2' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.universalFormat.enabledFormats' = 'iceberg'
    )
""")

# ---------------------------------------------------------
# 5. INSPECT & DIFFERENTIATE BOTH METADATA LAYERS
# ---------------------------------------------------------
print("--- 1. DELTA TRANSACTION LOG HISTORY ---")
spark.sql(f"DESCRIBE HISTORY {target_catalog}.format_demo.events_delta").select(
    "version", "timestamp", "operation"
).show(truncate=False)

print("--- 2. TABLE METADATA & ICEBERG UNIFORM STATUS ---")
# Look for 'delta.universalFormat.enabledFormats' = 'iceberg' 
# and 'Clustering Information' in the output
spark.sql(f"DESCRIBE EXTENDED {target_catalog}.format_demo.events_delta").filter(
    "col_name IN ('Provider', 'Location', 'Table Properties', 'Clustering Information')"
).show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Apache Iceberg partition evolution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. MANAGED ICEBERG: Must explicitly disable Deletion Vectors & Row Tracking for Liquid Clustering
# MAGIC CREATE OR REPLACE TABLE format_demo_catalog.format_demo.iceberg_evolution (
# MAGIC     id INT,
# MAGIC     event_date DATE,
# MAGIC     city STRING
# MAGIC )
# MAGIC USING ICEBERG
# MAGIC CLUSTER BY (event_date)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableDeletionVectors' = 'false',
# MAGIC     'delta.enableRowTracking' = 'false'
# MAGIC );
# MAGIC
# MAGIC INSERT INTO format_demo_catalog.format_demo.iceberg_evolution 
# MAGIC VALUES (1, DATE '2025-05-10', 'Stockholm');
# MAGIC
# MAGIC -- Evolve clustering keys on Iceberg
# MAGIC ALTER TABLE format_demo_catalog.format_demo.iceberg_evolution 
# MAGIC CLUSTER BY (city, event_date);
# MAGIC
# MAGIC INSERT INTO format_demo_catalog.format_demo.iceberg_evolution 
# MAGIC VALUES (2, DATE '2026-11-18', 'Gothenburg');
# MAGIC
# MAGIC
# MAGIC -- 2. DELTA LAKE: Supports Deletion Vectors & Row Tracking natively with Liquid Clustering
# MAGIC CREATE OR REPLACE TABLE format_demo_catalog.format_demo.delta_evolution (
# MAGIC     id INT,
# MAGIC     event_date DATE,
# MAGIC     city STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (event_date);
# MAGIC
# MAGIC INSERT INTO format_demo_catalog.format_demo.delta_evolution 
# MAGIC VALUES (1, DATE '2025-05-10', 'Stockholm');
# MAGIC
# MAGIC -- Evolve clustering keys on Delta
# MAGIC ALTER TABLE format_demo_catalog.format_demo.delta_evolution 
# MAGIC CLUSTER BY (city, event_date);
# MAGIC
# MAGIC INSERT INTO format_demo_catalog.format_demo.delta_evolution 
# MAGIC VALUES (2, DATE '2026-11-18', 'Gothenburg');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Lake: Liquid Clustering Updates

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Create Delta table with Liquid Clustering on event_date
# MAGIC CREATE TABLE format_demo_catalog.format_demo.delta_liquid (
# MAGIC     id INT,
# MAGIC     event_date DATE,
# MAGIC     city STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (event_date);
# MAGIC
# MAGIC INSERT INTO format_demo_catalog.format_demo.delta_liquid 
# MAGIC VALUES (1, DATE '2025-05-10', 'Stockholm');
# MAGIC
# MAGIC -- Step 2: Change clustering keys to prioritize 'city' (Metadata-only operation!)
# MAGIC ALTER TABLE format_demo_catalog.format_demo.delta_liquid 
# MAGIC CLUSTER BY (city, event_date);
# MAGIC
# MAGIC -- Insert new data (Clustered by city + event_date)
# MAGIC INSERT INTO format_demo_catalog.format_demo.delta_liquid 
# MAGIC VALUES (2, DATE '2026-11-18', 'Gothenburg');
# MAGIC
# MAGIC -- Step 3: Incrementally re-cluster existing data when convenient
# MAGIC -- OPTIMIZE only clusters unclustered files; use OPTIMIZE FULL to re-cluster everything
# MAGIC OPTIMIZE format_demo_catalog.format_demo.delta_liquid;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Trigger reclustering on existing files (Requires DBR 16.4 LTS+)
# MAGIC OPTIMIZE format_demo_catalog.format_demo.iceberg_evolution FULL;
# MAGIC
# MAGIC -- 4. Verify the table provider and clustering keys
# MAGIC DESCRIBE EXTENDED format_demo_catalog.format_demo.iceberg_evolution;