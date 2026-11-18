# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,dbt overview
# MAGIC %md
# MAGIC # Sample dbt Project (Databricks)
# MAGIC
# MAGIC dbt turns SQL `SELECT` statements into production tables/views with testing, documentation, and dependency management. On Databricks, run it with the **dbt-databricks** adapter (`pip install dbt-databricks`) against a SQL warehouse or serverless compute.
# MAGIC
# MAGIC **Typical project layout**
# MAGIC ``` 
# MAGIC analytics_demos/  
# MAGIC ├── dbt_project.yml          # project config
# MAGIC ├── profiles.yml             # connection (usually ~/.dbt/)
# MAGIC ├── models/
# MAGIC │   ├── staging/
# MAGIC │   │   ├── sources.yml      # raw source tables
# MAGIC │   │   ├── stg_events.sql
# MAGIC │   │   └── schema.yml       # tests & docs
# MAGIC │   └── marts/
# MAGIC │       └── fct_events_by_city.sql
# MAGIC └── macros/                  # reusable Jinja SQL
# MAGIC ```
# MAGIC
# MAGIC The cells below contain each file's contents — they are **project files, not notebook cells to execute**. This example builds on the tables in `format_demo_catalog.format_demo` from the Delta/Iceberg demo notebook.

# COMMAND ----------

# DBTITLE 1,dbt_project.yml
# MAGIC %md
# MAGIC ### `dbt_project.yml`
# MAGIC ```yaml
# MAGIC name: analytics_demos
# MAGIC version: "1.0.0"
# MAGIC profile: databricks_demo   # matches a profile in profiles.yml
# MAGIC
# MAGIC models:
# MAGIC   analytics_demos:
# MAGIC     staging:
# MAGIC       +materialized: view
# MAGIC       +schema: staging
# MAGIC     marts:
# MAGIC       +materialized: table
# MAGIC       +schema: marts
# MAGIC ```
# MAGIC
# MAGIC ### `profiles.yml` (connection to Databricks)
# MAGIC ```yaml
# MAGIC databricks_demo:
# MAGIC   target: dev
# MAGIC   outputs:
# MAGIC     dev:
# MAGIC       type: databricks
# MAGIC       catalog: format_demo_catalog
# MAGIC       schema: format_demo
# MAGIC       host: <your-workspace>.cloud.databricks.com
# MAGIC       http_path: /sql/1.0/warehouses/<warehouse-id>
# MAGIC       token: {{ env_var('DBT_DATABRICKS_TOKEN') }}   # never hardcode tokens
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,sources.yml
# MAGIC %md
# MAGIC ### `models/staging/sources.yml` — point dbt at raw tables
# MAGIC ```yaml
# MAGIC version: 2
# MAGIC
# MAGIC sources:
# MAGIC   - name: format_demo
# MAGIC     database: format_demo_catalog
# MAGIC     schema: format_demo
# MAGIC     tables:
# MAGIC       - name: events_delta
# MAGIC         description: "Raw events from the Delta Lake demo table"
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,stg_events.sql
# MAGIC %sql
# MAGIC -- models/staging/stg_events.sql
# MAGIC -- Staging model: clean + rename the raw source.
# MAGIC -- {{ source() }} resolves to format_demo_catalog.format_demo.events_delta
# MAGIC
# MAGIC with source as (
# MAGIC
# MAGIC     select * from {{ source('format_demo', 'events_delta') }}
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC renamed as (
# MAGIC
# MAGIC     select
# MAGIC         cast(id as int)             as event_id,
# MAGIC         cast(event_date as date)    as event_date,
# MAGIC         upper(city)                 as city
# MAGIC     from source
# MAGIC
# MAGIC )
# MAGIC
# MAGIC select * from renamed
# MAGIC where event_date >= '2026-11-01'

# COMMAND ----------

# DBTITLE 1,fct_events_by_city.sql
# MAGIC %sql
# MAGIC -- models/marts/fct_events_by_city.sql
# MAGIC -- Incremental model with MERGE strategy (native to Databricks tables).
# MAGIC -- {{ ref() }} creates a dependency on the staging model.
# MAGIC {{ config(
# MAGIC     materialized = 'incremental',
# MAGIC     incremental_strategy = 'merge',
# MAGIC     unique_key = 'city'
# MAGIC ) }}
# MAGIC
# MAGIC with events as (
# MAGIC
# MAGIC     select * from {{ ref('stg_events') }}
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC aggregated as (
# MAGIC
# MAGIC     select
# MAGIC         city,
# MAGIC         count(*)           as event_count,
# MAGIC         min(event_date)    as first_event,
# MAGIC         max(event_date)    as last_event
# MAGIC     from events
# MAGIC
# MAGIC     {% if is_incremental() %}
# MAGIC     -- On incremental runs, only process rows newer than what's already loaded
# MAGIC     where event_date > (select coalesce(max(last_event), '1900-01-01') from {{ this }})
# MAGIC     {% endif %}
# MAGIC
# MAGIC     group by city
# MAGIC
# MAGIC )
# MAGIC
# MAGIC select * from aggregated

# COMMAND ----------

# DBTITLE 1,schema.yml tests
# MAGIC %md
# MAGIC ### `models/staging/schema.yml` — tests & documentation
# MAGIC ```yaml
# MAGIC version: 2
# MAGIC
# MAGIC models:
# MAGIC   - name: stg_events
# MAGIC     description: "Cleaned events from the Delta demo table"
# MAGIC     columns:
# MAGIC       - name: event_id
# MAGIC         description: "Unique numeric identifier of the event (cast to int from the raw source)"
# MAGIC         tests: [unique, not_null]
# MAGIC       - name: event_date
# MAGIC         description: "Date the event occurred (cast to date from the raw source)"
# MAGIC         tests: [not_null]
# MAGIC       - name: city
# MAGIC         description: "Swedish city where the event occurred (uppercased)"
# MAGIC         tests:
# MAGIC           - not_null
# MAGIC           - accepted_values:
# MAGIC               arguments:
# MAGIC                 values: ["STOCKHOLM", "GOTHENBURG", "MALMO"]
# MAGIC
# MAGIC   - name: fct_events_by_city
# MAGIC     description: "Event counts per city, built incrementally with the merge strategy"
# MAGIC     columns:
# MAGIC       - name: city
# MAGIC         description: "City the events are grouped by (unique key of the incremental merge)"
# MAGIC       - name: event_count
# MAGIC         description: "Number of events recorded for the city"
# MAGIC       - name: first_event
# MAGIC         description: "Earliest event date seen for the city"
# MAGIC       - name: last_event
# MAGIC         description: "Latest event date seen for the city (drives incremental loading)"
# MAGIC ```
# MAGIC
# MAGIC ### Run it
# MAGIC ``` 
# MAGIC dbt run    # build models (view -> incremental merge)
# MAGIC dbt test   # run the unique / not_null / accepted_values tests above
# MAGIC dbt docs generate && dbt docs serve   # browse the auto-generated lineage docs
# MAGIC ```

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

pip install dbt-databricks

# COMMAND ----------

import os

# COMMAND ----------

os.environ["DBT_DATABRICKS_TOKEN"] = dbutils.secrets.get(scope="dbt_demo", key="dbt_token")

# COMMAND ----------

from dbt.cli.main import dbtRunner

# build models (view -> incremental merge)
# 'dbt run' is a CLI command, not valid Python - invoke it programmatically,
# pointing dbt at the project dir (the notebook cwd is not the dbt project).
result = dbtRunner().invoke([
    "run",
    "--project-dir", "/Workspace/Users/puli.reddy@gmail.com/cloud_analytics-na/databricks/analytics_demos",
    "--profiles-dir", "/Workspace/Users/puli.reddy@gmail.com/cloud_analytics-na/databricks/analytics_demos",
])
print("dbt run succeeded:", result.success)

# COMMAND ----------

from dbt.cli.main import dbtRunner

# 'dbt test' is a CLI command, not valid Python - invoke it programmatically,
# pointing dbt at the project dir (same pattern as the dbt run cell above).
result = dbtRunner().invoke([
    "test",
    "--project-dir", "/Workspace/Users/puli.reddy@gmail.com/cloud_analytics-na/databricks/analytics_demos",
    "--profiles-dir", "/Workspace/Users/puli.reddy@gmail.com/cloud_analytics-na/databricks/analytics_demos",
])
print("dbt test succeeded:", result.success)

# COMMAND ----------

from dbt.cli.main import dbtRunner

# 'dbt docs generate && dbt docs serve' is a shell command, not valid Python.
# Generate the docs site programmatically; 'docs serve' starts a local web
# server on the compute that the browser can't reach, so it can't run here.
result = dbtRunner().invoke([
    "docs", "generate",
    "--project-dir", "/Workspace/Users/puli.reddy@gmail.com/cloud_analytics-na/databricks/analytics_demos",
    "--profiles-dir", "/Workspace/Users/puli.reddy@gmail.com/cloud_analytics-na/databricks/analytics_demos",
])
print("dbt docs generate succeeded:", result.success)

# COMMAND ----------

import json

# 'dbt docs serve' cannot work from a notebook: it starts a blocking local
# web server (localhost:8080) that runs until interrupted, and the browser
# can't reach the compute's localhost. Instead, read the docs artifacts the
# previous cell generated (target/manifest.json) and show docs + lineage inline.
target_dir = "/Workspace/Users/puli.reddy@gmail.com/cloud_analytics-na/databricks/analytics_demos/target"

with open(f"{target_dir}/manifest.json") as f:
    manifest = json.load(f)

print("=== MODELS & LINEAGE ===")
for node_id, node in manifest["nodes"].items():
    if node["resource_type"] == "model":
        print(f"\n{node_id}")
        print(f"  description: {node.get('description') or '(none)'}")
        deps = node.get("depends_on", {}).get("nodes", [])
        print(f"  depends on: {', '.join(deps) if deps else '(nothing)'}")
        for col in node.get("columns", {}).values():
            print(f"  column {col['name']}: {col.get('description') or '(no description)'}")

print("\n=== SOURCES ===")
for src_id, src in manifest["sources"].items():
    print(f"{src_id} ({src['database']}.{src['schema']}.{src['name']})")
    print(f"  description: {src.get('description') or '(none)'}")

# COMMAND ----------

# DBTITLE 1,Build self-contained dbt docs site
import json

# 'dbt docs serve' can't work from a notebook, and opening target/index.html
# directly fails ('unable to load the manifest file and catalog file') because
# the browser can't fetch the sibling JSON files. dbt ships index.html with
# placeholders - var n={manifest:"MANIFEST.JSON INLINE DATA",
# catalog:"CATALOG.JSON INLINE DATA"} - that its loader uses instead of
# fetching when present. Splice the real JSON in to get a self-contained site
# that opens from the workspace preview or from disk with no web server.
proj = "/Workspace/Users/puli.reddy@gmail.com/cloud_analytics-na/databricks/analytics_demos"

with open(f"{proj}/target/index.html") as f:
    html = f.read()
with open(f"{proj}/target/manifest.json") as f:
    manifest = f.read()
with open(f"{proj}/target/catalog.json") as f:
    catalog = f.read()

# JSON is valid JS; escape "</" so a closing-tag sequence inside JSON strings
# can't terminate the surrounding <script> block prematurely.
out = html.replace(
    '"MANIFEST.JSON INLINE DATA"', manifest.replace("</", "<\\/")
).replace(
    '"CATALOG.JSON INLINE DATA"', catalog.replace("</", "<\\/")
)
assert "INLINE DATA" not in out, "placeholder left behind - regenerate docs first"

dest = f"{proj}/docs_site.html"
with open(dest, "w") as f:
    f.write(out)
print("Self-contained docs site written to:", dest, f"({len(out):,} bytes)")