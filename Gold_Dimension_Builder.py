# Databricks notebook source
from pyspark.sql.functions import col, lit, current_timestamp, max, when, row_number, coalesce
from pyspark.sql.window import Window
from delta.tables import DeltaTable


# COMMAND ----------

# dbutils.widgets.text() creates a text input box.
# The first argument is the widget's name (the parameter key).
# The second argument is its default value.
dbutils.widgets.text("source_table_name", "")
dbutils.widgets.text("primary_key", "")
dbutils.widgets.text("target_schema", "gold")
dbutils.widgets.text("cdc_column", "modified_date")

# COMMAND ----------

# dbutils.widgets.get() reads the value provided for each widget.
source_table_name = dbutils.widgets.get("source_table_name")
primary_key = dbutils.widgets.get("primary_key")
target_schema = dbutils.widgets.get("target_schema")
cdc_column = dbutils.widgets.get("cdc_column")

# COMMAND ----------

target_table_name = f"dim_{source_table_name}"
surrogate_key = f"dim_{source_table_name}_key"
source_path = f"silver.{source_table_name}"
target_path = f"{target_schema}.{target_table_name}"


# COMMAND ----------

# Default to a very old date for the initial load, ensuring all source records are selected.
last_load_date = "1900-01-01 00:00:00"

# COMMAND ----------

# spark.catalog.tableExists() checks if our target dimension table has already been created.
if spark.catalog.tableExists(target_path):
    print(f"Table {target_path} exists. Performing incremental load.")
    # If the table exists, run a SQL query to get the maximum value of our CDC column.
    max_date_df = spark.sql(f"SELECT max({cdc_column}) FROM {target_path}")
    
    # .collect()[0][0] is a common pattern to extract a single scalar value from a DataFrame.
    # It brings the small result (one row, one column) to the driver node.
    result = max_date_df.collect()[0][0]
    
    # A crucial check: if the table exists but is empty, max() will return None.
    # We only update our last_load_date if we get a valid timestamp back.
    if result:
        last_load_date = result
else:
    print(f"Table {target_path} does not exist. Performing initial full load.")

print(f"Last Load Date: {last_load_date}")


# COMMAND ----------

# .filter() selects only the rows where the CDC column's value is greater than our last_load_date.
# lit() is used to treat our Python variable as a literal value in the Spark filter.
source_df = (spark.read.table(source_path).filter(col(cdc_column) > lit(last_load_date)))

# COMMAND ----------

# A check to see if there's any new data.
# source_df.limit(1).count() is an efficient way to check for emptiness without scanning the whole table.
if source_df.limit(1).count() == 0:
    print("No new data to process. Exiting.")
    # dbutils.notebook.exit() stops the notebook execution gracefully.
    dbutils.notebook.exit("Success: No new data.")


# COMMAND ----------

# --- Case 1: Incremental Load (Target table already exists) ---
if spark.catalog.tableExists(target_path):
    target_df = spark.read.table(target_path)
    # Find the highest existing surrogate key. New keys will start from this number + 1.
    max_sk_result = target_df.select(max(col(surrogate_key)).alias("max_sk")).collect()[0]["max_sk"]

    max_sk = max_sk_result if max_sk_result is not None else 0  # Handle case where table is empty
    # A left join from source to target on the primary key identifies new vs. existing records.
    # If a source record has no match in the target, it's NEW.
    # If it has a match, it's an UPDATE.

    enriched_source_df = source_df.join(target_df, primary_key, "left_outer") \
        .select(source_df["*"], target_df[surrogate_key].alias("existing_sk"), target_df["create_date"])

    # A Window function allows us to perform calculations across a set of rows.
    # Here, we use row_number() to generate a sequence (1, 2, 3...) for our new records.
    window = Window.orderBy(primary_key)

    # The 'when' function is a CASE WHEN statement.
    # IF existing_sk is not null (it's an update), use the existing key.
    # OTHERWISE (it's a new record), use the new key we just generated.

    # coalesce() returns the first non-null value.
    # IF create_date exists (it's an update), keep it.
    # OTHERWISE (it's a new record), set it to the current timestamp.


    final_source_df = enriched_source_df \
        .withColumn("new_sk", (lit(max_sk) + row_number().over(window))) \
        .withColumn(surrogate_key, 
                    when(col("existing_sk").isNotNull(), col("existing_sk")).otherwise(col("new_sk"))) \
        .withColumn("create_date",
                    coalesce(col("create_date"), current_timestamp())) \
        .withColumn("update_date", current_timestamp())
    # --- Case 2: Initial Load (Target table does not exist) ---
else:
    # This is much simpler. All records are new.
    window = Window.orderBy(primary_key)
    final_source_df = source_df \
        .withColumn(surrogate_key, row_number().over(window)) \
        .withColumn("create_date", current_timestamp()) \
        .withColumn("update_date", current_timestamp())


# COMMAND ----------

# Check if the target table exists to determine the operation (CREATE vs. MERGE).
if not spark.catalog.tableExists(target_path):
    # INITIAL LOAD: Table does not exist, so we create it for the first time.
    print(f"Performing initial load. Creating new table '{target_path}'...")
    final_source_df.write.format("delta").mode("overwrite").saveAsTable(target_table_name)
    print("Initial load complete.")
else:
    # INCREMENTAL LOAD: Table exists, so we perform a MERGE (UPSERT).
    # DeltaTable.forName() loads the existing Delta table as an object we can operate on.
    delta_target = DeltaTable.forName(spark, target_path)
    
    delta_target.alias("target").merge(
        final_source_df.alias("source"),
        # The merge condition: match records based on their natural primary key.
        f"target.{primary_key} = source.{primary_key}"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    print("Incremental MERGE complete.")
print(f"Dimension '{target_table_name}' building complete.")
