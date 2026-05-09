# Databricks notebook source
dbutils.widgets.text("src_folder", "")

# COMMAND ----------

src_folder_value = dbutils.widgets.get("src_folder")

# COMMAND ----------

source_path = f"/Volumes/workspace/raw/raw_flight_data/{src_folder_value}"
checkpoint_path = f"/Volumes/workspace/raw/raw_flight_data/{src_folder_value}/checkpoint"
bronze_table_name = f"bronze.{src_folder_value}"

# COMMAND ----------

print(f"--- Ingestion Parameters ---")
print(f"Source Folder: {src_folder_value}")
print(f"Source Path: {source_path}")
print(f"Checkpoint Path: {checkpoint_path}")
print(f"Bronze Table: {bronze_table_name}")


# COMMAND ----------

df = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemaLocation",checkpoint_path).option("cloudFiles.schemaEvolutionMode","rescue").option("header", "true").load(source_path)

# COMMAND ----------

df.writeStream.format("delta").outputMode("append").trigger(once=True).option("checkpointLocation", checkpoint_path).option("path", f"/Volumes/workspace/bronze/bronze_volume/{src_folder_value}").start()



# COMMAND ----------

