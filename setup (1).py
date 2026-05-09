# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME raw.raw_flight_data

# COMMAND ----------

volume_path = "/Volumes/workspace/raw/raw_flight_data"

# COMMAND ----------

dbutils.fs.mkdirs(f"{volume_path}/bookings")

# COMMAND ----------

dbutils.fs.mkdirs(f"{volume_path}/airports")
dbutils.fs.mkdirs(f"{volume_path}/flights")
dbutils.fs.mkdirs(f"{volume_path}/passengers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS bronze.bronze_volume;
# MAGIC CREATE VOLUME IF NOT EXISTS silver.silver_volume;
# MAGIC CREATE VOLUME IF NOT EXISTS gold.gold_volume;

# COMMAND ----------

