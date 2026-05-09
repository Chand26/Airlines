# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronze_volume/bookings/")
display(df)

# COMMAND ----------

df_transformed = (df
  .withColumn("amount", col("amount").cast("decimal(10, 2)"))
  .withColumn("booking_date", to_date(col("booking_date")))
  .withColumn("modified_date", current_timestamp())
  .drop("_rescued_data")
)

display(df_transformed)


# COMMAND ----------

