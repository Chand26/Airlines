# Databricks notebook source
from delta.tables import DeltaTable

# COMMAND ----------

source_fact_table = "silver.silver_bookings"
target_fact_table = "gold.fact_bookings"
cdc_column = "modified_date"


# COMMAND ----------

# This configuration drives the dynamic query generation.
dimensions_config = [
    {"dim_table": "gold.dim_silver_passengers", "lookup_key": "passenger_id", "surrogate_key": "dim_silver_passengers_key"},
    {"dim_table": "gold.dim_silver_flights", "lookup_key": "flight_id", "surrogate_key": "dim_silver_flights_key"},
    {"dim_table": "gold.dim_silver_airports", "lookup_key": "airport_id", "surrogate_key": "dim_silver_airports_key"}
]

# COMMAND ----------

# Dynamic query generation
query = "SELECT\n"
for i, dim in enumerate(dimensions_config):
    query += f"  d{i}.{dim['surrogate_key']},\n"
query += "  f.amount,\n  f.booking_date\n"
query += f"FROM {source_fact_table} f\n"
for i, dim in enumerate(dimensions_config):
    query += f"LEFT JOIN {dim['dim_table']} d{i} ON f.{dim['lookup_key']} = d{i}.{dim['lookup_key']}\n"

print("--- Generated SQL Query ---\n" + query)


# COMMAND ----------

# Execute query and save as fact table
final_fact_df = spark.sql(query)

# For simplicity, we'll do a full overwrite. In production, you would add MERGE logic.
final_fact_df.write.format("delta").mode("overwrite").saveAsTable(target_fact_table)
print(f"Fact table {target_fact_table} created successfully.")
