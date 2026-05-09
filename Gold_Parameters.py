# Databricks notebook source
dimension_list = [
    {
        "source_table_name": "silver_passengers",
        "primary_key": "passenger_id"
    },
    {
        "source_table_name": "silver_flights",
        "primary_key": "flight_id"
    },
    {
        "source_table_name": "silver_airports",
        "primary_key": "airport_id"
    }
]


# COMMAND ----------

dbutils.jobs.taskValues.set(key="dimension_list", value=dimension_list)