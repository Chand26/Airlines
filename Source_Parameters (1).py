# Databricks notebook source
src_array = [

    {"src_folder": "bookings"},
    {"src_folder": "airports"},
    {"src_folder": "passengers"},
    {"src_folder": "flights"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key="source_list", value=src_array)