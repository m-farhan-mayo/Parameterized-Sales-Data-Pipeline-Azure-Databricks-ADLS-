# Databricks notebook source
datasets = [
    {
        'file_name':'orders'
    },
    {
        'file_name':'customer'
    },
    {
        'file_name':'products'
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set("output_datasets", datasets)

# COMMAND ----------

