# Databricks notebook source
# MAGIC %md
# MAGIC ## DLT PIPELINES

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### STREAMING TABLE

# COMMAND ----------

# EXPECTATIONS

my_rules = {
    'rule1' : 'product_id IS NOT NULL',
    'rule2' : 'product_name IS NOT NULL'
}

# COMMAND ----------

@dlt.table()

@dlt.expect_all_or_drop(my_rules)
def DimProduct_Stage():
  df = spark.readStream.table('farhan_cata_new.silver.customer_products')
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEAMING VIEW

# COMMAND ----------

@dlt.table()

def DimProduct_View():
  df = spark.readStream.table('Live.DimProduct_Stage')
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## DIMPRODUCTS

# COMMAND ----------

dlt.create_streaming_table('DimProducts')

# COMMAND ----------

dlt.apply_changes(
    target = 'DimProducts',
    source = 'Live.DimProduct_View',
    keys = ['product_id'],
    sequence_by = 'product_id',
    stored_as_scd_type= 2
)