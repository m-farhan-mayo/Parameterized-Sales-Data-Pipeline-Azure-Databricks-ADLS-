# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://bronze@farhanstorgedlnew.dfs.core.windows.net/products')

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop('_rescued_data')

# COMMAND ----------

df = df.withColumn('product_name_last', split(col('product_name'), ' ')[1])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### FUNCTIONS

# COMMAND ----------

df.createOrReplaceTempView('products')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION farhan_cata_new.bronze.discount_func(p_price DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN p_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_id, farhan_cata_new.bronze.discount_func(price) as discounted_price
# MAGIC FROM products

# COMMAND ----------

df = df.withColumn('discounted_price', expr('farhan_cata_new.bronze.discount_func(price)'))
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION farhan_cata_new.bronze.uppr_func(u_name STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS 
# MAGIC $$
# MAGIC  return u_name.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_id, product_name, farhan_cata_new.bronze.uppr_func(brand) as uprr_brand
# MAGIC FROM products

# COMMAND ----------

df.write.format('delta')\
    .mode('overwrite')\
    .option('path','abfss://silver@farhanstorgedlnew.dfs.core.windows.net/products')\
    .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS farhan_cata_new.silver.customer_products
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@farhanstorgedlnew.dfs.core.windows.net/products'
# MAGIC     
# MAGIC