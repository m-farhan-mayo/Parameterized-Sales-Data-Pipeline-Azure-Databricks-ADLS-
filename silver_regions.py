# Databricks notebook source
df = spark.read.table('farhan_cata_new.bronze.regions')

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop('_rescued_data')
df.display()

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('abfss://silver@farhanstorgedlnew.dfs.core.windows.net/regions')

# COMMAND ----------

df = spark.read.format('delta').load('abfss://silver@farhanstorgedlnew.dfs.core.windows.net/orders')
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS farhan_cata_new.silver.customer_regions
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@farhanstorgedlnew.dfs.core.windows.net/regions'
# MAGIC     
# MAGIC