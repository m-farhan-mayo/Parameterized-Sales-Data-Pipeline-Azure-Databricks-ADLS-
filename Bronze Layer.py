# Databricks notebook source
# MAGIC %md
# MAGIC ### DYNAMIC CAPABILITIES

# COMMAND ----------

dbutils.widgets.text('file_name', '')

# COMMAND ----------

p_file_name = dbutils.widgets.get('file_name')

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://bronze@farhanstorgedlnew.dfs.core.windows.net/orders')

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA READING
# MAGIC

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
  .option('cloudFiles.format','parquet')\
  .option('cloudFiles.SchemaLocation',f'abfss://bronze@farhanstorgedlnew.dfs.core.windows.net/schema/checkpoint_{p_file_name}')\
  .load(f'abfss://source@farhanstorgedlnew.dfs.core.windows.net/{p_file_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA WRITING

# COMMAND ----------

df.writeStream.format('parquet')\
    .outputMode('append')\
    .option('checkpointLocation',f'abfss://bronze@farhanstorgedlnew.dfs.core.windows.net/schema/checkpoint_{p_file_name}')\
    .option('path', f'abfss://bronze@farhanstorgedlnew.dfs.core.windows.net/{p_file_name}')\
    .trigger(once=True)\
    .start()

# COMMAND ----------

df = spark.read.format('parquet').load(f'abfss://bronze@farhanstorgedlnew.dfs.core.windows.net/{p_file_name}')
df.display()