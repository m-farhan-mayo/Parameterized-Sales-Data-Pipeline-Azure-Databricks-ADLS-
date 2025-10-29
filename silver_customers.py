# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA READING

# COMMAND ----------

df = spark.read.format('parquet')\
  .load('abfss://bronze@farhanstorgedlnew.dfs.core.windows.net/customer')

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop('_rescued_data')
df.display()

# COMMAND ----------

df = df.withColumn('domains', split(col('email'), '@')[1])
df.display()

# COMMAND ----------

df_new = df

# COMMAND ----------

df_new.display()

# COMMAND ----------

df_new = df_new.withColumn('Hosts',split(col('email'),'@')[0])
df_new.display()

# COMMAND ----------

class Window:

    def dfsplit(self,df):
        df_split = df.withColumn('domains', split(col('email'), '@')[1])

        return df_split

# COMMAND ----------

df.groupBy('domains').agg(count('customer_id').alias('count_domains')).orderBy(desc('count_domains')).display()

# COMMAND ----------

import time

# COMMAND ----------

df_gmail = df.filter(col('domains') == 'gmail.com')
df_gmail.display()
time.sleep(5)

df_hotmail = df.filter(col('domains') == 'hotmail.com')
df_hotmail.display()
time.sleep(5)

df_yahoo = df.filter(col('domains') == 'yahoo.com')
df_yahoo.display()
time.sleep(5)

# COMMAND ----------

df = df.withColumn('full_name', concat(col('first_name'), lit(' '), col('last_name')))
df = df.drop('first_name', 'last_name')
df.display()

# COMMAND ----------

df.write.mode('overwrite').format('delta').save('abfss://silver@farhanstorgedlnew.dfs.core.windows.net/customer')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS farhan_cata_new.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS farhan_cata_new.silver.customer_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@farhanstorgedlnew.dfs.core.windows.net/customer'
# MAGIC     
# MAGIC