# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://bronze@farhanstorgedlnew.dfs.core.windows.net/orders')


# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.drop('_rescued_data')


# COMMAND ----------

df = df.withColumn('order_date', to_timestamp(col('order_date')))

df.display()

# COMMAND ----------

df = df.withColumn('years', year(col('order_date')))

df.display()

# COMMAND ----------

df1 = df.withColumn('flags', dense_rank().over(Window.partitionBy('years').orderBy(desc('total_amount'))))
df1.display()

# COMMAND ----------

df1 = df1.withColumn('rank_flags', rank().over(Window.partitionBy('years').orderBy(desc('total_amount'))))
df1.display()

# COMMAND ----------

df1 = df1.withColumn('row_numbers', row_number().over(Window.partitionBy('years').orderBy(desc('total_amount'))))
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### CLASS - OOP

# COMMAND ----------

class windows:

  def denserank(self,df):
    df_dense_rank = df.withColumn('flags', dense_rank().over(Window.partitionBy('years').orderBy(desc('total_amount'))))

    return df_dense_rank

  def dfrank(self,df):
    df_rank = df.withColumn('flags', rank().over(Window.partitionBy('years').orderBy(desc('total_amount'))))

    return df_rank

  def rownumber(self,df):
    df_row_number = df.withColumn('flags', row_number().over(Window.partitionBy('years').orderBy(desc('total_amount'))))

    return df_row_number


# COMMAND ----------

df_new = df

# COMMAND ----------

obj = windows()

# COMMAND ----------

obj.denserank(df_new).display()
obj.dfrank(df_new).display()
obj.rownumber(df_new).display()


# COMMAND ----------

df.write.format('delta').mode('overwrite').save('abfss://silver@farhanstorgedlnew.dfs.core.windows.net/orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS farhan_cata_new.silver.customer_orders
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@farhanstorgedlnew.dfs.core.windows.net/orders'
# MAGIC     
# MAGIC