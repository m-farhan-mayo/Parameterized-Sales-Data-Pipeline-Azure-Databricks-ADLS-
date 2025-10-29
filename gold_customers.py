# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA READING FROM SOURCE

# COMMAND ----------

df = spark.sql("SELECT * FROM farhan_cata_new.silver.customer_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### REMOVING DUPLICATES

# COMMAND ----------

df  = df.dropDuplicates(subset=["customer_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC #**DIVIDING NEW VS OLD RECORDS**

# COMMAND ----------

if init_load_flag == 1:
    df_old = spark.sql('SELECT DimCustomerKey, customer_id, start_date, end_date FROM farhan_cata_new.gold.DimCustomers')

else:
    df_old = spark.sql('SELECT 0 DimCustomerKey,0 customer_id, 0 start_date, 0 end_date FROM farhan_cata_new.silver.customer_silver WHERE 1 = 0')

# COMMAND ----------

df_old = df_old.withColumnRenamed('DimCustomerKey','DimCustomerKey_old')\
                .withColumnRenamed('customer_id','customer_id_old')\
                .withColumnRenamed('start_date','start_date_old')\
                .withColumnRenamed('end_date','end_date_old')

# COMMAND ----------

# MAGIC %md
# MAGIC  **APPLYING JOINS WITH OLD RECORDS**

# COMMAND ----------

df_join = df.join(df_old, df.customer_id == df_old.customer_id_old , 'left')

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SEPERATING OLD VS NEW RECORDS

# COMMAND ----------

df_new = df_join.filter(df_join.DimCustomerKey_old.isNull())


# COMMAND ----------

df_old = df_join.filter(df_join.DimCustomerKey_old.isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### PREPARING DF_OLD

# COMMAND ----------

# Deleting all columns which are not required

df_old = df_old.drop('customer_id_old', 'end_date_old')

#renaming dimkey
df_old = df_old.withColumnRenamed('DimCustomerKey_old','DimCustomerKey')

# Renaming 'start_date_old' to ' start_date'

df_old = df_old.withColumnRenamed('start_date_old','start_date')
df_old = df_old.withColumn('start_date', to_timestamp(col('start_date')))



#Recreating 'update_date' column with current timestamp

df_old = df_old.withColumn('update_date', current_timestamp())





# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PREPARING DF_NEW

# COMMAND ----------

# Deleting all columns which are not required

df_new = df_new.drop('DimCustomerKey_old', 'customer_id_old', 'end_date_old', 'start_date_old')

#Recreating 'update_date', 'start_date" column with current timestamp

df_new = df_new.withColumn('update_date', current_timestamp())
df_new = df_new.withColumn('start_date', current_timestamp())


# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SURROGATE KEY - FROM 1
# MAGIC

# COMMAND ----------

df_new = df_new.withColumn('DimCustomerKey',monotonically_increasing_id()+lit(1))

# COMMAND ----------

df_new.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## MAX SURROGATE KEY

# COMMAND ----------

if init_load_flag == 0:
    max_surrogate_key = 0
else:
    df_maxsur = spark.sql('SELECT max(DimCustomerKey) as max_surrogate_key FROM farhan_cata_new.gold.DimCustomers')

    #CONVERTING DF_MAXSUR TO MAX_SURROGATE_KEY VARIABLE
    max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn('DimCustomerKey',lit(max_surrogate_key)+col('DimCustomerKey'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION OF DF_NEW & OLD

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD TYPE 1
# MAGIC

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

# Create the schema if it does not exist
spark.sql("CREATE SCHEMA IF NOT EXISTS farhan_cata_new.gold")

if (spark.catalog.tableExists('farhan_cata_new.gold.DimCustomers')):
    dlt_obj = DeltaTable.forPath(spark, 'abfss://gold@farhanstorgedlnew.dfs.core.windows.net/DimCustomers')

    dlt_obj.alias('trg').merge(df_final.alias('src'), 'trg.DimCustomerKey = src.DimCustomerKey')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    
else:
    df_final.write.mode('overwrite')\
        .format('delta')\
        .option('path','abfss://gold@farhanstorgedlnew.dfs.core.windows.net/DimCustomers')\
        .saveAsTable('farhan_cata_new.gold.DimCustomers')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM farhan_cata_new.gold.dimcustomers

# COMMAND ----------

