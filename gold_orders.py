# Databricks notebook source
# MAGIC %md
# MAGIC ## FACT ORDERS

# COMMAND ----------

df = spark.sql('SELECT * FROM farhan_cata_new.silver.customer_orders')
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM farhan_cata_new.gold.dimproducts

# COMMAND ----------

df_dimcust = spark.sql('SELECT DimCustomerKey, customer_id as dim_customer_id FROM farhan_cata_new.gold.dimcustomers')
df_dimpro = spark.sql('SELECT product_id AS DimCustomerKey, product_id AS dim_product_id FROM farhan_cata_new.gold.dimproducts')


# COMMAND ----------

# MAGIC %md
# MAGIC **FACT DATAFRAME**

# COMMAND ----------

df_fact = df.join(df_dimcust, df['customer_id'] == df_dimcust['dim_customer_id'], how='left').join(df_dimpro, df['product_id'] == df_dimpro['dim_product_id'], how='left')

df_fact_new = df_fact.drop('dim_customer_id', 'dim_product_id', 'customer_id', 'product_id')

# COMMAND ----------

df_fact_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPSERT ON FACT TABLE

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

if spark.catalog.tableExists('farhan_cata_new.gold.FactOrders'):
    
    # Rename the conflicting column in the source DataFrame
    df_fact_new_renamed = df_fact_new.withColumnRenamed('DimCustomerKey', 'DimCustomerKey_src')

    dlt_obj = DeltaTable.forName(spark, 'farhan_cata_new.gold.FactOrders')

    dlt_obj.alias('trg').merge(
        df_fact_new_renamed.alias('src'),
        'trg.order_id = src.order_id AND trg.DimCustomerKey = src.DimCustomerKey_src AND trg.DimProductKey = src.DimProductKey'
    ).whenMatchedUpdateAll()\
     .whenNotMatchedInsertAll()\
     .execute()

else:
    df_fact_new.write.format('delta')\
        .option('path', 'abfss://gold@farhanstorgedlnew.dfs.core.windows.net/FactOrders')\
        .saveAsTable('farhan_cata_new.gold.FactOrders')