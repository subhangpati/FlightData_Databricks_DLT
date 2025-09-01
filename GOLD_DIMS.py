# Databricks notebook source
##KEYCOLUMNS 
dbutils.widgets.text("keycols","")

##CDC COLUMNS 
dbutils.widgets.text("cdc_cols","")

#BACK DATED REFRESH 
dbutils.widgets.text("backdated_refresh","")

#SOURCE OBJECT 
dbutils.widgets.text("source_object","")

#SOURCE SCHEMA 
dbutils.widgets.text("source_schema","")

# COMMAND ----------

# #key cols list 
# key_col_list = eval(dbutils.widgets.get("keycols"))
# key_col_list

# #cdc col 
# cdc_col = dbutils.widgets.get("cdc_cols")

# #backdated refresh 
# backdated_refresh = dbutils.widgets.get("backdated_refresh")

# #SOURCE OBJECT 
# source_object = dbutils.widgets.get("source_object")

# #source schema 
# source_schema = dbutils.widgets.get("source_schema")

# #target schema 
# target_schema = "gold"

# #target object 
# target_object = "DimFlights"

# #surrogate key 
# surrogate_key = "DimFlightskey"

# COMMAND ----------

# #key cols list 
# key_col_list = eval(dbutils.widgets.get("keycols"))
# key_col_list

# #cdc col 
# cdc_col = dbutils.widgets.get("cdc_cols")

# #backdated refresh 
# backdated_refresh = dbutils.widgets.get("backdated_refresh")

# #SOURCE OBJECT 
# source_object = dbutils.widgets.get("source_object")

# #source schema 
# source_schema = dbutils.widgets.get("source_schema")

# #target schema 
# target_schema = "gold"

# #target object 
# target_object = "DimAirports"

# #surrogate key 
# surrogate_key = "DimAirportskey"

# COMMAND ----------

#key cols list 
key_col_list = eval(dbutils.widgets.get("keycols"))
key_col_list

#cdc col 
cdc_col = dbutils.widgets.get("cdc_cols")

#backdated refresh 
backdated_refresh = dbutils.widgets.get("backdated_refresh")

#SOURCE OBJECT 
source_object = dbutils.widgets.get("source_object")

#source schema 
source_schema = dbutils.widgets.get("source_schema")

#target schema 
target_schema = "gold"

#target object 
target_object = "DimPassengers"

#surrogate key 
surrogate_key = "DimPassengerskey"

# COMMAND ----------

# MAGIC %md
# MAGIC ### **INCREMENTAL DATA INGESTION**

# COMMAND ----------

# MAGIC %md
# MAGIC > we want to read all the data after a specific date, which is called as last load date.

# COMMAND ----------

# MAGIC %md
# MAGIC LAST LOAD DATE

# COMMAND ----------

if backdated_refresh == "":
  if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
    last_load = spark.sql(f"select max({cdc_col}) from workspace.{target_schema}.{target_object}").collect()[0][0]
    print(last_load)

  else :
    last_load = "1900-01-01"

else:
  last_load = backdated_refresh
print("backdated refresh")
last_load

# COMMAND ----------

df_src = spark.sql(f"SELECT * FROM workspace.{source_schema}.{source_object} WHERE {cdc_col} > '{last_load}'") 

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
    #the following loop will run for incremental load
    key_col_str_incremental =', '.join(key_col_list)
    df_tgt = spark.sql(f"SELECT {key_col_str_incremental},{surrogate_key},created_date ,update_date  FROM workspace.{target_schema}.{target_object}")

else:
    # the following loop will run for initial load
    key_col_str_init = [f"'' AS {i}" for i in key_col_list]
    key_col_str_init = ', '.join(key_col_str_init)
    df_tgt =spark.sql(f"SELECT {key_col_str_init},CAST('0' AS INT)  as {surrogate_key},cast('1900-01-01' as timestamp) as created_date ,cast('1900-01-01' as timestamp) as update_date where 1=0 ") 
#FROM workspace.{target_schema}.{target_object}

# COMMAND ----------

df_tgt.display()

# COMMAND ----------

join_condition = ' AND '.join([f"src.{i} = trg.{i}"for i in key_col_list])

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_tgt.createOrReplaceTempView("trg")


print(df_src.count())
print(df_tgt.count())
print(df_src.columns)
print(df_tgt.columns)


df_join = spark.sql(f"""
            SELECT src.*, 
                   trg.{surrogate_key},
                   trg.update_date,
                   trg.created_date 
            FROM src 
            LEFT JOIN trg 
            ON {join_condition}
          """)

# COMMAND ----------

df_join.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


#old records 
df_old = df_join.filter(col(surrogate_key).isNotNull())
#new records
df_new = df_join.filter(col(surrogate_key).isNull())


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preparing old DF**

# COMMAND ----------

df_old_enr = df_old.withColumn('update_date', current_timestamp())
df_old_enr.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preparing New DF**

# COMMAND ----------

df_new = df_new.withColumn('created_date',current_timestamp())
df_new = df_new.withColumn('update_date',current_timestamp())

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
  max_surrogate_key = spark.sql(f"SELECT max({surrogate_key}) FROM workspace.{target_schema}.{target_object}").collect()[0][0]
  df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                  .withColumn('update_date', current_timestamp())\
                  .withColumn('created_date', current_timestamp())  
else:
  max_surrogate_key = 0
  df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                  .withColumn('update_date', current_timestamp())\
                  .withColumn('created_date', current_timestamp())  
df_new_enr.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **UNION OF OLD AND NEW RECORDS**

# COMMAND ----------

df_union = df_old_enr.unionByName(df_new_enr)
df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **UPSERT**

# COMMAND ----------

# MAGIC %md
# MAGIC DeltaTable.forPath(spark,f"/Volumes/workspace/gold/goldvolume/{target_object}") is used for external datalake 
# MAGIC
# MAGIC when using table we need to use forName 

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
  dlt_obj = DeltaTable.forName(spark,f"workspace.{target_schema}.{target_object}")
  dlt_obj.alias("trg").merge(
    df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
    .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
    .whenNotMatchedInsertAll().execute()

else:
    df_union.write.format("delta")\
        .mode("append")\
        .saveAsTable(f"workspace.{target_schema}.{target_object}") 



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.dimpassengers 
# MAGIC  