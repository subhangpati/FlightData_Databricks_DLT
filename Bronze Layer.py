# Databricks notebook source
# MAGIC %md
# MAGIC ### "INCREMENTAL DATA INGESTION"

# COMMAND ----------

dbutils.widgets.text("source","")

# COMMAND ----------

src_val = dbutils.widgets.get("source")
src_val 

# COMMAND ----------

# %sql 
# CREATE VOLUME workspace.raw.bronze

# COMMAND ----------

# %sql 
# CREATE VOLUME workspace.raw.silver 

# COMMAND ----------

# %sql 
# CREATE VOLUME workspace.raw.gold


# COMMAND ----------

autoloader_df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation",f"/Volumes/workspace/bronze/bronzevolume/{src_val}/checkpoint")\
    .option("cloudFiles.schemaEvolutionMode", "rescue")\
    .load(f"/Volumes/workspace/raw/rawvolume/rawdata/{src_val}/")


# COMMAND ----------

autoloader_df.writeStream.format("delta")\
             .outputMode("append")\
             .option("checkpointLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_val}/checkpoint")\
             .option("path" , f"/Volumes/workspace/bronze/bronzevolume/{src_val}/data")\
             .trigger(once = True)\
             .start( )
            

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select count(*) as cnt from delta.`/Volumes/workspace/bronze/bronzevolume/customers/data/`