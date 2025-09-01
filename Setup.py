# Databricks notebook source
# MAGIC %sql
# MAGIC create volume workspace.raw.rawvolume 
# MAGIC
# MAGIC --create volumne creates a managed space where you can maintain unified data governance on data stored in file format and it replaces the dbfs 

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/airports")

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE VOLUME workspace.bronze.bronzevolume
# MAGIC
# MAGIC --DROP VOLUME workspace.bronze.bronzevolume

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE VOLUME workspace.silver.silvervolume

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE VOLUME workspace.gold.goldvolume

# COMMAND ----------

