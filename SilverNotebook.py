# Databricks notebook source
#import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")


df = df.withColumn("modifiedDate",current_timestamp())\
        .drop("_rescued_data")

display(df)

# COMMAND ----------

@dlt.table(
    name = "stage_bookings"
)

def stage_bookings():
    df = spark.readStream.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    return df 


# COMMAND ----------

# MAGIC %md
# MAGIC 3 things can be created in dlt 
# MAGIC - a. stream table
# MAGIC - b. mat views, they are like static table  
# MAGIC - c. streaming view 

# COMMAND ----------

rules = {
    "rule1" : "booking_id IS NOT NULL",
    "rule2" : "passenger_id IS NOT NULL"
    }

# COMMAND ----------

@dlt.view(
    name = "transformed_bookings"
)

def transformed_bookings():
    df = spark.readStream.table("stage_bookings")
    df = df.withColumn("amount",col("amount").cast("double"))\
    .withColumn("modifiedDate",current_timestamp())\
    .withColumn("booking_date",to_date(col("booking_date")))\
    .drop("_rescued_data")


# COMMAND ----------

@dlt.table(
    name = "silver_bookings"
)

@dlt.expect_all_or_drop(rules) 
def silver_bookings():
    df = spark.readStream.table("transformed_bookings")  #or here we can use dlt.table("transformed_bookings")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC Here we are using _dlt.expectall_ with Rules 
# MAGIC which is nothing but a dictoinary with all the rules for our table 
# MAGIC here @dlt expect all can do 3 things 
# MAGIC - raise a warning if any records are not following the rules (default)
# MAGIC - it can fail 
# MAGIC - or it can drop those records
# MAGIC
# MAGIC
# MAGIC `valid_pages = {"valid_count": "count > 0", "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"}
# MAGIC
# MAGIC @dlt.table
# MAGIC @dlt.expect_all(valid_pages)
# MAGIC def raw_data():
# MAGIC   # Create a raw dataset
# MAGIC
# MAGIC @dlt.table
# MAGIC @dlt.expect_all_or_drop(valid_pages)
# MAGIC def prepared_data():
# MAGIC   # Create a cleaned and prepared dataset
# MAGIC
# MAGIC @dlt.table
# MAGIC @dlt.expect_all_or_fail(valid_pages)
# MAGIC def customer_facing_data():
# MAGIC   # Create cleaned and prepared to share the dataset`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.silver_business 

# COMMAND ----------

