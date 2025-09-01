import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = "stage_bookings"
)

def stage_bookings():
    df = spark.readStream.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    return df 


# 3 things can be created in dlt  
# - a. stream table
# - b. materialized views, they are like static table  
# - c. streaming view 

rules = {
    "rule1" : "booking_id IS NOT NULL",
    "rule2" : "passenger_id IS NOT NULL"
    }


@dlt.view(
    name = "transformed_bookings"
)

def transformed_bookings():
    df = spark.readStream.table("stage_bookings")

    df = df.withColumn("amount",col("amount").cast("double"))\
    .withColumn("modifiedDate",current_timestamp())\
    .withColumn("booking_date",to_date(col("booking_date")))\
    .drop("_rescued_data")

    return df 


@dlt.table(
    name = "silver_bookings"
)

@dlt.expect_all_or_drop(rules) 
def silver_bookings():
    df = spark.readStream.table("transformed_bookings")  #or here we can use dlt.table("transformed_bookings")
    return df  


# Here we are using _dlt.expectall_ with Rules 
# which is nothing but a dictoinary with all the rules for our table 
# here @dlt expect all can do 3 things 
# - raise a warning if any records are not following the rules (default)
# - it can fail 
# - or it can drop those records


# `valid_pages = {"valid_count": "count > 0", "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"}

# @dlt.table
# @dlt.expect_all(valid_pages)
# def raw_data():
#   # Create a raw dataset

# @dlt.table
# @dlt.expect_all_or_drop(valid_pages)
# def prepared_data():
#   # Create a cleaned and prepared dataset

# @dlt.table
# @dlt.expect_all_or_fail(valid_pages)
# def customer_facing_data():
#   # Create cleaned and prepared to share the dataset`


###################################################################
#flight data 
@dlt.view(
    name = "transformed_flights"
)

def transformed_flights():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/flights/data/")

    df = df.withColumn("modifiedDate",current_timestamp())\
        .drop("_rescued_data")
    
    return df 

#BELOW CODE IS CDC TEMPLATE FROM DOCS  https://learn.microsoft.com/en-us/azure/databricks/dlt/cdc
dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
  target = "silver_flights",
  source = "transformed_flights",
  keys = ["flight_id"],
  sequence_by = col("flight_id"),
  stored_as_scd_type = 1
)

####################################################################################
#airports 
@dlt.view(
    name = "transformed_airports"
)

def transformed_airports():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
    
    df = df.withColumn("modifiedDate",current_timestamp())\
        .drop("_rescued_data")

    return df 


dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
  target = "silver_airports",
  source = "transformed_airports",
  keys = ["airport_id"],
  sequence_by = col("airport_id"),
  stored_as_scd_type = 1
)


##############################################################################################
#passenger data 
@dlt.view(
    name = "transformed_passenger"
)

def transformed_passenger():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
    
    df = df.withColumn("modifiedDate",current_timestamp())\
        .drop("_rescued_data")
        
    return df 


dlt.create_streaming_table("silver_passenger")

dlt.create_auto_cdc_flow(
  target = "silver_passenger",
  source = "transformed_passenger",
  keys = ["passenger_id"],
  sequence_by = col("passenger_id"),
  stored_as_scd_type = 1
) 

##############################################################################################
#STREAMING VIEW SAMPLE

@dlt.table(
    name = "silver_business"
)

def silver_busniess():
    df = dlt.readStream("silver_bookings")
    df = df.join(dlt.read("silver_airports"),["airport_id"])\
        .join(dlt.read("silver_passenger"),["passenger_id"])\
        .join(dlt.read("silver_flights"),["flight_id"])\
        .drop("modifiedDate")

    return df 
