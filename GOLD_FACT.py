# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### **Parameters**

# COMMAND ----------

# Catalog Name
catalog = "workspace"

# Source Schema
source_schema = "silver"

# Source Object 
source_object = "silver_bookings"

# CDC Column
cdc_column = "modifiedDate"

# Backdated Refresh
backdated_refresh = ""

# Source Fact Table
fact_table = f"{catalog}.{source_schema}.{source_object}"

# Target Schema 
target_schema = "gold"

# Target Object 
target_object = "FactBookings"

# Fact Key Cols List 
fact_key_cols = ["DimPassengersKey","DimFlightsKey","DimAirportsKey","booking_date"]


# COMMAND ----------

dimensions = [
    {
        "table": f"{catalog}.{target_schema}.DimPassengers",
        "alias": "DimPassengers",
        "join_keys": [("passenger_id", "passenger_id")]  # (fact_col, dim_col)
    },
    {
        "table": f"{catalog}.{target_schema}.DimFlights",
        "alias": "DimFlights",
        "join_keys": [("flight_id", "flight_id")]  # (fact_col, dim_col)
    },
    {
        "table": f"{catalog}.{target_schema}.DimAirports",
        "alias": "DimAirports",
        "join_keys": [("airport_id", "airport_id")]  # (fact_col, dim_col)
    },
]

'''
Note : You can use the below code to add more dimensions to the fact table.
 "join_keys": [
            ("CountryID", "CountryID"),
            ("RegionID", "RegionID")
        ]
'''


# Columns you want to keep from Fact table (besides the surrogate keys)
fact_columns = ["amount","booking_date","modifiedDate"]

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Last Load Date**

# COMMAND ----------

# No Back Dated Refresh
if len(backdated_refresh) == 0:
  
  # If Table Exists In The Destination
  if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    last_load = spark.sql(f"SELECT max({cdc_column}) FROM workspace.{target_schema}.{target_object}").collect()[0][0]
    
  else:
    last_load = "1900-01-01 00:00:00"

# Yes Back Dated Refresh
else:
  last_load = backdated_refresh

# Test The Last Load 
last_load

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DYNAMIC FACT QUERY [BRING KEYS]**

# COMMAND ----------

def generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_column, processing_date):
    fact_alias = "f"
    
    # Base columns to select
    select_cols = [f"{fact_alias}.{col}" for col in fact_columns]

    # Build joins dynamically
    join_clauses = []
    for dim in dimensions:
        table_full = dim["table"]
        alias = dim["alias"]
        table_name = table_full.split('.')[-1]
        surrogate_key = f"{alias}.{table_name}Key"
        select_cols.append(surrogate_key)

        # Build ON clause
        on_conditions = [
            f"{fact_alias}.{fk} = {alias}.{dk}" for fk, dk in dim["join_keys"]
        ]
        join_clause = f"LEFT JOIN {table_full} {alias} ON " + " AND ".join(on_conditions)
        join_clauses.append(join_clause)

    # Final SELECT and JOIN clauses
    select_clause = ",\n    ".join(select_cols)
    joins = "\n".join(join_clauses)

    # WHERE clause for incremental filtering
    where_clause = f"{fact_alias}.{cdc_column} >= DATE('{last_load}')"

    # Final query
    query = f"""
SELECT
    {select_clause}
FROM {fact_table} {fact_alias}
{joins}
WHERE {where_clause}
""".strip()

    return query


# COMMAND ----------

query = generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_column, last_load)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DF_FACT**

# COMMAND ----------

df_fact = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **UPSERT**

# COMMAND ----------

# Fact Key Columns Merge Condition
fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])
fact_key_cols_str

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_fact.alias("src"), fact_key_cols_str)\
                        .whenMatchedUpdateAll(condition = f"src.{cdc_column} >= trg.{cdc_column}")\
                        .whenNotMatchedInsertAll()\
                        .execute()

else: 

    df_fact.write.format("delta")\
            .mode("append")\
            .saveAsTable(f"{catalog}.{target_schema}.{target_object}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.factbookings

# COMMAND ----------

