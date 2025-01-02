# Databricks notebook source
from pyspark.sql.functions import col, lit, current_timestamp, sum as _sum
from delta.tables import DeltaTable
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
import os

print(os.environ['SPARK_VERSION'])

# COMMAND ----------

fact_table_path = "incremental_load.default.customer_transformed_zoom_car"
customer_transformation_before_scd = spark.read.format("delta").table(fact_table_path)
customer_transformation_before_scd.show()

# COMMAND ----------

from pyspark.sql.functions import row_number ,col
from pyspark.sql.window import Window
# # Step 1: Detect Duplicates # Assuming 'booking_transformation_before_scd' is your source DataFrame 
# window_spec = Window.partitionBy("customer_id").orderBy("customer_id") 
# # # Adding a row number to identify duplicates 
# source_df_with_row_num_cust = customer_transformation_before_scd .withColumn("row_num", row_number().over(window_spec)) # Step 2: Filter Duplicates f
# filtered_cust_source_df = source_df_with_row_num_cust.filter(col("row_num") == 1)

scd_table_path = "incremental_load.default.customer_zoom_car_Final1"
scd_table_exists = spark._jsparkSession.catalog().tableExists(scd_table_path)

# Check if the customers table exists
if scd_table_exists:
    # Load the existing SCD table
    scd_table = DeltaTable.forName(spark, scd_table_path)
    display(scd_table.toDF())
    
    # Perform SCD2 merge logic
    scd_table.alias("scd") \
        .merge(
            customer_transformation_before_scd.alias("updates"),
            "scd.customer_id = updates.customer_id"
        ) \
           .whenMatchedUpdate( condition="scd.name <> updates.name OR " "scd.email <> updates.email OR " "scd.phone_number <> updates.phone_number OR " "scd.signup_date <> updates.signup_date OR " "scd.status <> updates.status OR " "scd.normalized_phone <> updates.normalized_phone OR " "scd.tenure_days <> updates.tenure_days", 
                              set={  "scd.name": "updates.name", "scd.email": "updates.email", "scd.phone_number": "updates.phone_number", "scd.signup_date": "updates.signup_date", "scd.status": "updates.status", "scd.normalized_phone": "updates.normalized_phone", "scd.tenure_days": "updates.tenure_days" } ) \
           .whenMatchedDelete(condition="updates.status = 'in'") \
                .whenNotMatchedInsert(values={ "customer_id": "updates.customer_id", "name": "updates.name", "email": "updates.email", "phone_number": "updates.phone_number", "signup_date": "updates.signup_date", "status": "updates.status", "normalized_phone": "updates.normalized_phone", "tenure_days": "updates.tenure_days" }) \
                    .execute()

  
else:
    # If the SCD table doesn't exist, write the customer data as a new Delta table
    customer_transformation_before_scd.write.format("delta").mode("overwrite").saveAsTable(scd_table_path)

# COMMAND ----------

fact_table_path_booking = "incremental_load.default.booking_transformed_zoom_car_new"
booking_transformation_before_scd = spark.read.format("delta").table(fact_table_path_booking)
booking_transformation_before_scd.show()

# COMMAND ----------

from pyspark.sql.functions import row_number 
from pyspark.sql.window import Window
# # Step 1: Detect Duplicates # Assuming 'booking_transformation_before_scd' is your source DataFrame 
# window_spec = Window.partitionBy("Booking_id").orderBy("Booking_id") 
# # # Adding a row number to identify duplicates 
# source_df_with_row_num = booking_transformation_before_scd .withColumn("row_num", row_number().over(window_spec)) # Step 2: Filter Duplicates f
# iltered_source_df = source_df_with_row_num.filter(col("row_num") == 1)
# iltered_source_df.show()


scd_table_path = "incremental_load.default.Booking_zoom_car_Final"
scd_table_exists = spark._jsparkSession.catalog().tableExists(scd_table_path)

# Check if the booking table exists
if scd_table_exists:
    # Load the existing SCD table
    scd_table = DeltaTable.forName(spark, scd_table_path)
    display(scd_table.toDF())
    
    # Perform SCD2 merge logic
    scd_table.alias("scd") \
        .merge(
            booking_transformation_before_scd.alias("updates"),
            "scd.Booking_id = updates.Booking_id"
        ) \
        .whenMatchedUpdate(condition= "scd.customer_id <> updates.customer_id OR " "scd.car_id <> updates.car_id OR " "scd.start_time <> updates.start_time OR " "scd.end_time <> updates.end_time OR " "scd.booking_date <> updates.booking_date OR " "scd.total_amount <> updates.total_amount OR " "scd.status <> updates.status OR " "scd.booking_start_date <> updates.booking_start_date OR " "scd.booking_start_time <> updates.booking_start_time OR " "scd.booking_end_date <> updates.booking_end_date OR " "scd.booking_end_time <> updates.booking_end_time OR " "scd.duration_hours <> updates.duration_hours",
                           set={"booking_id":"updates.booking_id",
          "customer_id":"updates.customer_id","car_id":"updates.car_id","start_time":"updates.start_time","end_time":"updates.end_time","booking_date":"updates.booking_date","total_amount":"updates.total_amount","status":"updates.status","booking_start_date":"updates.booking_start_date","booking_start_time":"updates.booking_start_time","booking_end_date":"updates.booking_end_date","booking_end_time":"updates.booking_end_time","duration_hours":"updates.duration_hours"}) \
        .whenMatchedDelete(condition="updates.status = 'cancelled'")\
            .whenNotMatchedInsert(values={ "booking_id": "updates.booking_id","customer_id": "updates.customer_id", "car_id":"updates.car_id","start_time":"updates.start_time","end_time":"updates.end_time","booking_date":"updates.booking_date","total_amount":"updates.total_amount","status":"updates.status","booking_start_date":"updates.booking_start_date","booking_start_time":"updates.booking_start_time","booking_end_date":"updates.booking_end_date","booking_end_time":"updates.booking_end_time","duration_hours":"updates.duration_hours" }) \
        .execute()

        

    
else:
    # If the SCD table doesn't exist, write the customer data as a new Delta table
    booking_transformation_before_scd.write.format("delta").mode("overwrite").saveAsTable(scd_table_path)

# COMMAND ----------

from delta.tables import DeltaTable
import shutil
import os
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DeleteDeltaTable") \
    .getOrCreate()

# Path to the Delta table
delta_table_path = "incremental_load.default.booking_zoom_car_final"


# Check if the Delta table exists
if DeltaTable.isDeltaTable(spark, delta_table_path):
    # Delete the Delta table directory and its contents
    dbutils.fs.rm(delta_table_path, True)
    print(f"Delta table at path {delta_table_path} deleted successfully.")
else:
    print(f"No Delta table found at path {delta_table_path}.")




# COMMAND ----------

# Refresh the Metastore to clear any caching
spark.catalog.refreshTable("incremental_load.default.booking_zoom_car_final")


# COMMAND ----------

fact_table_path_booking = 'incremental_load.default.booking_zoom_car_final'


booking_transformation_final_final = spark.read.format("delta").table(fact_table_path_booking)
booking_transformation_final_final.show()

# COMMAND ----------

# %sql
# DROP TABLE incremental_load.default.customer_tran
