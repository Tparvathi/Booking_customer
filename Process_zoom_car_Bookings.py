# Databricks notebook source

from pyspark.sql.functions import col, lit, current_timestamp, sum as _sum
from delta.tables import DeltaTable
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
import os

print(os.environ['SPARK_VERSION'])

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType,TimestampType ,DoubleType# Define a new schema 
new_schema = StructType([ StructField("booking_id", StringType(), nullable = False), StructField("customer_id", StringType(), True), StructField("car_id", StringType(), True), StructField("start_time", TimestampType(), True), StructField("end_time", TimestampType(), True),StructField("booking_date", DateType(), True),
                        StructField("total_amount", DoubleType(), True) ,StructField("status", StringType(), True)])




# COMMAND ----------

# Get job parameters from Databricks
date_str = dbutils.widgets.get("file_date")
# date_str = "2024-01-02"

# Define file paths based on date parameter
booking_data = f"/Volumes/incremental_load/default/orders_data/booking_data/{date_str}.json"

# /Volumes/incremental_load/default/orders_data/booking_data/2024-01-01.json
customer_data = f"/Volumes/incremental_load/default/orders_data/customer_data/{date_str}_customer.json"

#/Volumes/incremental_load/default/orders_data/customer_data/
print(booking_data)
print(customer_data)

# COMMAND ----------

# Read booking data




booking_df = spark.read \
    .format("json") \
    .schema(new_schema) \
    .option("header", "true") \
    .option("multiLine", "true") \
        .load(booking_data)
    # .option("mode", "PERMISSIVE") \
    # .option("pathGlobFilter", "*.json") \
    # .option("recursiveFileLookup", "true") \
    

booking_df.printSchema()
display(booking_df)

# Filter out rows with null values in the "booking_id" column 
# df_filtered = booking_df.dropna(subset=["booking_id"]) # Show the filtered DataFrame 
df_filtered = booking_df.filter(booking_df["booking_id"].isNotNull())
print("Filtered DataFrame:") 
df_filtered.show(truncate=False) # Print schema to verify "not null" constraints 
print("Filtered Schema:") 
df_filtered.printSchema()







# COMMAND ----------

# Perform data cleaning and validation:
# ○ Remove records with null values in critical fields (booking_id,
# customer_id, car_id, booking_date).
# ○ Validate date formats.
# ○ Ensure status is one of the predefined statuses (e.g., completed,
# cancelled, pending).
# ○ Load cleaned data into the staging_bookings_delta table.

# Data Quality Checks on booking data

    
check_incremental = Check(spark, CheckLevel.Error, "Booking Data Check") \
   .isComplete("booking_id") \
    .isComplete("customer_id") \
    .isComplete("car_id") \
        .isComplete("booking_date") \
    .isContainedIn("status", ["completed", "cancelled", "pending"]) 

# Define a check to verify date format check = Check(spark, name="DateFormatCheck") \ .has("signup_date", "date_format", "yyyy-MM-dd") \ .on(df) \ .run()


    # Run the verification suite
booking_dq_check = VerificationSuite(spark) \
    .onData(booking_df) \
    .addCheck(check_incremental) \
    .run()


booking_dq_check_df = VerificationResult.checkResultsAsDataFrame(spark, booking_dq_check)
display(booking_dq_check_df)


# Register the custom constraint in Deequ check = Check(spark, CheckLevel.Error, "Date format check") check = check.isContainedIn("signup_date", check_date_format) # Run the verification suite verification_result = VerificationSuite(spark) \ .onData(df) \ .addCheck(check) \ .run()

# COMMAND ----------


# Check if verification passed
if booking_dq_check.status != "Success":
    raise ValueError("Data Quality Checks Failed for Booking Data")




# COMMAND ----------

# Check if the Delta table exists
booking_staging_table_path = "incremental_load.default.booking_staging_zoom_car"
fact_table_exists = spark._jsparkSession.catalog().tableExists(booking_staging_table_path)
# Write the final staging_booking_delta table data back to the Delta table
booking_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(booking_staging_table_path)


# COMMAND ----------

# Read the existing fact table
booking_transformation = spark.read.format("delta").table(booking_staging_table_path)
# booking_transformation.show()
#     Bookings Data Transformations:
# ■ Parse start_time and end_time into separate date and time
# columns. And calculate the total duration of each booking in hours.


from pyspark.sql.functions import col, split, to_date, to_timestamp,date_format,unix_timestamp
df_booking_start_end_date_time_Parse = booking_transformation.withColumn("booking_start_date", to_date(col("start_time").substr(1, 10), "yyyy-MM-dd")) .withColumn("booking_start_time", split(col("start_time").substr(12, 8), "\\.")[0]).withColumn("booking_end_date", to_date(col("end_time").substr(1, 10), "yyyy-MM-dd")) .withColumn("booking_end_time", split(col("end_time").substr(12, 8), "\\.")[0]).withColumn("duration_hours", (col("end_time").cast("long") - col("start_time").cast("long")) / 3600)
df_booking_start_end_date_time_Parse.show()




# COMMAND ----------

# Check if the Delta table exists
booking_transformed_table_path = "incremental_load.default.booking_transformed_zoom_car_new"
fact_table_exists = spark._jsparkSession.catalog().tableExists(booking_transformed_table_path)
# Write the final staging_booking_delta table data back to the Delta table
df_booking_start_end_date_time_Parse.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(booking_transformed_table_path)


# COMMAND ----------


