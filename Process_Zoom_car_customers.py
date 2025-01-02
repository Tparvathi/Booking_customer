# Databricks notebook source

from pyspark.sql.functions import col, lit, current_timestamp, sum as _sum
from delta.tables import DeltaTable
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
import os

print(os.environ['SPARK_VERSION'])

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType,TimestampType ,DoubleType
new_schema_customer = StructType([ StructField("customer_id", StringType(), nullable = True), StructField("name", StringType(), True), StructField("email", StringType(), True), StructField("phone_number", StringType(), True), StructField("signup_date", DateType(), True), 
                                   StructField("status", StringType(), True) ])
                                   

# COMMAND ----------

# Get job parameters from Databricks
date_str = dbutils.widgets.get("file_date"," ")
# date_str = "2024-01-02"

# /Volumes/incremental_load/default/orders_data/booking_data/2024-01-01.json
customer_data = f"/Volumes/incremental_load/default/orders_data/customer_data/{date_str}_customer.json"

#/Volumes/incremental_load/default/orders_data/customer_data/
print(customer_data)

# COMMAND ----------

# Read customer data for scd2 merge
customer_df = spark.read \
    .format("json") \
        .schema(new_schema_customer) \
    .option("header", "true") \
        .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .load(customer_data)

customer_df.printSchema()
display(customer_df)

# COMMAND ----------

check_scd = Check(spark, CheckLevel.Error, "Customer Data Check") \
    .hasSize(lambda x: x > 0) \
    .isComplete("customer_id") \
    .isComplete("name") \
    .isComplete("status") \
    .isComplete("email") \
     .isComplete("signup_date") \
         .isContainedIn("status", ["active", "inactive"]) 



customer_dq_check = VerificationSuite(spark) \
    .onData(customer_df) \
    .addCheck(check_scd) \
    .run()

customer_dq_check_df = VerificationResult.checkResultsAsDataFrame(spark, customer_dq_check)
display(customer_dq_check_df)


# COMMAND ----------

# check if verification passed

if customer_dq_check.status != "Success":
    raise ValueError("Data Quality Checks Failed for Customer Data")

# COMMAND ----------

# Check if the Delta table exists
fact_table_path = "incremental_load.default.customer_staging_zoom_car_new"
fact_table_exists = spark._jsparkSession.catalog().tableExists(fact_table_path)
# Write the final staging_customers_delta table data back to the Delta table
customer_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(fact_table_path)


# COMMAND ----------

# Customers Data Transformations:

# Read the customer_staging_customers_delta table data
customer_transformation = spark.read.format("delta").table(fact_table_path)
customer_transformation.show()
# ○ Normalize phone numbers to a standard format.

%pip install phonenumbers
import phonenumbers
from phonenumbers import parse, format_number, PhoneNumberFormat

from pyspark.sql.functions import *
from pyspark.sql.types import StringType

# Define a function to normalize phone numbers
def normalize_phone_number(phone):
    try:
        parsed_number = phonenumbers.parse(phone, "IN")  # "IN" is the country code for India
        formatted_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
        return formatted_number
    except phonenumbers.NumberParseException:
        return None

# Register the function as a UDF
normalize_phone_number_udf = udf(normalize_phone_number, StringType())

# Apply the UDF to normalize phone numbers
customer_transformation = customer_transformation.withColumn("normalized_phone", normalize_phone_number_udf(customer_transformation["phone_number"]))

customer_transformation.show()

# ○ Calculate customer tenure from signup_date.

# Calculate customer tenure in days 
customer_transformation = customer_transformation.withColumn("tenure_days", datediff(current_date(), customer_transformation["signup_date"])) 



customer_transformation.show()


# COMMAND ----------

# Check if the Delta table exists
customer_transformed_table_path = "incremental_load.default.customer_transformed_zoom_car"
fact_table_exists = spark._jsparkSession.catalog().tableExists(customer_transformed_table_path)
# Write the final staging_booking_delta table data back to the Delta table
customer_transformation.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(customer_transformed_table_path)

