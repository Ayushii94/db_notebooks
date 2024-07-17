# Databricks notebook source
# MAGIC %sql
# MAGIC use schema silver

# COMMAND ----------

mount_point = "/mnt/ayushi_ecom"
# Display the contents of the mounted directory to verify
display(dbutils.fs.ls("/mnt/ayushi_ecom/Updated_Customers"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO customers
# MAGIC FROM "/mnt/ayushi_ecom/Updated_Customers"
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS("header" = "true", "inferSchema" = "true", "mergeSchema" = "true", "timestampFormat" = "dd-MM-yyyy HH.mm")
# MAGIC COPY_OPTIONS("mergeSchema" = "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers
# MAGIC order by email 

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table ordereg;
# MAGIC use schema bronze;
# MAGIC create table ordereg;
# MAGIC ALTER TABLE ordereg SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COPY INTO ordereg
# MAGIC FROM "/mnt/ayushi_ecom/Updated_Orders"
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS("header" = "true", "inferSchema" = "true", "mergeSchema" = "true", "timestampFormat" = "dd-MM-yyyy HH.mm")
# MAGIC COPY_OPTIONS("mergeSchema" = "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ordereg

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use a timestamp that is before or at the latest available timestamp
# MAGIC SELECT * FROM table_changes('ordereg', '2024-07-16 13:40:01', '2024-07-16 13:45:01')

# COMMAND ----------

from pyspark.sql.functions import to_utc_timestamp, from_utc_timestamp, current_timestamp, expr, date_sub
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, TimestampType

# Define your time zone
time_zone = "America/New_York"

# Get current time in the specific time zone
current_time_tz = spark.createDataFrame([Row(current_time_tz=from_utc_timestamp(current_timestamp(), time_zone))]).collect()[0]['current_time_tz']

# Calculate Time with Specific Time Zone - 1 day
tz_minus_1_day = spark.createDataFrame([Row(tz_minus_1_day=date_sub(from_utc_timestamp(current_timestamp(), time_zone), 1))]).collect()[0]['tz_minus_1_day']

# Calculate Time with Specific Time Zone - 2 hours
tz_minus_2_hours = spark.createDataFrame([Row(tz_minus_2_hours=expr(f"from_utc_timestamp(current_timestamp(), '{time_zone}') - INTERVAL 2 HOURS"))]).collect()[0]['tz_minus_2_hours']

# Print results
print(f"Current Time in {time_zone}: {current_time_tz}")
print(f"Time in {time_zone} - 1 Day: {tz_minus_1_day}")
print(f"Time in {time_zone} - 2 Hours: {tz_minus_2_hours}")

# COMMAND ----------

from pyspark.sql.functions import to_utc_timestamp, from_utc_timestamp, current_timestamp, expr, date_sub
from pyspark.sql.types import StructType, StructField, TimestampType

# Define your time zone
time_zone = "America/New_York"

# Define the schema for the DataFrame
schema = StructType([StructField("current_time_tz", TimestampType())])

# Get current time in the specific time zone
current_time_tz = spark.sql(f"SELECT from_utc_timestamp(current_timestamp(), '{time_zone}') as current_time_tz").collect()[0]['current_time_tz']

# Calculate Time with Specific Time Zone - 1 day
tz_minus_1_day = spark.sql(f"SELECT date_sub(from_utc_timestamp(current_timestamp(), '{time_zone}'), 1) as tz_minus_1_day").collect()[0]['tz_minus_1_day']

# Calculate Time with Specific Time Zone - 2 hours
tz_minus_2_hours = spark.sql(f"SELECT from_utc_timestamp(current_timestamp(), '{time_zone}') - INTERVAL 2 HOURS as tz_minus_2_hours").collect()[0]['tz_minus_2_hours']

# Print results
print(f"Current Time in {time_zone}: {current_time_tz}")
print(f"Time in {time_zone} - 1 Day: {tz_minus_1_day}")
print(f"Time in {time_zone} - 2 Hours: {tz_minus_2_hours}")
