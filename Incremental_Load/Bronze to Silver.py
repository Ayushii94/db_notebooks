# Databricks notebook source
# MAGIC %sql
# MAGIC use schema bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO customers
# MAGIC FROM "/mnt/ayushi_ecom/Updated_Customers"
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS("header" = "true", "inferSchema" = "true", "mergeSchema" = "true", "timestampFormat" = "dd-MM-yyyy HH.mm")
# MAGIC COPY_OPTIONS("mergeSchema" = "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO orders
# MAGIC FROM "/mnt/ayushi_ecom/Updated_Orders"
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS("header" = "true", "inferSchema" = "true", "mergeSchema" = "true", "timestampFormat" = "dd-MM-yyyy HH.mm")
# MAGIC COPY_OPTIONS("mergeSchema" = "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO order_items
# MAGIC FROM "/mnt/ayushi_ecom/Updated_OrdersItems"
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS("header" = "true", "inferSchema" = "true", "mergeSchema" = "true", "timestampFormat" = "dd-MM-yyyy HH.mm")
# MAGIC COPY_OPTIONS("mergeSchema" = "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO payments
# MAGIC FROM "/mnt/ayushi_ecom/Updated_Payments"
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS("header" = "true", "inferSchema" = "true", "mergeSchema" = "true", "timestampFormat" = "dd-MM-yyyy HH.mm")
# MAGIC COPY_OPTIONS("mergeSchema" = "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use a timestamp that is before or at the latest available timestamp
# MAGIC SELECT * FROM table_changes('customers', '2024-07-17 12:00:01')

# COMMAND ----------

customers_df = spark.sql("SELECT * FROM table_changes('customers', '2024-07-17 12:00:01')")
display(customers_df)

# COMMAND ----------

from pyspark.sql.functions import col, sum

missing_values = customers_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in customers_df.columns])
display(missing_values)

# Check for Duplicates
duplicate_count = customers_df.groupBy(customers_df.columns).count().filter("count > 1").count()
print(f"Number of duplicate rows: {duplicate_count}")

# Summary Statistics
summary_stats = customers_df.describe()
display(summary_stats)

# Check for duplicates in the customer_id column
duplicates = customers_df.groupBy("CustomerID").count().filter("count > 1")

# Display the duplicate customer_ids
display(duplicates)

# Optionally, count the number of duplicate customer_ids
duplicate_count = duplicates.count()
print(f"Number of duplicate customer_ids: {duplicate_count}")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.customers AS sc
# MAGIC USING customers_df AS bc
# MAGIC ON sc.customer_id = bc.customer_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   UPDATE SET sc.* = bc.*;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver.customers SET TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true,
# MAGIC   delta.enableChangeDataFeed = true
# MAGIC );

# COMMAND ----------


