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
# MAGIC FROM "/mnt/ayushi_ecom/Updated_OrderItems"
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

cust_df = spark.sql("SELECT * FROM table_changes('customers', '2024-07-17 12:00:01')")
display(cust_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
