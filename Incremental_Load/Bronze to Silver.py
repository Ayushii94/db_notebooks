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
# MAGIC SELECT * FROM table_changes('customers', '2024-07-17 19:30:00')

# COMMAND ----------

customers_df = spark.sql("SELECT * FROM table_changes('customers', '2024-07-17 19:30:00')")
display(customers_df)

# COMMAND ----------

customers_df = customers_df.drop("_change_type", "_commit_version", "_commit_timestamp")
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

# Assuming customers_df is your DataFrame
customers_df.createOrReplaceTempView("customers_df")

# Enable schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO msklenq.silver.customers USING customers_df
# MAGIC ON msklenq.silver.customers.CustomerId = customers_df.CustomerId
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.customers

# COMMAND ----------

orders_df = spark.sql("SELECT * FROM table_changes('orders', '2024-07-17 19:30:00')")
display(orders_df)

# COMMAND ----------

from pyspark.sql.functions import col, sum

missing_values = orders_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in orders_df.columns])
display(missing_values)

# Check for Duplicates
duplicate_count = orders_df.groupBy(orders_df.columns).count().filter("count > 1").count()
print(f"Number of duplicate rows: {duplicate_count}")

# Summary Statistics
summary_stats = orders_df.describe()
display(summary_stats)

# Check for duplicates in the OrderID column
duplicates = orders_df.groupBy("OrderID").count().filter("count > 1")

# Display the duplicate OrderID
display(duplicates)

# Optionally, count the number of duplicate OrderID
duplicate_count = duplicates.count()
print(f"Number of duplicate OrderID: {duplicate_count}")

# COMMAND ----------

orders_df = orders_df.drop("_change_type", "_commit_version", "_commit_timestamp")

# Assuming orders_df is your DataFrame
orders_df.createOrReplaceTempView("orders_df")

# Enable schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO msklenq.silver.orders USING orders_df
# MAGIC ON msklenq.silver.orders.OrderID = orders_df.OrderID
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------


payments_df = spark.sql("SELECT * FROM table_changes('payments', '2024-07-17 19:30:00')")
display(payments_df)

# COMMAND ----------

payments_df = payments_df.drop("_change_type", "_commit_version", "_commit_timestamp")

# Assuming payments_df is your DataFrame
payments_df.createOrReplaceTempView("payments_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO msklenq.silver.payments USING payments_df
# MAGIC ON msklenq.silver.payments.PaymentID = payments_df.PaymentID
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------


orderitems_df = spark.sql("SELECT * FROM table_changes('order_items', '2024-07-17 19:30:00')")
display(orderitems_df)

# COMMAND ----------

orderitems_df = orderitems_df.drop("_change_type", "_commit_version", "_commit_timestamp")

# Assuming orderitems_df is your DataFrame
orderitems_df.createOrReplaceTempView("orderitems_df")

# COMMAND ----------

orderitems_dff.createOrReplaceTempView("orderitems_dff")
duplicates_df = spark.sql("""
    SELECT *
    FROM (
        SELECT *, COUNT(*) OVER (PARTITION BY OrderItemID) AS duplicate_count
        FROM orderitems_dff
    ) subquery
    WHERE duplicate_count > 1
""")
display(duplicates_df)

# COMMAND ----------

# Drop duplicates from orderitems_df based on OrderItemID
orderitems_dff = orderitems_df.join(duplicates_df, on="OrderItemID", how="left_anti")

# Display the updated orderitems_df
display(orderitems_dff)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO msklenq.silver.order_items USING orderitems_dff
# MAGIC ON msklenq.silver.order_items.OrderItemID = orderitems_dff.OrderItemID
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------


