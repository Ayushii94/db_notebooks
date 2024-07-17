# Databricks notebook source
# MAGIC %sql
# MAGIC use schema silver

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customers 
# MAGIC     SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC ALTER TABLE orders
# MAGIC     SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC ALTER TABLE payments
# MAGIC     SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC ALTER TABLE order_items
# MAGIC     SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

customers_df = spark.sql("SELECT * FROM table_changes('customers', '2024-07-17 13:56:00')")
display(customers_df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


