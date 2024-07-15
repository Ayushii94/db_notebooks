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
