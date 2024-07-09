# Databricks notebook source
# MAGIC %sql
# MAGIC create schema gold

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select OrderID, sum(Sale) as Bill
# MAGIC from
# MAGIC (select o.OrderID, p.Product_ID, ot.Quantity, p.Actual_Price , ot.Quantity*p.Actual_Price as Sale
# MAGIC from orders o join
# MAGIC order_items ot on o.OrderID = ot.OrderID
# MAGIC join products p on ot.ProductID = p.Product_ID) X
# MAGIC group by OrderID
