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
# MAGIC (select o.OrderID, p.Product_ID, ot.Quantity, p.Discounted_Price , ot.Quantity*p.Discounted_Price as Sale
# MAGIC from orders o join
# MAGIC order_items ot on o.OrderID = ot.OrderID
# MAGIC join products p on ot.ProductID = p.Product_ID) X
# MAGIC group by OrderID

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH OrderTotals AS (
# MAGIC     SELECT
# MAGIC         o.CustomerID,
# MAGIC         COUNT(DISTINCT o.OrderID) AS NumOrders,
# MAGIC         SUM(oi.Quantity) AS BasketSize,
# MAGIC         MIN(o.OrderDate) AS FirstOrderDate,
# MAGIC         MAX(o.OrderDate) AS LastOrderDate
# MAGIC     FROM
# MAGIC         orders o
# MAGIC         JOIN order_items oi ON o.OrderID = oi.OrderID
# MAGIC     GROUP BY
# MAGIC         o.CustomerID
# MAGIC ),
# MAGIC CustomerAnalysis AS (
# MAGIC     SELECT
# MAGIC         ot.CustomerID,
# MAGIC         c.Email,
# MAGIC         ot.NumOrders,
# MAGIC         round(ot.BasketSize / ot.NumOrders,0) AS AvgBasketSize,
# MAGIC         round(DATEDIFF(ot.LastOrderDate, ot.FirstOrderDate) / NULLIF(ot.NumOrders - 1, 0),0) AS PurchaseFrequency
# MAGIC     FROM
# MAGIC         OrderTotals ot
# MAGIC         JOIN customers c ON ot.CustomerID = c.CustomerID
# MAGIC )
# MAGIC SELECT
# MAGIC     CustomerID,
# MAGIC     Email,
# MAGIC     NumOrders,
# MAGIC     AvgBasketSize,
# MAGIC     PurchaseFrequency
# MAGIC FROM
# MAGIC     CustomerAnalysis
# MAGIC ORDER BY
# MAGIC     NumOrders DESC;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct, sum, min, max, datediff, round

# Load data
orders = spark.table("orders")
order_items = spark.table("order_items")
customers = spark.table("customers")

# Compute Order Totals
order_totals = orders.join(order_items, orders.OrderID == order_items.OrderID) \
    .groupBy("CustomerID") \
    .agg(
        countDistinct("orders.OrderID").alias("NumOrders"),
        sum("order_items.Quantity").alias("BasketSize"),
        min("orders.OrderDate").alias("FirstOrderDate"),
        max("orders.OrderDate").alias("LastOrderDate")
    )

# Compute Customer Analysis
customer_analysis = order_totals.join(customers, "CustomerID") \
    .select(
        col("CustomerID"),
        col("Email"),
        col("NumOrders"),
        round(col("BasketSize") / col("NumOrders"), 0).alias("AvgBasketSize"),
        round(datediff(col("LastOrderDate"), col("FirstOrderDate")) / (col("NumOrders") - 1), 0).alias("PurchaseFrequency")
    )

# Sort and display results
display(customer_analysis.orderBy(col("NumOrders").desc()))

# COMMAND ----------

customer_analysis.write.format("delta").mode("overwrite").saveAsTable("gold.customer_analysis")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.customer_analysis
# MAGIC order by Email

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH VendorOrderTotals AS (
# MAGIC     SELECT
# MAGIC         o.VendorID,
# MAGIC         COUNT(DISTINCT o.OrderID) AS NumOrders,
# MAGIC         SUM(oi.Quantity) AS TotalItems,
# MAGIC         MIN(o.OrderDate) AS FirstOrderDate,
# MAGIC         MAX(o.OrderDate) AS LastOrderDate
# MAGIC     FROM
# MAGIC         Orders o
# MAGIC         JOIN Order_Items oi ON o.OrderID = oi.OrderID
# MAGIC     GROUP BY
# MAGIC         o.VendorID
# MAGIC ),
# MAGIC VendorAnalysis AS (
# MAGIC     SELECT
# MAGIC         vot.VendorID,
# MAGIC         v.VendorName,
# MAGIC         vot.NumOrders,
# MAGIC         vot.TotalItems / vot.NumOrders AS AvgItemsPerOrder,
# MAGIC         DATEDIFF(vot.LastOrderDate, vot.FirstOrderDate) / NULLIF(vot.NumOrders - 1, 0) AS OrderFrequency
# MAGIC     FROM
# MAGIC         VendorOrderTotals vot
# MAGIC         JOIN Vendors v ON vot.VendorID = v.VendorID
# MAGIC )
# MAGIC SELECT
# MAGIC     VendorID,
# MAGIC     VendorName,
# MAGIC     NumOrders,
# MAGIC     AvgItemsPerOrder,
# MAGIC     OrderFrequency
# MAGIC FROM
# MAGIC     VendorAnalysis
# MAGIC ORDER BY
# MAGIC     NumOrders DESC;
