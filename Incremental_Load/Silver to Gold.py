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

customers_df = spark.sql("SELECT * FROM table_changes('customers', '2024-07-17 19:34:00')")
display(customers_df)

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load tables
orders = spark.sql(
    "SELECT * FROM table_changes('orders', '2024-07-17 19:34:00')"
)
order_items = spark.sql(
    "SELECT * FROM table_changes('order_items', '2024-07-17 19:34:00')"
)
returns = spark.table("returns")
products = spark.table("products")
addresses = spark.table("addresses")
vendors = spark.table("vendors")

# Compute CityStateTotals
city_state_totals = (
    orders
    .join(order_items, "OrderID")
    .join(products, order_items["ProductID"] == products["Product_ID"])
    .join(addresses, "CustomerID")
    .join(returns, "OrderID", "left")
    .groupBy("State", "City")
    .agg(
        F.countDistinct("orders.CustomerID").alias("TotalCustomers"),
        F.countDistinct("orders.OrderID").alias("TotalOrders"),
        F.count("order_items.ProductID").alias("TotalProductsSold"),
        F.sum(F.when(returns["OrderID"].isNotNull(), order_items["Quantity"]).otherwise(0)).alias("TotalProductsReturned"),
        (F.countDistinct("returns.OrderID") * 100.0 / F.countDistinct("orders.OrderID")).alias("ReturnRate"),
        F.sum(order_items["Quantity"] * products["Discounted_Price"]).alias("TotalRevenue"),
        F.avg(F.datediff("orders.ActualDeliveryDate", "orders.ShippingDate")).alias("AvgDeliveryTime")
    )
)

# Compute TopVendorByCityState
window_spec = Window.partitionBy("State", "City").orderBy(F.col("VendorRevenue").desc())
top_vendor_by_city_state = (
    orders
    .join(order_items, "OrderID")
    .join(products, order_items["ProductID"] == products["Product_ID"])
    .join(vendors, "VendorID")
    .join(addresses, "CustomerID")
    .groupBy("State", "City", "VendorName")
    .agg(F.sum(order_items["Quantity"] * products["Discounted_Price"]).alias("VendorRevenue"))
    .withColumn("Rank", F.rank().over(window_spec))
    .filter(F.col("Rank") == 1)
)

# Join results
final_result = (
    city_state_totals
    .join(top_vendor_by_city_state, ["State", "City"], "left")
    .select(
        "State",
        "City",
        "TotalCustomers",
        "TotalOrders",
        "TotalProductsSold",
        "TotalProductsReturned",
        F.round("ReturnRate", 2).alias("ReturnRate"),
        "TotalRevenue",
        F.col("VendorName").alias("TopVendorByRevenue"),
        F.round("AvgDeliveryTime", 2).alias("AvgDeliveryTime")
    )
    .orderBy("State", "City")
)

# Display the final result
display(final_result)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE gold.Regional_analysis
# MAGIC     SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

final_result.createOrReplaceTempView("final_result")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO msklenq.gold.Regional_analysis ra USING final_result fr
# MAGIC ON ra.City = fr.City
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET ra.TotalCustomers = ra.TotalCustomers+fr.TotalCustomers,
# MAGIC            ra.TotalOrders = ra.TotalOrders+fr.TotalOrders,
# MAGIC            ra.TotalProductsSold = ra.TotalProductsSold+fr.TotalProductsSold,
# MAGIC            ra.TotalProductsReturned = ra.TotalProductsReturned+fr.TotalProductsReturned,
# MAGIC            ra.ReturnRate = ra.ReturnRate,
# MAGIC            ra.TotalRevenue = ra.TotalRevenue+fr.TotalRevenue,
# MAGIC            ra.AvgDeliveryTime = fr.AvgDeliveryTime
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.Regional_analysis

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


