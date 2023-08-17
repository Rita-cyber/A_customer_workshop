-- Databricks notebook source
-- MAGIC %python
-- MAGIC df = spark.table("hive_metastore.default.Superstore_orders")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.columns

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime
-- MAGIC delta_df = df.filter(df.Order_Date > datetime(2019,8,10,12,0))
-- MAGIC display(delta_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC delta_df.createOrReplaceTempView('Shipment')

-- COMMAND ----------

SELECT * FROM Shipment

-- COMMAND ----------

SELECT * ,round(Sales,2)
FROM Shipment


-- COMMAND ----------

SELECT count(*), count(Order_ID), count(Order_Date), count(Customer_ID), count(Product_ID)
FROM Shipment

-- COMMAND ----------

SELECT count_if(Order_ID IS NULL) FROM Shipment;
SELECT count(*) FROM Shipment WHERE Order_ID IS NULL;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql.functions import col
-- MAGIC usersDF = spark.read.table("Shipment")
-- MAGIC
-- MAGIC usersDF.selectExpr("count_if(Order_ID IS NULL)")
-- MAGIC usersDF.where(col("Order_ID").isNull()).count()

-- COMMAND ----------

SELECT DISTINCT(*) FROM Shipment

-- COMMAND ----------

-- MAGIC %python
-- MAGIC usersDF.distinct().count()

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_order_in_Oceanside_cleaned
COMMENT "Sales orders in Oceanside."
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
AS
SELECT City, Order_Date, Customer_ID, Customer_Name, Product_ID, SUM(Sales) as sales, COUNT(Product_ID) as product_count
FROM Shipment
WHERE City = 'Oceanside'
GROUP BY City, Order_Date, Customer_ID, Customer_Name, Product_ID



-- COMMAND ----------

SELECT * FROM sales_order_in_SA_cleaned
