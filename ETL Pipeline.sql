-- Databricks notebook source
CREATE STREAMING LIVE TABLE Superstore_orders
COMMENT "The customers buying finished products, ingested from aws S3 Bucket."
TBLPROPERTIES ("myCompanyPipeline.quality" = "mapping")
AS SELECT * FROM cloud_files("/raw_data/Superstore_orders.csv/", "csv");

-- COMMAND ----------

CREATE LIVE TABLE sales_order_in_SA_cleaned
COMMENT "Sales orders in SA."
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
AS
SELECT City, Order_Date, Customer_ID, Customer_Name, Product_ID, SUM(Sales) as sales, COUNT(Product_ID) as product_count
FROM LIVE.Superstore_orders 
WHERE city = 'San Francisco'
GROUP BY City, Order_Date, Customer_ID, Customer_Name, Product_ID
