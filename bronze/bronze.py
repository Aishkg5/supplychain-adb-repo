# Databricks notebook source
bronze_path = 'abfss://bronze@supplychainadlsacc.dfs.core.windows.net/'

# COMMAND ----------

product_df = spark.read.format('parquet').load(f'{bronze_path}/product')
product_df.write.mode("append").saveAsTable("supplychain_catalog.bronze.product")

purchase_orders_df = spark.read.format('parquet').load(f'{bronze_path}/purchaseorders')
purchase_orders_df.write.mode("append").saveAsTable("supplychain_catalog.bronze.purchase_orders")

supplier_df = spark.read.format('parquet').load(f'{bronze_path}/supplier')
supplier_df.write.mode("append").saveAsTable("supplychain_catalog.bronze.supplier")

transportation_df = spark.read.format('parquet').load(f'{bronze_path}/transportation')
transportation_df.write.mode("append").saveAsTable("supplychain_catalog.bronze.transportation")
