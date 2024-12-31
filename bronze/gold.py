# Databricks notebook source
product_df = spark.read.table("supplychain_catalog.silver.product")
supplier_df = spark.read.table("supplychain_catalog.silver.supplier")
transportation_df = spark.read.table("supplychain_catalog.silver.transportation")
purchase_orders_df = spark.read.table("supplychain_catalog.silver.purchase_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Advanced analytics:
# MAGIC Example 1: Calculate the average number of services provided by suppliers

# COMMAND ----------

from pyspark.sql.functions import  countDistinct, avg

# COMMAND ----------

supplier_service_count = supplier_df.groupBy('company_name').agg(
countDistinct('business_type').alias('service_count')
)
#.agg(avg('service_count').alias('avg_service_count'))

display(supplier_service_count)

# COMMAND ----------

# MAGIC %md
# MAGIC Example 2: Identify top suppliers by the number of materials provided

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC p.Product_type,s.Business_Type,s.Company_Name,
# MAGIC sum(cast(Number_of_products_sold as int)) as num_of_prod
# MAGIC from supplychains.purchaseorders po 
# MAGIC inner join supplychains.product  p on p.p_id =po.P_ID
# MAGIC inner join supplychains.supplier s on s.S_ID = po.Supplier_Id
# MAGIC --where business_type = 'Constrution materialsConstruction'
# MAGIC group by p.Product_type,s.Company_Name,s.Business_Type
# MAGIC order by p.Product_type,s.Business_Type,s.Company_Name
