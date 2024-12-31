# Databricks notebook source
product_df = spark.read.table("cn.silver.product")
supplier_df = spark.read.table("cn.silver.supplier")
transportation_df = spark.read.table("cn.silver.transportation")
purchase_order_df = spark.read.table("cn.silver.purchase_order")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Advanced analytics:
# MAGIC Example 1: Calculate the average number of services provided by suppliers

# COMMAND ----------

supplier_service_count = supplier_df.groupBy('company_name').agg(
countDistinct('business_type').alias('service_count').
).agg(avg('service_count').alias('avg_service_count'))

# COMMAND ----------

# MAGIC %md
# MAGIC Example 2: Identify top suppliers by the number of materials provided

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC p.Product_type,s.Business_Type,s.Company_Name,
# MAGIC sum(cast(Number_of_products_sold as int)) as num_of_prod
# MAGIC from supplychains.purchaseorders po 
# MAGIC inner join supplychains.product  p on p.[ P_ID] =po.P_ID
# MAGIC inner join supplychains.supplier s on s.S_ID = po.Supplier_Id
# MAGIC --where business_type = 'Constrution materialsConstruction'
# MAGIC group by p.Product_type,s.Company_Name,s.Business_Type
# MAGIC order by p.Product_type,s.Business_Type,s.Company_Name
