# Databricks notebook source
# MAGIC %md
# MAGIC #product col renaming

# COMMAND ----------

#product col renaming
product_df = spark.read.table("supplychain_catalog.bronze.product")
product_df = product_df.withColumnRenamed('P_ID','product_id').withColumnRenamed('Product_type','product_type').withColumnRenamed('Price','price')

#transportation col renaming
transportation_df = spark.read.table("supplychain_catalog.bronze.transportation")
transportation_df = transportation_df.withColumnRenamed('TRANSPORTATION_ID','transportation_id')\
.withColumnRenamed('MODES','modes')

#purchase order col renaming
purchase_orders_df = spark.read.table("supplychain_catalog.bronze.purchase_orders")
purchase_orders_df = purchase_orders_df.withColumnRenamed('PO_ID','purchase_order_id')\
.withColumnRenamed('P_ID','product_id')\
.withColumnRenamed('Availability','availability')\
.withColumnRenamed('Number_of_products_sold','number_of_products_sold')\
.withColumnRenamed('Revenue_generated','revenue_generated')\
.withColumnRenamed('Stock_levels','stock_levels')\
.withColumnRenamed('Lead_times','lead_times')\
.withColumnRenamed('Order_quantities','order_quantities')\
.withColumnRenamed('Shipping_times','shipping_times')\
.withColumnRenamed('Shipping_carriers','shipping_carriers')\
.withColumnRenamed('Shipping_costs','shipping_costs')\
.withColumnRenamed('Supplier_Id','supplier_id')\
.withColumnRenamed('Location','location')\
.withColumnRenamed('Lead_time','lead_time')\
.withColumnRenamed('Production_volumes','production_volumes')\
.withColumnRenamed('Manufacturing_lead_time','manufacturing_lead_time')\
.withColumnRenamed('Manufacturing_costs','manufacturing_costs')\
.withColumnRenamed('Inspection_results','inspection_results')\
.withColumnRenamed('Defect_rates','defect_rates')\
.withColumnRenamed('Transportation_id','transportation_id')\
.withColumnRenamed('Routes','routes')\
.withColumnRenamed('Costs','costs')


#supplier col renaming
supplier_df = spark.read.table("supplychain_catalog.bronze.supplier")
supplier_df = supplier_df.withColumnRenamed('S_ID','supplier_id')\
.withColumnRenamed('Tin_No','tin_no')\
.withColumnRenamed('Company_Name','company_name')\
.withColumnRenamed('Date_of_Reg','date_of_reg')\
.withColumnRenamed('SubCity','sub_city')\
.withColumnRenamed('Town','town')\
.withColumnRenamed('Telephone','telephone')\
.withColumnRenamed('Fax','fax')\
.withColumnRenamed('EMail','e_mail')\
.withColumnRenamed('Business_License_No','business_license_no')\
.withColumnRenamed('Business_Type','business_type')

# COMMAND ----------

# MAGIC %md 
# MAGIC schema validation

# COMMAND ----------


def schemaValidation(expected_schema,df):
	if not all(col in df.columns for col in expected_schema):
		print(f"schema mismatched for {expected_schema}")

#product
product_expected_schema = ['product_id','product_type','price']
schemaValidation(product_expected_schema,product_df)

#transportation
transportation_expected_schema = ['transportation_id','modes']
schemaValidation(transportation_expected_schema,transportation_df)

#supplier
supplier_expected_schema = ['supplier_id','tin_no','company_name','date_of_reg','sub_city','town','telephone','fax','e_mail','business_license_no','business_type']
schemaValidation(supplier_expected_schema,supplier_df)

#purchase order
purchase_order_expected_schema = ['purchase_order_id','product_id','availability','number_of_products_sold','revenue_generated','stock_levels','lead_times','order_quantities','shipping_times','shipping_carriers','shipping_costs','supplier_id','location','lead_time','production_volumes','manufacturing_lead_time','manufacturing_costs','inspection_results','defect_rates','transportation_id','routes','costs']
schemaValidation(purchase_order_expected_schema,purchase_orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality Framework - Total null counts for each table
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col,when,count

# COMMAND ----------

product_null_counts = product_df.select(
[count( when(col(c).isNull(),c)).alias(c+'_null_count') for c in product_df.columns]
)
display(product_null_counts)


# COMMAND ----------

purchase_orders_null_counts = purchase_orders_df.select(
[count( when(col(c).isNull(),c)).alias(c+'_null_count') for c in purchase_orders_df.columns]
)
display(purchase_orders_null_counts)



# COMMAND ----------

supplier_null_counts = supplier_df.select(
[count( when(col(c).isNull(),c)).alias(c+'_null_count') for c in supplier_df.columns]
)
display(supplier_null_counts)



# COMMAND ----------

transportation_null_counts = transportation_df.select(
[count( when(col(c).isNull(),c)).alias(c+'_null_count') for c in transportation_df.columns]
)
display(transportation_null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC #Data type validation

# COMMAND ----------

def validate_data_types(data, table_name):
    print(f"Validating data types for {table_name}...")
    for field in data.schema.fields:
        column_name = field.name
        column_type = str(field.dataType)
        
        # Check for Integer type
        if "int" in column_type.lower():
            data = data.withColumn(column_name, when(col(column_name).isNotNull(), col(column_name).cast("int")))
            print(f"{column_name}: Integer type validation applied.")
        
        # Check for Double type
        elif "double" in column_type.lower():
            data = data.withColumn(column_name, when(col(column_name).isNotNull(), col(column_name).cast("double")))
            print(f"{column_name}: Double type validation applied.")
        
        # Check for String type
        elif "string" in column_type.lower():
            data = data.withColumn(column_name, when(col(column_name).isNotNull(), col(column_name).cast("string")))
            print(f"{column_name}: String type validation applied.")
        
        # Check for Date type
        elif "date" in column_type.lower():
            data = data.withColumn(column_name, when(col(column_name).isNotNull(), col(column_name).cast("date")))
            print(f"{column_name}: Date type validation applied.")
        
        # Check for Boolean type
        elif "boolean" in column_type.lower():
            data = data.withColumn(column_name, when(col(column_name).isNotNull(), col(column_name).cast("boolean")))
            print(f"{column_name}: Boolean type validation applied.")
        
        # Add more data types as needed
    
    print(f"Validation for {table_name} completed.\n")
    return data


# Validate data types for each table
supplier_data = validate_data_types(supplier_df, "supplier_data")
transportation_data = validate_data_types(transportation_df, "transportation_data")
product_data = validate_data_types(product_df, "product_data")
purchase_orders_data = validate_data_types(purchase_orders_df, "purchase_orders_data")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Cleaning - Handling missing values

# COMMAND ----------

# Data Cleaning - Handling missing values
supplier_data = supplier_data.na.fill({"sub_city": "Unknown", "telephone": "999999999"})
transportation_data = transportation_data.na.fill({"modes": "Unknown"})
product_data = product_data.na.fill({"product_id": "Unknown", "product_type": "Unknown"})
purchase_order_data = purchase_orders_data.na.fill({"purchase_order_id": 0, "product_id": 0, "order_quantities": 0})

# COMMAND ----------

# MAGIC %md
# MAGIC # business rules

# COMMAND ----------

supplier_data = supplier_data.withColumn("negotiation_score", 
                                         when(col("business_type") == "Printing Press", 0.9)
                                         .when(col("business_type") == "Stationery Materials", 0.8)
                                         .when(col("business_type") == "Software Development and Design", 0.95)
                                         .otherwise(0.75))

supplier_data = supplier_data.withColumn("defect_quality", 
                                         when(col("business_type").isin("Detergents", "Sanitary Items"), "High")
                                         .when(col("business_type").isin("Building and Construction Materials", 
                                                                       "Metal and Metal Products"), "Medium")
                                         .otherwise("Low"))

# Business Rule Application (Silver Layer) - Transportation Data
transportation_data = transportation_data.withColumn("priority",
                                                     when(col("modes") == "road", 1)
                                                     .when(col("modes") == "Sea", 2)
                                                     .when(col("modes") == "Air", 3)
                                                     .otherwise(0))


# Business Rule Application (Silver Layer) - Product Data
product_data = product_data.withColumn("price_category",
                                       when(col("price") < 50, "Low Price")
                                       .when((col("price") >= 50) & (col("price") < 200), "Medium Price")
                                       .when(col("price") >= 200, "High Price")
                                       .otherwise("Unknown"))


# Business Rule Application (Silver Layer) - Purchase Order Data
purchase_orders_data = purchase_orders_data.withColumn("total_cost", col("order_quantities") * col("costs"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load to silver tables

# COMMAND ----------

product_data.write.mode("overwrite").saveAsTable("supplychain_catalog.silver.product")

transportation_data.write.mode("overwrite").saveAsTable("supplychain_catalog.silver.transportation")

supplier_data.write.mode("overwrite").saveAsTable("supplychain_catalog.silver.supplier")

purchase_orders_data.write.mode("overwrite").saveAsTable("supplychain_catalog.silver.purchase_orders")
