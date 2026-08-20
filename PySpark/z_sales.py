#!/usr/bin/env python
# coding: utf-8

# ## z_sales
# 
# 
# 

# #### Bronze

# In[17]:


from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_date


# In[18]:


data = [
    ("SKU-102", "Smartphone", "Pear", "Phone 15", 999, 15, "New"),
    ("SKU-205", "Laptop", "Macrosoft", "Surface 5", 1200, 8, "Refurbished"),
    ("SKU-301", "Headphones", "Soney", "WH-1000", 350, 22, "New"),
    ("SKU-102", "Smartphone", "Pear", "Phone 15", 999, 12, "New"),
    ("SKU-404", "Smartwatch", "Garmon", "Fenix 7", 700, 5, "New"),
    ("SKU-205", "Laptop", "Macrosoft", "Surface 5", 1100, 3, "Refurbished"),
    ("SKU-301", "Headphones", "Soney", "WH-1000", 350, 18, "New"),
    ("SKU-505", "Tablet", "Pear", "Pad Air", 600, 10, "New"),
    ("SKU-102", "Smartphone", "Pear", "Phone 15", 950, 7, "Open Box"),
    ("SKU-404", "Smartwatch", "Garmon", "Fenix 7", 700, 2, "New"),
    ("SKU-205", "Laptop", "Macrosoft", "Surface 5", 1200, 6, "New"),
    ("SKU-301", "Headphones", "Soney", "WH-1000", 320, 14, "Open Box"),
    ("SKU-505", "Tablet", "Pear", "Pad Air", 600, 4, "New"),
    ("SKU-102", "Smartphone", "Pear", "Phone 15", 999, 20, "New"),
    ("SKU-606", "Monitor", "Dellta", "UltraWide 34", 850, 9, "New"),
    ("SKU-404", "Smartwatch", "Garmon", "Fenix 7", 650, 1, "Open Box")
]


# In[19]:


schema = StructType([
    StructField("Product_ID", StringType(), False),
    StructField("Category", StringType(), False),
    StructField("Brand", StringType(), False),
    StructField("Model", StringType(), False),
    StructField("Price_USD", IntegerType(), False),
    StructField("Stock_Level", IntegerType(), False),
    StructField("Condition", StringType(), False)
])


# In[20]:


df_bronze = spark.createDataFrame(data, schema)


# In[21]:


df_bronze_with_date = df_bronze.withColumn("extraction_date", current_date())


# In[22]:


df_bronze_snake_case = (df_bronze_with_date
    .withColumnRenamed("Product_ID", "product_id")
    .withColumnRenamed("Category", "category")
    .withColumnRenamed("Brand", "brand")
    .withColumnRenamed("Model", "model")
    .withColumnRenamed("Price_USD", "price_usd")
    .withColumnRenamed("Stock_Level", "stock_level")
    .withColumnRenamed("Condition", "condition")
    .withColumnRenamed("extraction_date", "extraction_date")
)


# ```
#  display(df_bronze)
#  display(df_bronze_with_date)
# ```

# In[23]:


display(df_bronze_snake_case)


# In[24]:


df_bronze_snake_case.write.format("delta").mode("overwrite").save("abfss://bronze@dcdatahubsynapsessbdls.dfs.core.windows.net/sales")


# #### Silver

# In[9]:


df_silver = spark.read.format("delta").load("abfss://bronze@dcdatahubsynapsessbdls.dfs.core.windows.net/sales/")


# In[10]:


df_products = df_silver.select("product_id", "category", "brand", "model").distinct()


# In[11]:


df_sales = df_silver.select("product_id", "price_usd", "stock_level", "condition", "extraction_date")


# In[12]:


df_prices = df_silver.select("product_id", "price_usd", "extraction_date")


# In[13]:


df_inventory = df_silver.select("product_id", "stock_level", "extraction_date")


# In[14]:


df_conditions = df_silver.select("condition").distinct()


# df_products - urządzenia
# 
# df_sales - Historię tego, co się działo w sklepie.
# 
# df_prices - Kalendarz zmian cen.
# 
# df_inventory - Raport o ilościach
# 
# df_conditions - Słownik tego, w jakim stanie są przedmioty.

# ```
#  display(df_silver)
#  display(df_products)
#  display(df_sales)
#  display(df_prices)
#  display(df_inventory)
#  
# ```

# In[15]:


display(df_conditions)


# In[16]:


df_products.write.format("delta").mode("overwrite").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/products")

df_sales.write.format("delta").mode("overwrite").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/sales")

df_prices.write.format("delta").mode("overwrite").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/prices")

df_inventory.write.format("delta").mode("overwrite").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/inventory")

df_conditions.write.format("delta").mode("overwrite").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/conditions")

