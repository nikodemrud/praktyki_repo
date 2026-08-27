#!/usr/bin/env python
# coding: utf-8

# ## cafe
# 
# 
# 

# In[70]:


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, when, lit, lower, concat_ws, udf, avg, desc, dense_rank
from pyspark.sql.window import Window


# In[41]:


cafe_schema = StructType([
    StructField("Transaction ID", StringType(), False), 
    StructField("Item", StringType(), True),
    StructField("Quantity", StringType(), True),
    StructField("Price Per Unit", StringType(), True),
    StructField("Total Spent", StringType(), True),
    StructField("Payment Method", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Transaction Date", StringType(), True)
])


# In[42]:


source_cafe = spark.read.csv("abfss://source@dcdatahubsynapsessbdls.dfs.core.windows.net/new_data/dirty_cafe_sales.csv", header=True, schema=cafe_schema, sep=",")


# In[43]:


source_cafe = (source_cafe
       .withColumnRenamed("Transaction ID", "transaction_id")
       .withColumnRenamed("Item", "item")
       .withColumnRenamed("Quantity", "quantity")
       .withColumnRenamed("Price Per Unit", "price_per_unit")
       .withColumnRenamed("Total Spent", "total_spent")
       .withColumnRenamed("Payment Method", "payment_method")
       .withColumnRenamed("Location", "location")
       .withColumnRenamed("Transaction Date", "transaction_date")
)


# In[44]:


display(source_cafe)
source_cafe.printSchema()


# In[45]:


source_cafe.write.format("delta").mode("overwrite").save("abfss://source@dcdatahubsynapsessbdls.dfs.core.windows.net/new_data/")


# In[46]:


source_cafe = spark.read.format("delta").load("abfss://source@dcdatahubsynapsessbdls.dfs.core.windows.net/new_data/")


# In[47]:


bronze_cafe = (source_cafe
    .withColumn("total_spent", when(col("total_spent") == "ERROR", "0").otherwise(col("total_spent")))
    .withColumn("quantity", when(col("quantity") == "ERROR", "0").otherwise(col("quantity")))
    .withColumn("price_per_unit", when(col("price_per_unit") == "ERROR", "0").otherwise(col("price_per_unit")))
)


# In[48]:


bronze_cafe = (bronze_cafe
    .withColumn("total_spent", col("total_spent").cast(FloatType()))
    .withColumn("price_per_unit", col("price_per_unit").cast(FloatType()))
    .withColumn("quantity", col("quantity").cast(IntegerType()))
)


# In[49]:


bronze_cafe = bronze_cafe.withColumn("is_current", lit(1))


# In[50]:


display(bronze_cafe)


# In[51]:


bronze_cafe.write.format("delta").mode("overwrite").save("abfss://bronze@dcdatahubsynapsessbdls.dfs.core.windows.net/cafe/")


# In[52]:


bronze_cafe = spark.read.format("delta").load("abfss://bronze@dcdatahubsynapsessbdls.dfs.core.windows.net/cafe/")


# In[53]:


silver_cafe = bronze_cafe.dropDuplicates(["transaction_id"])


# In[54]:


silver_cafe = silver_cafe.filter((col("quantity") > 0) | (col("total_spent") > 0))


# In[55]:


silver_cafe = (silver_cafe
    .withColumn(
        "payment_method", 
        when(col("payment_method").isNull() | col("payment_method").isin("UNKNOWN", "ERROR"), lit("Unknown"))
        .otherwise(col("payment_method"))
    )
    .withColumn(
        "location", 
        when(col("location").isNull() | col("location").isin("UNKNOWN", "ERROR"), lit("Unknown"))
        .otherwise(col("location"))
    )
    .withColumn(
        "transaction_date", 
        when(col("transaction_date").isNull() | col("transaction_date").isin("UNKNOWN", "ERROR"), lit("Unknown"))
        .otherwise(col("transaction_date"))
    )
)


# In[56]:


silver_cafe = (silver_cafe
    .withColumn("item", lower(col("item")))
    .withColumn("payment_method", lower(col("payment_method")))
    .withColumn("location", lower(col("location")))
    .withColumn("transaction_date", lower(col("transaction_date")))
)


# In[57]:


silver_cafe = silver_cafe.withColumn("receipt_id", concat_ws("-", "transaction_date", "location", "transaction_id"))


# In[58]:


bonus = udf(lambda total_spent: total_spent + 1.50 if total_spent is not None else None, FloatType())
silver_cafe = silver_cafe.withColumn("final_total", bonus(col("total_spent")))


# Rozbij swój obecny DataFrame na dwie osobne tabele:df_sales (kolumny: transaction_id, item, quantity, total_spent)  df_operations (kolumny: transaction_id, location, payment_method, transaction_date)  Następnie połącz je ze sobą z powrotem w jeden główny zbiór na podstawie kolumny transaction_id
# 
# df_15 = personal_data.join(employee_data, on="login", how="inner")
# 
# employee_data = df_4.select("position", "gross_salary", "login")

# In[59]:


df_sales = silver_cafe.select("transaction_id", "item", "quantity", "total_spent")
df_operations  =silver_cafe.select("transaction_id", "location", "payment_method", "transaction_date")


# In[60]:


silver_cafe_transactions = df_sales.join(df_operations, on= "transaction_id", how= "inner")


# In[61]:


display(silver_cafe)


# In[62]:


silver_cafe.write.format("delta").mode("overwrite").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/cafe/")
silver_cafe_transactions.write.format("delta").mode("overwrite").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/cafe_transactions/")


# In[63]:


silver_cafe = spark.read.format("delta").load("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/cafe/")


# Cel: Stwórz DataFrame o nazwie gold_avg_sales_by_location, który pokaże średnią wartość rachunku (final_total lub total_spent) dla każdej kawiarni (location).

# In[64]:


gold_avg_by_loc = silver_cafe.groupBy("location").agg(
    avg("total_spent").alias("avg_total_spent"),
    avg("final_total").alias("avg_final_total")
)


# In[65]:


display(gold_avg_by_loc)


# Stwórz nową kolumnę order_ranking, która ułoży zamówienia w ranking dla każdej lokalizacji z osobna (gdzie 1 to najwyższy rachunek w danej kawiarni).

# In[71]:


window_spec = Window.partitionBy("location").orderBy(col("final_total").desc())
gold_ranked_orders = silver_cafe.withColumn("order_ranking", dense_rank().over(window_spec))


# In[72]:


display(gold_ranked_orders)


# Wyciągnij do osobnego widoku gold_premium_customers tylko takie transakcje, które spełniają oba te warunki naraz.

# In[74]:


gold_premium_customers = silver_cafe.filter((col("payment_method") == "credit card") & (col("final_total") > 15))


# In[75]:


display(gold_premium_customers)

