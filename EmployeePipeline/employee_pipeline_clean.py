#!/usr/bin/env python
# coding: utf-8

# ## employee_pipeline_clean
# 
# 
# 

# In[21]:


from pyspark.sql.functions import col, lit


# ##### SOURCE

# In[22]:


source_df = spark.read.csv("abfss://source@dcdatahubsynapsessbdls.dfs.core.windows.net/employee/employee.csv", header=True, inferSchema=True, sep=";")


# ##### BRONZE

# In[25]:


source_df.write.format("delta").mode("overwrite").save("abfss://bronze@dcdatahubsynapsessbdls.dfs.core.windows.net/employee")


# ##### SILVER

# In[26]:


bronze_df = spark.read.format("delta").load("abfss://bronze@dcdatahubsynapsessbdls.dfs.core.windows.net/employee")


# In[28]:


silver_df = bronze_df.select(
    col("Name").alias("name"),
    col("Surname").alias("surname"),
    col("Login").alias("login"),
    col("Age").alias("age"),
    col("Position").alias("position"),
    col("GrossSalary").alias("gross_salary"),
    col("Login").alias("primary_key"),
    lit(1).alias("is_current")
)


# In[29]:


silver_df.write.format("delta").mode("overwrite").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/employee")


# ##### GOLD

# In[31]:


employee_personal_df = silver_df.select("name", "surname", "age", "primary_key")


# In[32]:


employee_company_df = (silver_df
    .select("login", "gross_salary", "primary_key")
    .withColumn("net_salary", col("gross_salary")*0.75)
)


# In[33]:


gold_df = employee_personal_df.join(employee_company_df, on="primary_key") 


# In[37]:


employee_personal_df.write.format("delta").mode("overwrite").save("abfss://gold@dcdatahubsynapsessbdls.dfs.core.windows.net/employee_personal")


# In[38]:


employee_company_df.write.format("delta").mode("overwrite").save("abfss://gold@dcdatahubsynapsessbdls.dfs.core.windows.net/employee_company")


# In[39]:


gold_df.write.format("delta").mode("overwrite").save("abfss://gold@dcdatahubsynapsessbdls.dfs.core.windows.net/employee")

