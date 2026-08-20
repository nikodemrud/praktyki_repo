#!/usr/bin/env python
# coding: utf-8

# ## employee_date
# 
# 
# 

# In[37]:


p_trigger_date = None


# In[38]:


import json
from pyspark.sql.functions import col, lit, current_date


# ##### SOURCE

# In[39]:


source_df = spark.read.csv("abfss://source@dcdatahubsynapsessbdls.dfs.core.windows.net/employee/employee.csv", header=True, inferSchema=True, sep=";")


# ##### BRONZE

# In[40]:


bronze_df = source_df


# ##### SILVER

# In[41]:


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


# ##### GOLD

# In[42]:


employee_personal_df = silver_df.select("name", "surname", "age", "primary_key")


# In[43]:


employee_company_df = (silver_df
    .select("login", "gross_salary", "primary_key")
    .withColumn("net_salary", col("gross_salary")*0.75)
)


# In[44]:


gold_df = employee_personal_df.join(employee_company_df, on="primary_key") 


# In[45]:


gold_df_with_timestamp = gold_df.withColumn("ingestion_time", current_date())


# In[46]:


gold_df_filtered = gold_df_with_timestamp.filter(col("ingestion_time") == p_trigger_date)


# In[ ]:


login_list = [row.login for row in gold_df_filtered.select('login').collect()]


# In[ ]:


print(login_list)


# In[ ]:


mssparkutils.notebook.exit(
    json.dumps(
        {
            "login_list": login_list
        }
    )
)

