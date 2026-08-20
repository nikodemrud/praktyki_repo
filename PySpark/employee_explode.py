#!/usr/bin/env python
# coding: utf-8

# ## employee_explode
# 
# 
# 

# In[5]:


import json
from pyspark.sql.functions import col, explode, explode_outer
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType


# In[6]:


def normal_explode(df):
    df_exploded = df.withColumn("data", explode(col("value"))).drop("value")
    return df_exploded


# In[7]:


def outer_explode(df):
    df_exploded = df.withColumn("data", explode_outer(col("value"))).drop("value")
    return df_exploded


# In[8]:


def apply_transformation(df):
    df_transformation =(df
        .withColumn("department", col("data.Department"))
        .withColumn("display_name", col("data.DisplayName"))
        .withColumn("last_mod_date_time", col("data.LastModDateTime"))
        .withColumn("login_id", col("data.LoginID"))
        .withColumn("rec_id", col("data.RecId"))
        .drop("data") 
    )

    return df_transformation


# In[9]:


path = "abfss://source@dcdatahubsynapsessbdls.dfs.core.windows.net/ivanti_employee/ivanti_employee.json"
df_raw = spark.read.option("multiline", "true").json(path)


# In[10]:


df_out = outer_explode(df_raw)
df_outer = apply_transformation(df_out)


# In[11]:


display(df_outer)


# In[12]:


df_exp = normal_explode(df_raw)
df_explode = apply_transformation(df_exp)


# In[13]:


display(df_explode)


# explode: Wyciąga każdy produkt z listy do osobnego wiersza. Ale uwaga! Jeśli ktoś oddał pustą listę zakupów, ten wiersz całkowicie znika z tabeli.
# 
# explode_outer: Robi to samo, ale jest bezpieczniejsze. Jeśli lista zakupów jest pusta, wiersz zostaje w tabeli, a w miejscu produktu pojawia się po prostu null (pusto). Zazwyczaj w takich procesach używa się explode_outer, żeby nie zgubić danych.
