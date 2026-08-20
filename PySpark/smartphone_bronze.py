#!/usr/bin/env python
# coding: utf-8

# ## smartphone_bronze
# 
# 
# 

# In[23]:


import requests
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType


# In[24]:


response = requests.get('https://api.restful-api.dev/objects')


# In[25]:


data_json = response.json()   # otwieramy to w json


# In[26]:


df_bronze = spark.createDataFrame(data_json)


# In[27]:


display(df_bronze)


# In[28]:


df_bronze.printSchema()


# In[29]:


df_bronze.write.format("delta").mode('overwrite').save('abfss://bronze@dcdatahubsynapsessbdls.dfs.core.windows.net/smartphone/')

