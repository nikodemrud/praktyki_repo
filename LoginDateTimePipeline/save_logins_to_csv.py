#!/usr/bin/env python
# coding: utf-8

# ## save_logins_to_csv
# 
# 
# 

# In[1]:


import json
from itertools import zip_longest
from pyspark.sql.types import StructType, StructField, StringType


# In[2]:


p_max_6 = '[]'
p_more_than_6 = '[]'


# In[3]:


short_logins = json.loads(p_max_6)
long_logins = json.loads(p_more_than_6)

ready_rows = list(zip_longest(short_logins, long_logins, fillvalue=None))
schema = StructType([
    StructField("max_6_letters", StringType(), True),
    StructField("more_than_6_letter", StringType(), True)
])

df = spark.createDataFrame(ready_rows, schema=schema)

df.coalesce(1).write.mode("overwrite").option("header", "true").csv("abfss://source@dcdatahubsynapsessbdls.dfs.core.windows.net/employee/login_results")


# 1. wyciągam dane i zamieniam na 2 zwykłe listy
# 
# 2. komputer bierze po jednym loginie z pierwszej i drugiej listy i łączy je w pary (tworzy gotowe wiersze do naszej tabeli)
# 
# 3. Zrób mi tabelę z dwiema kolumnami. Pierwsza ma się nazywać max_5_letters, a druga more_than_5_letter. W obu będą tylko teksty (StringType) i pozwalam na to, żeby niektóre kratki były puste (True na końcu)
# 
# 4. tworzę właściwą tabelę
# 
# 5. zapisuje tabelę 
# .coalesce(1) - wymusza, aby powatał jeden plik
#    
# .csv(link) - zapisuje plik w formacie csv 

# sprawdzenie poprawności pipelinu znajduje się w notaniku "employee" jako df_90. jest wykonywany po df_7.

# ```
# df_90 = df_4.filter(length(col("login")) <= 5)
# display(df_90)
# ```
