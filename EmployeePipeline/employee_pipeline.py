#!/usr/bin/env python
# coding: utf-8

# ## employee_pipeline
# 
# 
# 

# In[3]:


from pyspark.sql.functions import col, lit, current_timestamp


# ##### SOURCE

# Read 'employee.csv' file as a dataframe (with column names and original schema) from the source container.

# In[4]:


source_df = spark.read.csv("abfss://source@dcdatahubsynapsessbdls.dfs.core.windows.net/employee/employee.csv", header=True, inferSchema=True, sep=";")


# inferSchema - pobiera schema (z orginalnych danych. od razu ustawi nam np: integer)

# Display df.

# In[5]:


display(source_df)


# Print schema.

# In[6]:


source_df.printSchema()


# ##### BRONZE

# Save df as 'employee' delta table.

# In[7]:


source_df.write.format("delta").mode("overwrite").save("abfss://bronze@dcdatahubsynapsessbdls.dfs.core.windows.net/employee")


# ##### SILVER

# Read brozne delta table.

# In[8]:


bronze_df = spark.read.format("delta").load("abfss://bronze@dcdatahubsynapsessbdls.dfs.core.windows.net/employee")


# In[9]:


display(bronze_df)


# Change column names to snake_case. Add 'primary_key' column (login). Add 'is_current' column equals to 1.

# In[10]:


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


# ```
# # It's correct, but try to use select instead
# bronze_df_snake_case = (bronze_df  # it's a silver transformation, so it would be better to name it as silver_df
#     .withColumnRenamed("Name", "name")
#     .withColumnRenamed("Surname", "surname")
#     .withColumnRenamed("Login", "login")
#     .withColumnRenamed("Age", "age")
#     .withColumnRenamed("Position", "position")
#     .withColumnRenamed("GrossSalary", "gross_salary")
#     .withColumn("primary_key", col("login"))
#     .withColumn("is_current", lit(1))
# )
# ```

# Save df as 'employee' delta table.

# In[11]:


silver_df.write.format("delta").mode("overwrite").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/employee")


# Dispaly df.

# In[12]:


display(silver_df)  


# ##### GOLD

# Select name, surname, age and pk and save it as 'employee_personal' delta table.

# In[13]:


employee_personal_df = silver_df.select("name", "surname", "age", "primary_key")


# Select login, gross salary and pk, add 'net_salary' column, which would be 75% of the gross value, and save it as 'emplyee_company' delta table.

# In[14]:


employee_company_df = (silver_df
    .select("login", "gross_salary", "primary_key")
    .withColumn("net_salary", col("gross_salary")*0.75)
)


# Join both tables and save it as 'employee' delta table.

# In[15]:


gold_df = employee_personal_df.join(employee_company_df, on="primary_key")  # it's not silver anymore


# Display all dfs.

# In[16]:


display(employee_company_df)


# In[17]:


display(employee_personal_df)


# In[18]:


display(gold_df)


# Save all gold dataframes.

# In[19]:


employee_personal_df.write.format("delta").mode("overwrite").save("abfss://gold@dcdatahubsynapsessbdls.dfs.core.windows.net/employee_personal")


# In[20]:


employee_company_df.write.format("delta").mode("overwrite").save("abfss://gold@dcdatahubsynapsessbdls.dfs.core.windows.net/employee_company")


# In[21]:


gold_df.write.format("delta").mode("overwrite").save("abfss://gold@dcdatahubsynapsessbdls.dfs.core.windows.net/employee")


# Add a new column to gold_df (employee) with current timestamp.

# In[22]:


gold_df_with_timestamp = gold_df.withColumn("ingestion_time", current_timestamp())


# Filter current timestamp based on a parameter passed to the notebook.

# In[23]:


target_date = "2026-04-01"
gold_df_filtered = gold_df_with_timestamp.filter(col("ingestion_time").cast("date") == target_date)

