#!/usr/bin/env python
# coding: utf-8

# ## employee
# 
# 
# 

# In[ ]:


from pyspark.sql.functions import udf, col, lit, concat_ws, when, avg, regexp_replace, regexp_extract, row_number, desc, dense_rank, substring, lower, concat, length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql import Row


# In[ ]:


employee_txt = """Name;Surname;Login;Age;Position;GrossSalary
Andrzej;Nowak;annow;52;analyst;5000
Stanislaw;Kowalski;stakow;43;architect;12000
Jan;Niezbedny;janiez;45;lead;10000
Piotr;Domyslny;piodom;30;engineer;6500
Andrzej;Nowak;annowa;52;analyst;7000
Stanislaw;Kowalski;stakowa;58;architect;14000
Jan;Niezbedny;janiezb;45;lead;13000
Piotr;Domyslny;piodomy;36;engineer;8000
Andrzej;Nowak;annowak;52;analyst;7000
Stanislaw;Kowalski;stakowal;61;architect;12000
Jan;Niezbedny;jannie;45;lead;10000
Piotr;Domyslny;piotdo;25;engineer;9000
Andrzej;Nowak;andnow;52;analyst;8000
Stanislaw;Kowalski;stanko;51;architect;13500
Jan;Niezbedny;janniez;45;lead;11000
Piotr;Domyslny;piotdom;30;engineer;8000"""


# In[ ]:


employee_lst = [r.split(';') for r in employee_txt.splitlines()]
employee_schema = employee_lst[0]
employee_data = employee_lst[1:]


# Create dataframe from 'employee_data' list.

# In[ ]:


df = spark.createDataFrame(data=employee_data, schema=employee_schema)


# Display df.

# In[ ]:


display(df)


# Print df schema.

# In[ ]:


df.printSchema()


# Change column names to snake_case.

# In[ ]:


df_1 = (df
       .withColumnRenamed("Name", "name")
       .withColumnRenamed("Surname", "surname")
       .withColumnRenamed("Login", "login")
       .withColumnRenamed("Age", "age")
       .withColumnRenamed("Position", "position")
       .withColumnRenamed("GrossSalary", "gross_salary")
)
display(df_1)


# Add primary_key column with login values.

# In[ ]:


df_1 = df_1.withColumn("primary_key", col("login"))
display(df_1)


# Add is_current column with 1 value.

# In[ ]:


df_1=df_1.withColumn("is_current", lit(1))
display(df_1)
df_1.printSchema()


# Create a dataframe from 'employee_data' once again, but this time also create a full schema structure, where login column must not contain NULL values.

# In[ ]:


data_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Surname", StringType(), True),
    StructField("Login", StringType(), False), 
    StructField("Age", StringType(), True),
    StructField("Position", StringType(), True),
    StructField("GrossSalary", StringType(), True)
])


# In[ ]:


df_2 = spark.createDataFrame(data=employee_data, schema=data_schema)


# In[ ]:


display(df_2)


# Change column names to snake_case. Use a dict if preferable. Convert iteger values to int type.

# In[ ]:


df_3 = df.withColumnsRenamed({
    "Name": "name", 
    "Surname": "surname", 
    "Login": "login", 
    "Age": "age", 
    "Position": "position", 
    "GrossSalary": "gross_salary"
    })
display(df_3)


# In[ ]:


df_3 = (df_3
        .withColumn("age", col("age").cast(IntegerType()))
        .withColumn("gross_salary", col("gross_salary").cast(IntegerType()))
)
df_3.printSchema()


# Select all columns. Change names to snake_case. Convert iteger values to int type.

# In[ ]:


df_4 = df.select(
    col("Name").alias("name"),
    col("Surname").alias("surname"),
    col("Login").alias("login"),
    col("Age").cast(IntegerType()).alias("age"),
    col("Position").alias("position"),
    col("GrossSalary").cast(IntegerType()).alias("gross_salary"),
)
display(df_4)
df_4.printSchema()


# Filter employees who earn at least 10000.

# In[ ]:


df_5 = df_4.filter(col("gross_salary") >= 10000)
display(df_5)


# Add a primary_key column which consists of login and position (login-position).

# In[ ]:


df_4 = df_4.withColumn("primary_key", concat_ws("-", col("login"), col("position")))  # using col func not necessary
display(df_4)


# Filter engineers who earn less than 9000.

# In[ ]:


df_6 = df_4.filter((col("gross_salary") < 9000) & (col("position") == "engineer"))
display(df_6)


# other option (but it's useless)
# 
# df_6 = (df_4
#     
#     .filter(col("gross_salary") < 9000)
#     .where(col("position") == "engineer")
# )
# 
# display(df_6)

# Filter employees who are not architects.

# In[ ]:


df_7 = df_4.filter(col("position") != "architect")
display(df_7)


# In[ ]:


df_90 = df_4.filter(length(col("login")) <= 6)
liczba_pracownikow = df_90.count()
display(liczba_pracownikow)


# Select distinct (drop duplicates) positions.

# In[ ]:


df_8 = df_4.select("position").distinct()  # also dropDuplicates()
display(df_8)


# Filter employees who earn 10000 or 12000. Think of two ways of doing it.

# In[ ]:


df_9 = df_4.filter((col("gross_salary") == 10000) | (col("gross_salary") == 12000))
display(df_9)


# In[ ]:


df_10 = df_4.filter(col("gross_salary").isin(10000, 12000))
display(df_10)


# Remove duplicated rows based on name and surname columns.

# In[ ]:


df_12 = df_4.dropDuplicates(("name", "surname"))
display(df_12)


# Remove name and surname columns.

# In[ ]:


df_13 = df_4.drop("name", "surname")
display(df_13)


# Update records where salary is less than 7000 to be equal to 7000.

# In[ ]:


df_14 = df_4.withColumn("gross_salary", when(col('gross_salary')  < 7000, 7000).otherwise(col('gross_salary') ))  
display(df_14)

# col('gross_salary') instead of df_4.gross_salary
# funcion col is more safety than df_4.name_of_the_column


# df_15 = df_4.filter(col("gross_salary") < 7000).withColumn("gross_salary", lit(7000))
# 
# df_16 = df_4.filter(col("gross_salary") >= 7000).withColumn("gross_salary", col("gross_salary"))
# 
# display(df_15)
# display(df_16)
# 
# df_17 = df_15.union(df_16)
# 
# Opcja z tworzeniem 2 tabel i połączniem ich

# Create two dataframes: 'personal_data' (name, surname, age, login) and 'employee_data' (position, gross_salary, login) from the existing one.

# In[ ]:


personal_data = df_4.select("name", "surname", "age", "login")


# In[ ]:


employee_data = df_4.select("position", "gross_salary", "login")


# Join 'personal_data' and 'employee_data'.

# In[ ]:


df_15 = personal_data.join(employee_data, on="login", how="inner")
df_16 = df_15.select("name", "surname", "age", "login", "position", "gross_salary")  
#display(df_15) #kolumna login jest 1
display(df_16) #kolumna login jest na pozycji 4


# Create two dataframes: 'personal_data' (name, surname, age, login as personal_login) and 'employee_data' (position, gross_salary, login as employee_login) from the existing one.

# In[ ]:


personal_data_v2 = df_4.select("name", "surname", "age", col("login").alias("personal_login"))


# In[ ]:


employee_data_v2 = df_4.select("position", "gross_salary", col("login").alias("employee_login"))


# Join 'personal_data_v2' and 'employee_data_v2'.

# In[ ]:


df_17 = personal_data_v2.join(employee_data_v2, personal_data_v2["personal_login"] == employee_data_v2["employee_login"], "inner").drop(employee_data_v2["employee_login"])


# In[ ]:


# df_17 = (
#     personal_data_v2.alias('personal_data')
#     .join(
#         employee_data_v2.alias('employee_data'),
#         col('personal_data.personal_login') == col('employee_data.employee_login')
#     ).drop(col('employee_data.employee_login'))
# )


# In[ ]:


display(df_17)


# Calculate an average salary for every position.

# In[ ]:


#df_18 = df_16.select(avg("gross_salary"))
#display(df_18)


# agg() - agregacja
# 
# avg() - średnia

# In[ ]:


df_18 = df_16.groupBy("position").agg(avg("gross_salary").alias("avg_gross_salary"))
display(df_18)


# In 'login' replace double n ('nn') with a single 'n'.

# df_19 = df_16.select("*", regexp_replace(col("login"), "nn", "n").alias("new_login")).drop("login")
# 
# display(df_19)
# 
# First code, which I wrote for this exercise
# 
# Minus: diffrent name for new column

# regaxp_replace() -  funkcja do zmiany wartości w kolumnach

# In[ ]:


df_19 = df_16.withColumn("login", regexp_replace(col("login"), "nn", "n"))  # col func not necessary: regexp_replace("login", "nn", "n") works just fine
display(df_19)


# Create a new column containing numbers (1, 2, 3 etc.) for every position based on the salary, where 1 means the highest one. (Difficult, so don't hesitate to ask questions!)

# https://www.geeksforgeeks.org/python/pyspark-window-functions/ -> notes

# In[ ]:


df_w = Window.partitionBy("position").orderBy(col("gross_salary").desc())
df_20 = df_16.withColumn("gross_salary_ranking_by_position", dense_rank().over(df_w))
display(df_20)


# Create three columns: 'name_3' containing 3 first name letters, 'surname_3' containing 3 first surname letters, 'new_login' with concatenated lowercased 'name_3' and 'surname_3'.

# concat - nie potrzebuje separatora
# 
# concat_ws - wpierw podajemy separator, a potem kolumny, które "łączymy" (lepsze, bo traktuje Nulla jako pustego stringa)

# lower() - funkcja do zmniejszania liter

# substring() - służy do wyodrębniania fragmentu tekstu (podciągu) z kolumny typu string w DataFrame

# In[ ]:


df_21 = (df_4
    .withColumn("name_3_letters", substring("name", 1, 3))
    .withColumn("surname_3_letters", substring("surname", 1, 3))
    .withColumn("new_log", lower(concat_ws('', "name_3_letters", "surname_3_letters")))
)

display(df_21)


# Imagine there is a string pattern in the previous example. How would you do it then?

# rlike(pattern) – zwraca True, jeśli wartość pasuje do wzorca regex.
# 
# regexp_extract(col, pattern, group) – wyciąga fragment tekstu pasujący do wzorca.
# 
# regexp_replace(col, pattern, replacement) – zamienia fragmenty pasujące do wzorca na inny tekst.

# In[ ]:


df_22 = (df_4
    .withColumn("name_clean", regexp_extract(col("name"), r"[A-Za-z]{3}", 0))
    .withColumn("surname_clean", regexp_extract(col("surname"), r"\w{3}", 0))
    .withColumn("new_log", lower(concat_ws('', col("name_clean"), col("surname_clean"))))
)

display(df_22)


# Create three columns: 'name_3' containing 3 last name letters, 'surname_3' containing 3 last surname letters, 'new_login' with concatenated lowercased 'name_3' and 'surname_3'

# In[ ]:


df_23 = (df_4
    .withColumn("name_clean", regexp_extract(col("name"), r"[A-Za-z]{3}$", 0))
    .withColumn("surname_clean", regexp_extract(col("surname"), r"\w{3}$", 0))
    .withColumn("new_log", lower(concat_ws('', col("name_clean"), col("surname_clean"))))
)

display(df_23)


# Create a function (UDF) to do the same.

# In[ ]:


@udf(StringType())
def login(name,surname):
    if name is None or surname is None:
        return None
    part_name = name[:3]
    part_surname = surname[:3]
    login = (part_name + part_surname).lower()
    return login


# would't that be more neat and readable if you applied a decorator instead? :)

# In[ ]:


df_24 = df_4.withColumn("n_login", login(col("name"), col("surname")))
display(df_24)


# Add 1000 to every salary using a UDF anonymous (lambda) function.

# In[ ]:


bonus = udf(lambda gross_salary: gross_salary + 1000, IntegerType())
df_25 = df_4.withColumn("new_salary", bonus(col("gross_salary")))
display(df_25)


# In PySpark, a lambda function is an anonymous function (no name) that you can use inline, often with transformations like map(), filter(), or reduceByKey() on RDDs or with DataFrame operations.

# Convert dataframe to RDD.

# In[ ]:


rdd_1 = df_4.rdd
display(rdd_1.toDF())


# Tabela z różnicami
# 
# ![1](https://danishcrown-my.sharepoint.com/:i:/r/personal/niru_danishcrown_com/Documents/Pictures/Screenshots/Zrzut%20ekranu%202026-03-30%20131943.png?csf=1&web=1&e=60CdIF)
# 
# ![2](https://danishcrown-my.sharepoint.com/:i:/r/personal/niru_danishcrown_com/Documents/Pictures/Screenshots/Zrzut%20ekranu%202026-03-30%20131957.png?csf=1&web=1&e=8sG8qm)
# 
# ![3](https://danishcrown-my.sharepoint.com/:i:/r/personal/niru_danishcrown_com/Documents/Pictures/Screenshots/Zrzut%20ekranu%202026-03-30%20132032.png?csf=1&web=1&e=sz1nEI)

# Map lambda function on RDD to add 'senior' prefix to every position. Convert it back to DF.

# In[ ]:


rdd_2 = rdd_1.map(lambda row: Row(
    name = row.name,
    surname = row.surname,
    login = row.login,
    age = row.age,
    gross_salary = row.gross_salary,
    primary_key = row.primary_key,
    position = "senior " + row.position
    ))
df_26 = rdd_2.toDF()
display(df_26)


# lambda position: Row(position="senior " + position.position)
# 
# row - bierze cały wiersz z danymi pracownika
# 
# : - robi w następujący sposób
# 
# Row(position="senior " + s.position) – zagląda do stanowiska tego pracownika (position.position), doklejam z przodu słowo *senior * i zapisuję to jako jego nowe stanowisko.

# The map() method in Pandas Series is primarily used for transforming values based on a mapping or function. It provides a convenient way to apply a function or a mapping dictionary to each element in the Series, creating a new Series with the transformed values.
# 
# When you use the map() function, it creates a new Series with the transformed values. The original Series remains unchanged. If you want to modify the original Series, you need to assign the result back to it.
# 
# The map() function is highly flexible and can accommodate various use cases. It allows you to use a dictionary, a function, or even another Series to define the mapping for transforming values.
# 
# Custom functions, including lambda functions, can be employed to define complex transformations. This flexibility makes it a versatile tool for data manipulation, especially when dealing with categorical data, data cleaning, or creating derived features based on existing ones.
# 
# The map() function provides the na_action parameter, allowing you to specify how to handle NaN (Not a Number) values. You can choose to ignore them, raise an error, or handle them in a custom way.
# 

# In[ ]:


rdd_1.collect()


# collect() - wypisuje po wierszach

# Do the same using a defined function. Don't select all the elements, rather try to make the row mutable and then convert it to a Row again.

# In[ ]:


def add_senior(row):
    row_dict = row.asDict()
    row_dict["position"] = "senior " + row_dict["position"]
    new_position = Row(**row_dict)
    return new_position

rdd_2 = rdd_1.map(senior)
df_27 = rdd_2.toDF()
display(df_27)


# ** - wyciągnij wszystkie dane (kwargs)
# 
# asDict() - używane na obiekcie Row, aby przekonwertować go w słownik Pythona
