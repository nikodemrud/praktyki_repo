#!/usr/bin/env python
# coding: utf-8

# ## regular_expressions
# 
# 
# 

# rlike(pattern) – zwraca True, jeśli wartość pasuje do wzorca regex.
# 
# regexp_extract(col, pattern, group) – wyciąga fragment tekstu pasujący do wzorca.
# 
# regexp_replace(col, pattern, replacement) – zamienia fragmenty pasujące do wzorca na inny tekst.

# In[2]:


from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, concat_ws, regexp_extract, regexp_replace


# In[3]:


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


# In[4]:


employee_lst = [r.split(';') for r in employee_txt.splitlines()]
employee_schema = employee_lst[0]
employee_data = employee_lst[1:]


# In[5]:


data_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Surname", StringType(), True),
    StructField("Login", StringType(), False), 
    StructField("Age", StringType(), True),
    StructField("Position", StringType(), True),
    StructField("GrossSalary", StringType(), True)
])


# In[6]:


df = spark.createDataFrame(data=employee_data, schema=data_schema)


# In[7]:


display(df)


# #### RLIKE

# Wyfiltruj wszystkich pracowników, których Login zaczyna się od "an".

# In[8]:


df_1 = df.filter(col("Login").rlike("^an"))
display(df_1)


# Znajdź rekordy, gdzie Surname kończy się na "ski".

# In[9]:


df_2 = df.filter(col("Surname").rlike("ski$"))
display(df_2)


# Wybierz pracowników, których Login zawiera cyfrę (jeśli są takie lub przygotuj zapytanie na przyszłość).

# In[10]:


df_3 = df.filter(col("Login").rlike("[0-9]"))
display(df_3)


# Znajdź osoby, których Name zaczyna się wielką literą i ma dokładnie 3 litery więcej (np. "Jan").

# In[11]:


df_4 = df.filter(col("Name").rlike("^[A-Z][a-z]{2}$"))
display(df_4)


# Wyfiltruj rekordy, gdzie Position zawiera literę "a" gdziekolwiek.

# In[12]:


df_5 = df.filter(col("Position").rlike("a"))
display(df_5)


# Znajdź pracowników, których Login ma dokładnie 6 znaków

# In[13]:


df_6 = df.filter(col("Login").rlike("^.{6}$"))
display(df_6)


# Wybierz osoby, których Surname zawiera tylko litery (bez cyfr i znaków specjalnych)

# In[14]:


df_7 = df.filter(col("Surname").rlike("^[a-zA-Z]+$"))
display(df_7)


# Znajdź pracowników, których Login zaczyna się od 2 liter, a potem ma co najmniej jedną kolejną literę.

# In[15]:


df_8 = df.filter(col("Login").rlike("^[a-z]{2}[a-z]+"))
display(df_8)


# Wyfiltruj rekordy, gdzie GrossSalary jest liczbą 5-cyfrową (zakładamy string)

# ('^[0-9]{5}$')

# In[16]:


df_9 = df.filter(col("GrossSalary").rlike('^\d{5}$'))
display(df_9)


# Znajdź pracowników, których Login zawiera powtarzającą się literę pod rząd (np. "nn")

# "\\\1" - powtórzenie

# In[17]:


df_10 = df.filter(col("Login").rlike("([a-z])\\1"))
display(df_10)


# #### REGEX_REPLACE

# df_19 = df_16.withColumn("login", regexp_replace(col("login"), "nn", "n"))  # col func not necessary: regexp_replace("login", "nn", "n") works just fine
# display(df_19)

# Usuń wszystkie samogłoski z kolumny Login.

# In[18]:


df_11 = df.withColumn("Login", regexp_replace(col("Login"), "[aeiou]", ""))
display(df_11)


# Zastąp wszystkie litery w Name znakiem "*" (zachowaj długość)

# In[19]:


df_12 = df.withColumn("Login", regexp_replace(col("Login"),"([A-Za-z])", "*"))
display(df_12)


# W kolumnie GrossSalary dodaj prefiks "PLN " przed każdą wartością (regex tylko dopasuj początek).

# In[20]:


df_13 = df.withColumn("GrossSalary", regexp_replace(col("GrossSalary"),"^", "PLN "))
display(df_13)


# Zamień wszystkie podwójne litery w Login (np. nn) na jedną literę.

# zrób to w Pythonie

# In[36]:


df_99 = df.withColumn("Login", regexp_replace(col("Login"), "([A-Za-z])\\1+","$1"))
display(df_99)


# In[22]:


df_14 = df.withColumn("Login", regexp_replace(col("Login"), "nn", "n"))
display(df_14)


# Usuń wszystkie spacje lub inne białe znaki (na przyszłość – gdyby były).

# In[23]:


df_15 = df.withColumn("Login", regexp_replace("Login", "\\s+", ""))
display(df_15)


# #### regexp_extract

# Wyciągnij pierwsze 2 litery z kolumny Login.

# In[24]:


df_16 = df.withColumn("Login", regexp_extract("Login", "^([a-z]{2})", 0))
display(df_16)


# Wyciągnij ostatnią literę z Surname.

# In[25]:


df_17 = df.withColumn("Surname", regexp_extract("Surname", "([a-z]{1}$)", 0))
display(df_17)


# Wyciągnij numer wieku jako liczbę (jeśli traktujemy Age jako string).

# In[26]:


df_18 = df.withColumn("Age", regexp_extract("Age", "(\d+)", 0))
display(df_18)


# Wyciągnij z Login fragment zaczynający się od "jan" (jeśli istnieje).

# In[27]:


df_19 = df.withColumn("Login", regexp_extract("Login", "^(jan)+[a-z]+", 0))
display(df_19)


# Wyciągnij długość liczby w GrossSalary (np. ile cyfr ma pensja).

# ?

# In[28]:


df_19 = df.withColumn("GrossSalary", regexp_extract("GrossSalary", "([0-9]+)", 0))
display(df_19)


# Z Login wyciągnij pierwszy powtarzający się znak pod rząd (np. z jannie → n)

# In[29]:


df_nn = df.withColumn("Login", regexp_extract("Login", "nn", 0))
df_20 = df_nn.withColumn("Login", regexp_replace("Login", "nn", "n"))
display(df_20)


# Zbuduj nową kolumnę z inicjałami:
# 
# 1 litera z Name
# 1 litera z Surname

# how to di it better?

# In[30]:


df_i = df.withColumn("Initial", concat_ws("", col("Name"), col("Surname")))
df_oi = df_i.select("Initial")

df_21 = df_oi.withColumn("Initial", regexp_replace(col("Initial"), "([a-z])", ""))
display(df_21)

