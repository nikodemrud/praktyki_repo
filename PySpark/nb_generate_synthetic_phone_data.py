#!/usr/bin/env python
# coding: utf-8

# ## nb_generate_synthetic_phone_data
# 
# 
# 

# #### Bronze

# importy daje się alfabetycznie (pamiętaj)

# In[173]:


from datetime import datetime, timedelta
import random

from pyspark.sql import Row, Window
from pyspark.sql.functions import col, current_date, date_format, dayofmonth, month, row_number, to_date, weekofyear, year
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from com.microsoft.spark.sqlanalytics.Constants import Constants


# przygotowanie stałych danych

# jak daje schema to od razu daj typ danych

# In[141]:


products_data = [
    (101, "Pear", "Phone 15", 128, 2023),
    (102, "Pear", "Phone 15 Pro", 256, 2023),
    (103, "Soney", "Xperia 10", 128, 2024),
    (104, "Macrosoft", "Surface Phone", 512, 2024),
    (105, "Dellta", "D-Phone Gen 2", 256, 2022)
]

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("brand", StringType(), True),
    StructField("model", StringType(), True),
    StructField("storage_gb", IntegerType(), True),
    StructField("release_year", IntegerType(), True)
])
df_dim_product = spark.createDataFrame(products_data, products_schema)


# In[142]:


cities_data = [
    (1, "Warszawa", "Polska", "Mazowieckie"),
    (2, "Wrocław", "Polska", "Dolnośląskie"),
    (3, "Kraków", "Polska", "Małopolskie"),
    (4, "Gdańsk", "Polska", "Pomorskie"),
    (5, "Poznań", "Polska", "Wielkopolskie")
]

cities_schema = StructType([
    StructField("city_id", IntegerType(), True),
    StructField("city_name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True)
])
df_dim_city = spark.createDataFrame(cities_data, cities_schema)


# In[143]:


customers_data = [
    (5001, "Jan Kowalski", "Premium"),
    (5002, "Anna Nowak", "Standard"),
    (5003, "Marek Kwiatkowski", "Premium"),
    (5004, "Zofia Zielińska", "Standard")
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_segment", StringType(), True)
])
df_dim_customer = spark.createDataFrame(customers_data, customers_schema)


# In[144]:


stores_data = [
    (10, "Sklep Główny", "Stacjonarny"),
    (20, "Sklep Online", "Internetowy"),
    (30, "Punkt Express", "Mały format")
]

stores_schema = StructType([
    StructField("store_id", IntegerType(), True),
    StructField("store_name", StringType(), True),
    StructField("store_type", StringType(), True)
])
df_dim_store = spark.createDataFrame(stores_data, stores_schema)


# In[145]:


start_date = datetime(2026, 1, 1)
execution_date = datetime.now()

product_ids = [101, 102, 103, 104, 105]
city_ids = [1, 2, 3, 4, 5]
customer_ids = [5001, 5002, 5003, 5004]
store_ids = [10, 20, 30]

return_reasons = ["factory_defect", "flip_flop", "damaged_box"]


# dwie puste listy do , których wrzucamy sprzedaże i zwroty
# 
# pętla wykonuje się 120 razy
# 
# Bierze naszą datę startową (np. 1 stycznia) i dodaje do niej tyle dni, w którym obrocie pętli aktualnie jesteśmy
# 
# losujemy ile było sprzedaży danego dnia 
# 
# nabija id tyle razy ile było klientów
# 
# sklejamy id z dnia sprzedaży, id miasta i id sprzedazy
# 
# na końcu założyliśmy, że 12% to zwroty, jeśli łamek będzie mniejszy to przypisujemy go do zwrotów

# In[146]:


all_sales_rows = []
all_returns_rows = []

for i in range(120):
    logical_date = start_date + timedelta(days=i)
    
    num_sales_today = random.randint(5, 20)
    
    for s in range(num_sales_today):
        order_id = f"ord_{logical_date.strftime('%Y%m%d')}_{s}"
        
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 2)
        
        customer_id = random.choice(customer_ids)
        city_id = random.choice(city_ids)
        
        sale_row = Row(
            sale_date=logical_date,
            order_id=order_id,
            customer_id=customer_id,
            product_id=product_id,
            store_id=random.choice(store_ids),
            city_id=city_id,
            quantity=quantity,
            load_date=logical_date,
            execution_date=execution_date
        )
        all_sales_rows.append(sale_row)
        
        
        if random.uniform(0, 1) < 0.12:
            return_row = Row(
                return_date=logical_date + timedelta(days=random.randint(1, 5)),
                order_id=order_id,
                customer_id=customer_id,
                product_id=product_id,
                city_id=city_id,
                return_reason=random.choice(return_reasons),
                quantity_returned=quantity,
                load_date=logical_date,
                execution_date=execution_date
            )
            all_returns_rows.append(return_row)


# tworzenie tabel sales i returns

# In[147]:


df_fact_sales = spark.createDataFrame(all_sales_rows) \
    .withColumn("sale_date", col("sale_date").cast("date")) \
    .withColumn("load_date", col("load_date").cast("date"))

df_fact_returns = spark.createDataFrame(all_returns_rows) \
    .withColumn("return_date", col("return_date").cast("date")) \
    .withColumn("load_date", col("load_date").cast("date"))


# In[148]:


df_fact_sales.write.format("delta").mode("append").save("abfss://bronze@dcdatahubsynapsessbdls.dfs.core.windows.net/fact_sales/")
df_fact_returns.write.format("delta").mode("append").save("abfss://bronze@dcdatahubsynapsessbdls.dfs.core.windows.net/fact_returns/")


# In[150]:


df_fact_sales.createOrReplaceTempView("bronze_fact_sales")
df_fact_returns.createOrReplaceTempView("bronze_fact_returns")


# In[151]:


get_ipython().run_cell_magic('sql', '', 'SELECT * FROM bronze_fact_sales LIMIT 5;\n')


# #### Silver

# tworzenie tabeli srebne returns poprzez:
# 
# złączenie tabeli returns (wybieram kolumny selectem)
# 
# z tabelą z produktami (tam interesuje mnie tylko id [left, bo in])
# 
# i również z miastami (też ineteresują nas tylko id [left bo])

# In[152]:


df_returns_silver = df_fact_returns \
    .join(df_dim_product, "product_id", "left") \
    .join(df_dim_city, "city_id", "left") \
    .select(
        col("return_date"),
        col("order_id"),
        col("brand"),
        col("model"),
        col("city_name"),
        col("return_reason"),
        col("quantity_returned"),
        col("load_date"),
        col("execution_date")
    )


# numer tygodnia dodaj
# 
# sprawdź sobie iso-week
# 
# numerowanie tygodnia na podstawie różnych standartów

# In[153]:


df_dim_date = df_fact_sales.select("sale_date").distinct() \
    .withColumnRenamed("sale_date", "calendar_date") \
    .withColumn("year", year("calendar_date")) \
    .withColumn("month", month("calendar_date")) \
    .withColumn("day", dayofmonth("calendar_date")) \
    .withColumn("day_of_week", date_format("calendar_date", "EEEE")) \
    .withColumn("iso_week", weekofyear("calendar_date"))


# ### Czym różnią się standardy numerowania tygodni?
# 
# W tabeli kalendarza (`dim_date`) często musimy wyciągnąć numer tygodnia w roku. Problem w tym, że istnieją dwa główne standardy:
# 
# 1. **Standard ISO 8601 (Europejski / Biznesowy)**
#    * Tydzień zawsze zaczyna się w **poniedziałek**.
#    * Tydzień nr 1 w roku to ten, który ma w nowym roku co najmniej 4 dni (czyli musi zawierać pierwszy czwartek roku).
#    * W PySparku używamy do tego gotowej funkcji: `weekofyear()`.
# 
# 2. **Standard Amerykański (US)**
#    * Tydzień zawsze zaczyna się w **niedzielę**.
#    * Tydzień nr 1 to po prostu ten tydzień, w którym wypada 1 stycznia (nawet jeśli 1 stycznia to sobota i ten "tydzień" trwa tylko jeden dzień).
#    * W PySparku nie ma do tego jednej gotowej funkcji, ale możemy to wyciągnąć formatując datę: `date_format(col("data"), "w")`.
# 
# Dodanie obu tych kolumn pozwala analitykom filtrować dane w zależności od tego, dla jakiego rynku robią raport.

# zastosuj to co masz na Teams - funkcja window

# In[154]:


price_history_data = [
    (101, "2025-12-01", "2026-02-28", 4000), 
    (101, "2026-03-01", "9999-12-31", 4500), 
    
    (102, "2025-12-01", "9999-12-31", 5500),
    
    (103, "2025-12-01", "2026-04-15", 3000),
    (103, "2026-04-16", "9999-12-31", 2500),

    (104, "2025-12-01", "9999-12-31", 4800),
    (105, "2025-12-01", "9999-12-31", 3500)
]

price_history_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("price_valid_from", StringType(), True), 
    StructField("price_valid_to", StringType(), True),   
    StructField("historical_price", IntegerType(), True)
])

df_dim_product_price = spark.createDataFrame(price_history_data, price_history_schema) \
    .withColumn("price_valid_from", to_date(col("price_valid_from"))) \
    .withColumn("price_valid_to", to_date(col("price_valid_to")))


# In[155]:


window_spec = Window.partitionBy("product_id").orderBy(col("price_valid_from").desc())

df_current_prices = df_dim_product_price \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num") 

display(df_current_prices)


# W naszym łączeniu lewa tabela to sprzedaż, a prawa to cennik. Używamy łączenia lewostronnego, aby mieć absolutną pewność, że żaden paragon nie zniknie z bazy. 
# Gdybyśmy użyli zwykłego łączenia (tzw. `INNER JOIN`), a w cenniku przez jakiś błąd brakowałoby ceny dla danego telefonu w danym dniu – program usunąłby cały ten paragon do kosza
# 
# 
# Skleja surową sprzedaż z historią cen.
# 
# Zestawia ten sam model telefonu ze sprzedaży z tym samym modelem w cenniku (`product_id` == `product_id`).
# 
# Sprawdza, czy data na paragonie wpada w odpowiednie ramy czasowe z cennika (data sprzedaży mieści się między `price_valid_from` a `price_valid_to`)

# In[156]:


join_conditions = [
    df_fact_sales.product_id == df_dim_product_price.product_id,
    df_fact_sales.sale_date >= df_dim_product_price.price_valid_from,
    df_fact_sales.sale_date <= df_dim_product_price.price_valid_to
]

df_fact_sales_silver = df_fact_sales.join(
    df_dim_product_price,
    join_conditions,  
    how="left"
).select(
    df_fact_sales["sale_date"],
    df_fact_sales["order_id"],
    df_fact_sales["customer_id"],
    df_fact_sales["product_id"],
    df_fact_sales["store_id"],
    df_fact_sales["city_id"],
    df_fact_sales["quantity"],
    col("historical_price").alias("unit_price"), 
    df_fact_sales["load_date"],
    df_fact_sales["execution_date"]
).withColumn(
    "total_amount", col("quantity") * col("unit_price")
)


# In[163]:


df_dim_product.write.format("delta").mode("append").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/dim_product/")
df_dim_city.write.format("delta").mode("append").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/dim_city/")
df_dim_customer.write.format("delta").mode("append").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/dim_customer/")
df_dim_store.write.format("delta").mode("append").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/dim_store/")
df_dim_date.write.format("delta").mode("append").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/dim_date/")
df_dim_product_price.write.format("delta").mode("append").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/dim_product_price/")

df_fact_sales_silver.write.format("delta").mode("append").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/fact_phone_sales/")
df_returns_silver.write.format("delta").mode("append").save("abfss://silver@dcdatahubsynapsessbdls.dfs.core.windows.net/fact_phone_returns/")


# In[164]:


df_dim_product.createOrReplaceTempView("silver_dim_product")
df_dim_city.createOrReplaceTempView("silver_dim_city")
df_dim_customer.createOrReplaceTempView("silver_dim_customer")
df_dim_store.createOrReplaceTempView("silver_dim_store")
df_dim_date.createOrReplaceTempView("silver_dim_date")
df_dim_product_price.createOrReplaceTempView("silver_dim_product_price")

df_fact_sales_silver.createOrReplaceTempView("silver_fact_phone_sales")
df_returns_silver.createOrReplaceTempView("silver_fact_phone_returns")


# In[165]:


get_ipython().run_cell_magic('sql', '', 'SELECT * FROM silver_fact_phone_sales;\n')

