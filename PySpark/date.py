#!/usr/bin/env python
# coding: utf-8

# ## date
# 
# 
# 

# 
# (Uwaga: relativedelta jest w pakiecie python-dateutil.)
# 
# C1) Dla daty 2026-01-31 policz datę:
#     - +1 miesiąc
#     - +2 miesiące
# 
#     Miejsce na odpowiedź:
# 
# 
# C2) Policz “pierwszy dzień następnego miesiąca” dla daty 2026-12-15.
# 
#     Miejsce na odpowiedź:
# 
# 
# C3) Policz “ostatni dzień miesiąca” dla dowolnej daty (np. 2026-02-10).
#     Wskazówka: przejdź na 1. dzień następnego miesiąca i odejmij 1 dzień.
# 
#     Miejsce na odpowiedź:
# 
# 
# C4) Mając birthdate (data urodzenia), policz wiek w latach i miesiącach
#     na dzień 2026-05-07.
#     Przetestuj np. na birthdate = 2000-11-20.
# 
#     Miejsce na odpowiedź:
# 
# 
# --------------------------------
# PODPOWIEDZI (krótkie)
# --------------------------------
# 
# ISO WEEK (datetime.date):
# - d.isocalendar() -> (iso_year, iso_week, iso_weekday)
# - iso_weekday: 1=poniedziałek ... 7=niedziela
# 
# timedelta:
# - timedelta(days=..., hours=..., minutes=...)
# - różnica dat/czasów: dt2 - dt1 -> timedelta
# 
# relativedelta (dateutil):
# - relativedelta(months=1, years=1)
# - obsługuje przesunięcia kalendarzowe (miesiące, lata)
# - weekday=MO(+1) itp. do “następnego poniedziałku”
# 
# --------------------------------
# BONUS (opcjonalnie)
# --------------------------------
# 
# D1) Napisz funkcję, która dla podanej daty zwraca datę najbliższego
#     poniedziałku (jeśli data jest poniedziałkiem, zwróć ją samą).
# 
# D2) Walidacja: dostajesz string w formacie ISO week-date (extended)
#     np. 2026-W19-4. Sprawdź, czy jest poprawny i zamień na datę.
# 

# In[23]:


from datetime import date
import datetime


# A1) Dla daty 2026-01-01 wypisz:
#     - ISO year
#     - ISO week number
#     - ISO weekday

# In[10]:


d = date(2026, 1, 1)
iso_year, iso_week, iso_weekday = d.isocalendar()
print(iso_year, "iso_year")
print(iso_week, "iso_week")
print(iso_weekday, "iso_weekday")


# A2) Napisz funkcję/algorytm iso_week_string(d), która dla obiektu daty zwraca napis w formacie:
#     - extended: YYYY-Www-D
#     - basic:    YYYYWwwD

# In[24]:


def iso_week_string(d):
    y, w, wd = d.isocalendar()
    extended = f"{y}-W{w:02d}-{wd}"
    basic    = f"{y}W{w:02d}{wd}"
    return extended, basic

my_date = datetime.date(2026, 1, 1)
display(iso_week_string(my_date))


# A3) Mając iso_year=2025 i iso_week=53, wyznacz datę PONIEDZIAŁKU tego tygodnia ISO.

# In[27]:


iso_year = 2025
week = date(iso_year, 12, 29).isocalendar()
display(week)
display("brak 53 tygodnia")


# A4) Sprawdź, które lata w przedziale 2015–2030 mają tydzień 53 (ISO).
#     Wynik wypisz jako listę lat, np. [2015, 2020, ...]

# In[32]:


years=[]
for iso_year in range (2015,2031,1):
    if date(iso_year, 12, 29).isocalendar().week == 53:
       years.append(iso_year) 
display(years) 


# B1) Masz start = 2026-05-07 11:00. Dodaj kolejno:

# In[ ]:





# B2) Policz liczbę dni między datami:

# B3) Dostajesz listę timestampów (datetime) i chcesz wykryć, czy przerwa
