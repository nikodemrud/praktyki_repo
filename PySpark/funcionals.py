#!/usr/bin/env python
# coding: utf-8

# ## funcionals
# 
# 
# 

# In[25]:


from functools import reduce


# In[26]:


number = [252, 354, 321, 834, 893, 234, 293, 593, 622, 782]


# lamba argument : co robimy

# In[27]:


divide_by_2 = filter(lambda x: x%2==0, number)
multiplication_2 = map(lambda x: x*2, divide_by_2)
sum_all = reduce(lambda a,b : a+b, multiplication_2)

print(sum_all)


# Zadanie: Napisz funkcję rekurencyjną, która zsumuje wszystkie liczby z tej listy

# In[28]:


def sum(n):
    if len(n) == 0:       
        return 0
    else:
        return n[0] + sum(n[1:]) 

print(sum(number))


# Napisz funkcję rekurencyjną, która znajdzie i zwróci największą liczbę z tej listy

# In[29]:


def maximum(n):
    if len(n) == 1:
        return n[0]

    if n[0] > maximum(n[1:]):
        return n[0]
    else:
        return maximum(n[1:])

print(maximum(number))


# Napisz funkcję rekurencyjną, która zliczy, ile na tej liście znajduje się liczb parzystyc

# In[30]:


def count_even(n):
    if len(n) == 0:
        return 0
        
    if n[0] % 2 == 0:
        return 1 + count_even(n[1:])
    else:
        return count_even(n[1:])

print(count_even(number))


# Zwróć odwróconą listę (od 782 na początku do 252 na końcu), używając wyłącznie rekurencji.

# In[31]:


def reverse_list(n):
    if len(n) == 0:
        return []
        
    return reverse_list(n[1:]) + [n[0]]

print(reverse_list(number))


# In[32]:


def create_batches(n, batch_size):
    if len(n) == 0:
        return []
        
    return [n[:batch_size]] + create_batches(n[batch_size:], batch_size)

print(create_batches(number, 2))


# In[ ]:




