#!/usr/bin/env python
# coding: utf-8

# ## defaultdict
# 
# 
# 

# In[318]:


from collections import defaultdict, Counter
import re
import pprint


# stworzyć słownik, ktory będzie zawierał slowa pogrupowane, ze względu na literę na ktorą się zaczynają
# 
# -wyrzuczmy kropki i inne znaki interpunkcyjne
# -małe literki
# -słownik, gdzie kluczami będą 1. litery

# In[319]:


txt = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."


# In[320]:


clean_txt = re.sub(r"[^\w\s]", "", txt)
lower_txt = clean_txt.lower()
words = lower_txt.split()


# In[321]:


dict_by_1_letter = defaultdict(list)


# In[322]:


for word in words:
    dict_by_1_letter[word[0]].append(word)


# In[323]:


print(dict(dict_by_1_letter))


# In[324]:


first_and_last_letter_dict = defaultdict(lambda: defaultdict(list))


# In[325]:


for word in words:
    first_and_last_letter_dict[word[0]][word[-1]].append(word)


# In[326]:


print(first_and_last_letter_dict)


# In[327]:


first_and_last_letter_dict["x"]


# policzyć ile jest danych słów

# In[328]:


how_many_words = defaultdict(int)


# In[329]:


for word in words:
    how_many_words[word] += 1


# In[330]:


print(how_many_words)


# In[335]:


cnt = Counter(words)
print(cnt)


# .uptade() - Adds counts from another iterable or mapping. Existing counts increase and new elements are added.
# 
# .elements() - Returns an iterator over elements repeating each as many times as its count. Elements are returned in arbitrary order.
# 
# .subtract()- Subtracts element counts from another iterable or mapping. Counts can go negative

# most.common nie uwzględnia remisów
# 
# ```
# from collections import Counter
# 
# def top_n_with_ties(words, n):
#     if n <= 0:
#         return []
# 
#     cnt = Counter(words)
#     common = cnt.most_common()
# 
#     if n > len(common):
#         return common
# 
#     threshold = common[n - 1][1]
#     
#     return [(word, count) 
#             for word, count in common 
#             if count >= threshold]
# 
# print(top_n_with_ties(words,3))
# 
# to uwzględnia remisy
# ```

# In[342]:


print(cnt.most_common(3))


# In[347]:


cnt["fdbhjcd"]

