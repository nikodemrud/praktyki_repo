#!/usr/bin/env python
# coding: utf-8

# ## html_python
# 
# 
# 

# In[160]:


import re


# In[161]:


html_txt = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Detailed HTML Text Guide</title>
</head>
<body>

  <!-- Level 1 Heading: The Main Topic -->
  <h1>Mastering HTML Text Elements</h1>

  <!-- Standard Paragraph -->
  <p>HTML (HyperText Markup Language) is the backbone of the web. It uses 
  various tags to tell a browser how to interpret different types of 
  content.</p>

  <!-- Level 2 Heading: Subtopic -->
  <h2>Common Formatting Styles</h2>
  
  <p>When writing content, you often need to emphasize specific words:
    <ul>
      <li><strong>Strong:</strong> Used for <strong>highly important</strong> content.</li>
      <li><em>Emphasis:</em> Used to <em>stress</em> a word or phrase.</li>
      <li><mark>Mark:</mark> Ideal for <mark>highlighting</mark> key terms.</li>
      <li><u>Underline:</u> Traditionally used for <u>hyperlinks</u>, but can underline text.</li>
    </ul>
  </p>

  <!-- Level 2 Heading: Advanced Text -->
  <h2>Scientific & Technical Notation</h2>
  
  <p>HTML also handles specialized text easily:
    
<!-- Single line break -->
    Chemical Formula: H<sub>2</sub>O (uses the <code>&lt;sub&gt; tag for subscripts).
    

    Mathematical Equation: E = mc<sup>2</sup> (uses the <code>&lt;sup&gt;</code> tag for superscripts).
  </p>

  <!-- Level 2 Heading: Quotations -->
  <h2>Using Quotations</h2>
  
  <p>For shorter quotes, we use the <code>&lt;q&gt;</code> tag, but for longer 
  excerpts, the <code>&lt;blockquote&gt;</code> is preferred:</p>

  <blockquote cite="https://developer.mozilla.org">
    The HTML blockquote element indicates that the enclosed text is an 
    extended quotation from another source.
  </blockquote>

  <hr><!-- Horizontal rule to separate sections -->

  <p><small>This footer text is intentionally <del>large</del> <ins>small</ins> 
  using the <code>&lt;small&gt;</code> tag.</small></p>

</body>
</html>
 </code>
"""


# In[171]:


no_tags = re.sub(r'<.*?>', '', html_txt)


# In[172]:


print(no_tags)


# In[173]:


no_and = re.sub(r'&lt.*?&gt', '', no_tags)


# In[174]:


no_extra_spaces = re.sub(r'\s+', ' ', no_and)


# In[175]:


final_txt = no_extra_spaces.lower().strip()


# In[176]:


print(final_txt)


# In[168]:


"my\\folder\path"

