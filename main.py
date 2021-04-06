#!/usr/bin/env python
# coding: utf-8

# In[26]:


import psycopg2
import pandas as pd

from sqlalchemy import create_engine
from datetime import datetime


# ## In this first step we're going to load the date in the memory, create a connection with our database, create our tables and load our data to the database

# In[2]:


jobs_df = pd.read_csv("data/jobs.txt", delimiter="|")
categories_df = pd.read_csv("data/category.txt", delimiter="|")
conn_string = 'postgres://postgres:postgres@localhost:5431/levee_test'
db = create_engine(conn_string)
conn = db.connect()


# In[3]:


def create_table():
    conn = psycopg2.connect(host="localhost", database="levee_test", user="postgres",
                            password="postgres", port="5431")

    queries = """
    CREATE TABLE IF NOT EXISTS jobs(
        partnerId SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        categoryId SERIAL,
        ExpiresAt DATE,
        openPositionAmnt INTEGER     
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS category(
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    """
    cursor = conn.cursor()
    try:
        for query in queries:
            cursor.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()


# In[4]:


create_table()


# In[5]:


jobs_df.to_sql('jobs', con=conn, if_exists='replace', index=False)
categories_df.to_sql('category', con=conn, if_exists='replace', index=False)


# ## Now that we have already loaded our data to the datebase is time to retrieve the data to answer the challange questions

# In[10]:


def create_pandas_table(sql_query, database=conn):
    table = pd.read_sql_query(sql_query, database)
    return table


# In[14]:


jobs_info = create_pandas_table("SELECT * FROM jobs ORDER BY title")
categories_info = create_pandas_table("SELECT * FROM category ORDER BY name")


# In[17]:


jobs_info.head()


# In[16]:


categories_info.head()


# In[18]:


df = jobs_info.join(categories_info.set_index('id'), on='categoryId')


# In[20]:


df.head()


# In[24]:


# Solution 1

df_sum = df.groupby(['name']).sum()
open_positions = df_sum['openPositionAmnt']
print(f'The number of open positions per category name is: {open_positions}')


# In[29]:


# Solution 2
date = datetime.today()

df['ExpiresAt'] = pd.to_datetime(df['ExpiresAt'])
expired_positions = (
    df.loc[df['ExpiresAt'] < date].sort_values('ExpiresAt').iloc[-3:])
print(f'The last three jobs that expired is: \n{expired_positions}')


# ![thatsall](https://media.giphy.com/media/upg0i1m4DLe5q/giphy.gif "thatsall")

# In[ ]:
