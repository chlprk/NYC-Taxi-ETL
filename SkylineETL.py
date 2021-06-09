#!/usr/bin/env python
# coding: utf-8

# In[1]:


import timeit
start = timeit.default_timer()
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t


# In[2]:


spark = SparkSession.builder.appName('SkylineETL').getOrCreate()


# In[3]:


# function to remove all leading and trailing whitespace
def remove_whitespaces(df):
    for col in df.columns:
        df = df.withColumnRenamed(col, col.strip().upper().replace(" ","_"))
    for col in df.columns:
        df = df.withColumn(col, f.upper(f.trim(df[col])))
    return df


# In[4]:


# function to add source column
def add_source(df, fname):
    name = fname.split('_')
    df = df.withColumn('SOURCE', f.lit(name[0]).cast('string'))
    return df


# In[5]:


def format_Date(df):
    df = df.withColumn('DATE', f.from_unixtime(f.unix_timestamp(df.DATE, 'MM/dd/yyyy'), 'yyyy-MM-dd'))
    return df


# In[6]:


def format_Timestamp(df):
    df = df.withColumn('DATETIME', f.concat_ws(' ', df.DATE, df.TIME))
    df = df.withColumn('DATETIME', f.from_unixtime(f.unix_timestamp(df.DATETIME, 'yyyy-MM-dd HH:mm'), 'yyyy-MM-dd HH:mm:ss'))
    df = df.drop('TIME', 'DATE')
    return df


# In[7]:


@f.udf(t.StructType([t.StructField("CITY", t.StringType(), True), t.StructField("STATE", t.StringType(), True)]))
def city_state(array):
    if len(array) == 1:
        return(array[0], ' ')
    else:
        return(' '.join(array[0:len(array)-1]), array[len(array)-1])


# In[8]:


def airport(df):
    df = df.withColumn('STREET_ADDRESS', f.when(f.col('CITY_STATE') == 'LAG', '102-05 DITMARS BLVD').otherwise(f.col('STREET_ADDRESS')))
    df = df.withColumn('CITY_STATE', f.regexp_replace('CITY_STATE', 'LAG', 'EAST ELMHURST NY'))
    
    df = df.withColumn('STREET_ADDRESS', f.when(f.col('CITY_STATE') == 'JFK', '148-18 134TH ST').otherwise(f.col('STREET_ADDRESS')))
    df = df.withColumn('CITY_STATE', f.regexp_replace('CITY_STATE', 'JFK', 'JAMAICA NY'))
    
    df = df.withColumn('STREET_ADDRESS', f.when(f.col('CITY_STATE') == 'NWK', '3 BREWSTER RD').otherwise(f.col('STREET_ADDRESS')))
    df = df.withColumn('CITY_STATE', f.regexp_replace('CITY_STATE', 'NWK', 'NEWARK NJ'))
    
    df = df.withColumn('STREET_ADDRESS', f.when(f.col('CITY_STATE') == 'BDL', 'SCHOEPHOESTER RD').otherwise(f.col('STREET_ADDRESS')))
    df = df.withColumn('CITY_STATE', f.regexp_replace('CITY_STATE', 'BDL', 'WINDSOR LOCKS CT'))
    
    df = df.withColumn('STREET_ADDRESS', f.when(f.col('CITY_STATE') == 'WEA', '240 AIRPORT RD').otherwise(f.col('STREET_ADDRESS')))
    df = df.withColumn('CITY_STATE', f.regexp_replace('CITY_STATE', 'WEA', 'WHITE PLAINS NY'))
    
    df = df.withColumn('STREET_ADDRESS', f.when(f.col('CITY_STATE') == 'TTB', '111 INDUSTRIAL AVE').otherwise(f.col('STREET_ADDRESS')))
    df = df.withColumn('CITY_STATE', f.regexp_replace('CITY_STATE', 'TTB', 'TETERBORO NJ'))
    
    df = df.withColumn('STREET_ADDRESS', f.when(f.col('CITY_STATE') == 'MCA', '100 ARRIVAL AVE').otherwise(f.col('STREET_ADDRESS')))
    df = df.withColumn('CITY_STATE', f.regexp_replace('CITY_STATE', 'MCA', 'RONKONKOMA NY'))
    
    df = df.withColumn('STREET_ADDRESS', f.when(f.col('CITY_STATE') == 'NBG', '4000 HADLEY ROAD MC 429').otherwise(f.col('STREET_ADDRESS')))
    df = df.withColumn('CITY_STATE', f.regexp_replace('CITY_STATE', 'NBG', 'SOUTH PLAINFIELD NJ'))
    
    return df


# In[9]:


def ny_city_state(df):
    df = df.withColumn('CITY_STATE', f.when(df.CITY_STATE.contains('NEW YORK'), 'NEW YORK CITY NY').otherwise(f.col('CITY_STATE')))
    df = df.withColumn('CITY_STATE', f.when(df.CITY_STATE.endswith('QU'), 'QUEENS NY').otherwise(f.col('CITY_STATE')))
    df = df.withColumn('CITY_STATE', f.when(df.CITY_STATE.endswith('BX'), 'BRONX NY').otherwise(f.col('CITY_STATE')))
    df = df.withColumn('CITY_STATE', f.when((df.CITY_STATE.endswith('M')) | (df.CITY_STATE.endswith('M M')), 'MANHATTAN NY').otherwise(f.col('CITY_STATE')))
    df = df.withColumn('CITY_STATE', f.when(df.CITY_STATE.endswith('BK'), 'BROOKLYN NY').otherwise(f.col('CITY_STATE')))
    df = df.withColumn('CITY_STATE', f.when(df.CITY_STATE.endswith('SI'), 'STATEN ISLAND NY').otherwise(f.col('CITY_STATE')))
    return df


# In[10]:


def state(df):
    df = df.withColumn('STATE', f.when((df.STATE.endswith('LI')) | (df.STATE.endswith('WE')), 'NY').otherwise(f.col('STATE')))
    return df


# In[11]:


def streets(df):
    df = df.withColumn('STREET_ADDRESS', f.regexp_replace('STREET_ADDRESS', 'FIRST', '1ST'))
    df = df.withColumn('STREET_ADDRESS', f.regexp_replace('STREET_ADDRESS', 'SECOND', '2ND'))
    df = df.withColumn('STREET_ADDRESS', f.regexp_replace('STREET_ADDRESS', 'THIRD', '3RD'))
    df = df.withColumn('STREET_ADDRESS', f.regexp_replace('STREET_ADDRESS', 'FOUR', '4'))
    df = df.withColumn('STREET_ADDRESS', f.regexp_replace('STREET_ADDRESS', 'FIFTH', '5TH'))
    df = df.withColumn('STREET_ADDRESS', f.regexp_replace('STREET_ADDRESS', 'SIX', '6'))
    df = df.withColumn('STREET_ADDRESS', f.regexp_replace('STREET_ADDRESS', 'SEVEN', '7'))
    df = df.withColumn('STREET_ADDRESS', f.regexp_replace('STREET_ADDRESS', 'EIGHTH', '8TH'))
    df = df.withColumn('STREET_ADDRESS', f.regexp_replace('STREET_ADDRESS', 'NINTH', '9TH'))
    
    df = df.withColumn('STREET_ADDRESS', f.when((df.STREET_ADDRESS.endswith('HY')), f.regexp_replace('STREET_ADDRESS', 'HY', 'HWY')).otherwise(f.col('STREET_ADDRESS')))
    df = df.withColumn('STREET_ADDRESS', f.when((df.STREET_ADDRESS.endswith('BL')), f.regexp_replace('STREET_ADDRESS', 'BL', 'BLVD')).otherwise(f.col('STREET_ADDRESS')))
    df = df.withColumn('STREET_ADDRESS', f.when((df.STREET_ADDRESS.contains('PZ')), f.regexp_replace('STREET_ADDRESS', 'PZ', 'PLZ')).otherwise(f.col('STREET_ADDRESS')))
    return df


# In[12]:


def format_Table(df):
    df = df.select('DATETIME', 'STREET_ADDRESS', 'CITY', 'STATE', 'SOURCE')
    return df


# In[13]:


path = 'C://ChloePark/Bootcamp/Labs/SCD_Lab/'
fname = 'Skyline_B00111.csv'
skyline = spark.read.csv(path+fname,inferSchema=True, header=True)
# skyline, _ = skyline.randomSplit([0.0008, 0.9992])


# In[14]:


skyline = remove_whitespaces(skyline)
skyline = add_source(skyline, fname)
skyline = format_Date(skyline)
skyline = format_Timestamp(skyline)


# In[15]:


skyline = airport(skyline)


# In[16]:


skyline = ny_city_state(skyline)


# In[17]:


skyline = skyline.withColumn('CITY_STATE', city_state(f.split('CITY_STATE', ' '))).select(f.col('DATETIME'), f.col('STREET_ADDRESS'), f.col('CITY_STATE.*'), f.col('SOURCE'))


# In[18]:


skyline = state(skyline)
skyline = streets(skyline)
skyline = format_Table(skyline)


# In[19]:


skyline.repartition(1).write.csv(path+'Skyline_Full_ETL', header=True, mode='overwrite')


# In[20]:


spark.stop()


# In[21]:


stop = timeit.default_timer()
print('Skyline Standardization Time:', (stop-start))

