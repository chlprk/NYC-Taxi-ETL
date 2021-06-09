#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t
import string


# In[2]:


import timeit
start = timeit.default_timer()


# In[3]:


spark = SparkSession.builder.appName('FirstClassETL').getOrCreate()


# In[4]:


# function to remove all leading and trailing whitespace
def remove_whitespaces(df):
    for col in df.columns:
        df = df.withColumnRenamed(col, col.strip().upper().replace(" ","_"))
    for col in df.columns:
        df = df.withColumn(col, f.trim(df[col]))
        df = df.withColumn(col, f.upper(df[col]))
    return df


# In[5]:


# function to add source column
def add_source(df, fname):
    name = fname.split('_')
    df = df.withColumn('SOURCE', f.lit(name[0]).cast('string'))
    return df


# In[6]:


def format_Date(df):
    df = df.withColumn('DATE', f.from_unixtime(f.unix_timestamp(df.DATE, 'MM/dd/yyyy'), 'yyyy-MM-dd'))
    return df


# In[7]:


def format_Timestamp(df):
    df = df.withColumn('DATETIME', f.concat_ws(' ', df.DATE, df.TIME))
    df = df.withColumn('DATETIME', f.from_unixtime(f.unix_timestamp(df.DATETIME, 'yyyy-MM-dd hh:mm:ss a'), 'yyyy-MM-dd HH:mm:ss'))
    df = df.drop('TIME', 'DATE')
    return df


# In[8]:


def airport(df):
    df = df.withColumn('PICK_UP_ADDRESS', f.when(df.PICK_UP_ADDRESS.contains('JFK'), '148-18 134TH ST JAMAICA, NY').otherwise(df.PICK_UP_ADDRESS))
    df = df.withColumn('PICK_UP_ADDRESS', f.when((df.PICK_UP_ADDRESS.contains('LGA ') | df.PICK_UP_ADDRESS.contains('LA GUA')), '102-05 DITMARS BLVD EAST ELMHURST, NY').otherwise(df.PICK_UP_ADDRESS))
    df = df.withColumn('PICK_UP_ADDRESS', f.when((df.PICK_UP_ADDRESS.contains('NWK') | df.PICK_UP_ADDRESS.contains('EWR')), '3 BREWSTER RD NEWARK, NJ').otherwise(df.PICK_UP_ADDRESS))
    df = df.withColumn('PICK_UP_ADDRESS', f.when(df.PICK_UP_ADDRESS.contains('HPN'), '240 AIRPORT RD WHITE PLAINS, NY').otherwise(df.PICK_UP_ADDRESS))
    return df


# In[9]:


def street_num(string):
    array = string.split(' ')
    digits = []
    for i in range(len(array)):
        if array[i].isdigit():
            digits.append(i)
    if len(digits) > 1:
        j = digits[1]
        if len(array[j]) > 1:
            if array[j][-2] != '1':
                if array[j][-1] == '1':
                    array[j] = array[j] + 'ST'
                elif array[j][-1] == '2':
                    array[j] = array[j] + 'ND'
                elif array[j][-1] == '3':
                    array[j] = array[j] + 'RD'
                else:
                    array[j] = array[j] + 'TH'
            else:
                array[j] = array[j] + 'TH'
        elif len(array[j]) == 1:
            if array[j][-1] == '1':
                array[j] = array[j] + 'ST'
            elif array[j][-1] == '2':
                array[j] = array[j] + 'ND'
            elif array[j][-1] == '3':
                array[j] = array[j] + 'RD'
            else:
                array[j] = array[j] + 'TH'
    return ' '.join(array)

street_num_udf = f.udf(street_num, t.StringType())


# In[10]:


def street_type(df):
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', ' ROAD ', ' RD '))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', 'DRIVE', 'DR'))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', 'AVENUE', 'AVE'))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', 'AV ', 'AVE '))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', 'LANE', 'LN'))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', 'CIRCLE', 'CIR'))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', 'SQUARE', 'SQ'))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', 'TERRACE', 'TER'))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', 'TER W', 'TER'))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', 'TER E', 'TER'))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', 'CRESCENT', 'CRES'))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', 'PKY', 'PKWY'))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', 'PARKWAY', 'PKWY'))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', ' WALK ', ' WLK '))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', ' BL ', ' BLVD '))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', 'BLVD,', 'BLVD'))
    df = df.withColumn('PICK_UP_ADDRESS', f.regexp_replace('PICK_UP_ADDRESS', 'BOULEVARD', 'BLVD'))
    
    return df


# In[11]:


def splitAddress(text):
    nopunc = []
    for i in range(len(text)):
        if text[i] == ',' and i == len(text)-4:
            nopunc.append(text[i])
        elif text[i] not in string.punctuation:
            nopunc.append(text[i])

    text = ''.join(nopunc)
    
    end = len(text)
    
    street_type = ['TH ','ST', ' ST ',' RD ','MILE', 'LN ', 'CIR ', ' PARK ', ' HILL', ' SQ ', 'SQ,', ' CT ', ' OVERLOOK ', ' MALL, ',' MALL ',' OVAL ', ' PLZ ',' PLAZA ',' PL ', ' PLACE ',' TER ', ' CRES ', ' PKWY ', ' LN ', 'CONCOURSE ', ' WLK ', ' APTS ', ' HWY ', ' DR ',' BLVD ', ' EXPY ', 'EXPRESSWAY ','AVE ',' AVE D ',' LP ', ' LOOP ', 'WAY']
    digits = []
    for word in street_type:
        if word in text:
            end = text.find(word) + len(word)
    city_state = text[end:]
    if len(city_state) == 0:
        if ',' in text:
            end = text.rfind(' ', 0, text.rfind(' '))
        else:
            end = text.rfind(' ')
            
    city_state = text[end:]
    street = text[0:end]
    array = street.split()
    
    for i in range(len(array)):
        if array[i].isdigit():
            digits.append(i)
    if len(digits) != 0:
        start = digits[0]
    else:
        start = 0
    
    street = ' '.join(array[start:])
    
    return (street, city_state)
splitAdd_udf = f.udf(splitAddress, t.StructType([t.StructField('STREET_ADDRESS', t.StringType(), True), t.StructField('CITY_STATE', t.StringType(), True)]))


# In[12]:


# def city_state(df):
#     df = df.withColumn('CITY_STATE', f.when((df.CITY_STATE.endswith('QU')) | (df.CITY_STATE.endswith('QUEENS')), 'QUEENS, NY').otherwise(df.CITY_STATE))
#     df = df.withColumn('CITY_STATE', f.when((df.CITY_STATE.endswith('BX')) | (df.CITY_STATE.endswith('NX')) | df.CITY_STATE.endswith('ONC'), 'BRONX, NY').otherwise(df.CITY_STATE))
#     df = df.withColumn('CITY_STATE', f.when((df.CITY_STATE.endswith('BK')) | (df.CITY_STATE.contains('LYN')), 'BROOKLYN, NY').otherwise(df.CITY_STATE))
#     df = df.withColumn('CITY_STATE', f.when((df.CITY_STATE.endswith('SI')), 'STATEN ISLAND, NY').otherwise(df.CITY_STATE))
#     df = df.withColumn('CITY_STATE', f.when((df.CITY_STATE.endswith('NYC')) | (df.CITY_STATE.contains('YORK')), 'NEW YORK CITY, NY').otherwise(df.CITY_STATE))
#     df = df.withColumn('CITY_STATE', f.when(df.CITY_STATE.contains('YONK'), 'YONKERS, NY').otherwise(df.CITY_STATE))
# #     df = df.withColumn('CITY_STATE', f.when((df.CITY_STATE.endswith('QU')) | (df.CITY_STATE.endswith('ENS')) | (df.CITY_STATE.endswith('BX')) | (df.CITY_STATE.endswith('ONX')) | (df.CITY_STATE.endswith('ONC')) | (df.CITY_STATE.endswith('BK')) | (df.CITY_STATE.contains('KLYN')) | (df.CITY_STATE.endswith('NYC')) | (df.CITY_STATE.endswith('YORK')), 'NEW YORK CITY, NY'). otherwise(df.CITY_STATE))
#     df = df.withColumn('CITY', f.split(df.CITY_STATE, '\,')[0])
#     df = df.withColumn('STATE', f.split(df.CITY_STATE, '\,')[1])
#     df = df.withColumn('STATE', f.when(df.STATE=='LI', f.regexp_replace('STATE', 'LI', 'NY')).otherwise(df.STATE))
#     return df

def city_state(df):
    df = df.withColumn('STATE', f.substring(df.PICK_UP_ADDRESS, -3, 3))
    df = df.withColumn('CITY', f.lit(None))
    df = df.withColumn('CITY', f.when(((df.STATE=='NYC') | (df.STATE=='ORK')), 'NEW YORK CITY').otherwise(df.CITY))
    df = df.withColumn('CITY', f.when(((df.STATE==' BX') | (df.STATE=='ONX') | (df.STATE=='ONC')), 'BRONX').otherwise(df.CITY))
    df = df.withColumn('CITY', f.when(((df.STATE==' BK') | (df.STATE=='LYN')), 'BROOKLYN').otherwise(df.CITY))
    df = df.withColumn('CITY', f.when(((df.STATE==' QU') | (df.STATE=='ENS')), 'QUEENS').otherwise(df.CITY))
    df = df.withColumn('CITY', f.when(((df.STATE==' SI') | (df.STATE=='AND')), 'STATEN ISLAND').otherwise(df.CITY))
    df = df.withColumn('CITY', f.when(df.STATE=='NY.', 'YONKERS').otherwise(df.CITY))
    df = df.withColumn('STATE', f.when((df.STATE==' NY') | (df.STATE=='NY.') | (df.STATE=='NYC') | (df.STATE=='ORK') | (df.STATE==' BX') | (df.STATE=='ONX') | (df.STATE==' BK') | (df.STATE=='LYN') | (df.STATE==' QU') | (df.STATE=='ENS') | (df.STATE==' SI') | (df.STATE=='AND'), 'NY').otherwise(df.STATE))
    for col in df.columns:
        df = df.withColumn(col, f.trim(df[col]))
    return df


# In[13]:


def streetAdd(text, city, state):

    nopunc = []
    for i in range(len(text)):
        if text[i] not in string.punctuation:
            nopunc.append(text[i])

    text = ''.join(nopunc)
    
    end = len(text)
    
    street_type = ['TH ','ST', ' ST ',' RD ','MILE', 'LN ', 'CIR ', ' PARK ', ' HILL', ' SQ ', 'SQ,', ' CT ', ' OVERLOOK ', ' MALL, ',' MALL ',' OVAL ', ' PLZ ',' PLAZA ',' PL ', ' PLACE ',' TER ', ' CRES ', ' PKWY ', ' LN ', 'CONCOURSE ', ' WLK ', ' APTS ', ' HWY ', ' DR ',' BLVD ', ' EXPY ', 'EXPRESSWAY ','AVE ',' AVE D ',' LP ', ' LOOP ', 'WAY']
    digits = []
    for word in street_type:
        if word in text:
            end = text.find(word) + len(word)

    street = text[0:end]
    array = street.split()
    
    for i in range(len(array)):
        if array[i].isdigit():
            digits.append(i)
    if len(digits) != 0:
        start = digits[0]
    else:
        start = 0
    
    street = ' '.join(array[start:])
    
    if not city:
        if len(state) == 2:
            st_loc = text.find(state) - 1
            city = text[end:st_loc]
        else:
            city = city
    
    return street, city

streetAdd_udf = f.udf(streetAdd, t.StructType([t.StructField('STREET_ADDRESS', t.StringType(), True), t.StructField('CITY', t.StringType(), True)])) 


# In[14]:


def format_Table(df):
    df = df.select('DATETIME', 'STREET_ADDRESS', 'CITY', 'STATE', 'SOURCE')
    return df


# In[15]:


path = 'C://ChloePark/Bootcamp/Labs/SCD_Lab/'
fname = 'Firstclass_B01536.csv'


# In[16]:


# fname = 'FirstClass_Top100.csv'
firstclass = spark.read.csv(fname,inferSchema=True, header=True)
firstclass = remove_whitespaces(firstclass)
# firstclass.printSchema()


# In[17]:


firstclass = add_source(firstclass, fname)
firstclass = format_Date(firstclass)
firstclass = format_Timestamp(firstclass)
firstclass = airport(firstclass)
firstclass = street_type(firstclass)
firstclass = firstclass.withColumn('PICK_UP_ADDRESS', street_num_udf(firstclass.PICK_UP_ADDRESS))
firstclass = city_state(firstclass)
firstclass = firstclass.withColumn('STREET_ADDRESS', streetAdd_udf(firstclass.PICK_UP_ADDRESS, firstclass.CITY, firstclass.STATE)).select('DATETIME', 'STREET_ADDRESS.*', 'STATE', 'SOURCE')


# In[18]:


firstclass.repartition(1).write.csv(path + 'FirstClass_Full_ETL', header=True, mode="overwrite")


# In[19]:


spark.stop()


# In[20]:


stop = timeit.default_timer()
print('FirstClass Standardization Time:', (stop - start))

