import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t

def remove_whitespaces(df):
    for col in df.columns:
        df = df.withColumnRenamed(col, col.strip().upper().replace(" ","_"))
    for col in df.columns:
        df = df.withColumn(col, f.upper(f.trim(df[col])))
    return df

def add_source(df, fname):
    name = fname.split('_')
    df = df.withColumn('SOURCE', f.lit(name[0]).cast('string'))
    return df

def format_Date(df):
    df = df.withColumn('DATE', f.from_unixtime(f.unix_timestamp(df.DATE, 'yyyy.MM.dd'), 'yyyy-MM-dd'))
    return df

def format_Timestamp(df):
    df = df.withColumn('DATETIME', f.concat_ws(' ', df.DATE, df.TIME))
    df = df.withColumn('DATETIME', f.from_unixtime(f.unix_timestamp(df.DATETIME, 'yyyy-MM-dd HH:mm'), 'yyyy-MM-dd HH:mm:ss'))
    df = df.drop('TIME', 'DATE')
    return df

def city_name(df):
    df = df.withColumn('PUFROM', f.when((df.PUFROM.startswith('WE')) & (df.PUFROM.endswith('RK')), 'NEW YORK CITY').otherwise(f.col('PUFROM')))
    df = df.withColumn('PUFROM', f.when((df.STATE == 'M'), 'MANHATTAN').otherwise(f.col('PUFROM')))
    df = df.withColumn('STATE', f.when((df.STATE == 'M'), 'NY').otherwise(f.col('STATE')))
    return df

def street_num(string):
    array = string.split(' ')
    j = -1
    for i in range(len(array)):
        if (j < 0) and (array[i].isdigit()):
            j = i
            break
    if (j >= 0) and (len(array[j]) > 1):
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
    elif (j >= 0) and (len(array[j]) == 1):
        if array[j][-1] == '1':
            array[j] = array[j] + 'ST'
        elif array[j][-1] == '2':
            array[j] = array[j] + 'ND'
        elif array[j][-1] == '3':
            array[j] = array[j] + 'RD'
        else:
            array[j] = array[j] + 'TH'
    return ' '.join(array)

streetnum_udf = f.udf(street_num, t.StringType())

def streets(df):
    df = df.withColumn('STREET', f.regexp_replace('STREET', ',', ''))
    df = df.withColumn('STREET', f.regexp_replace('STREET', 'FIRST', '1'))
    df = df.withColumn('STREET', f.regexp_replace('STREET', 'SECOND', '2'))
    df = df.withColumn('STREET', f.regexp_replace('STREET', 'THIRD', '3'))
    df = df.withColumn('STREET', f.regexp_replace('STREET', 'FOUR', '4'))
    df = df.withColumn('STREET', f.regexp_replace('STREET', 'FIFTH', '5'))
    df = df.withColumn('STREET', f.regexp_replace('STREET', 'SIX', '6'))
    df = df.withColumn('STREET', f.regexp_replace('STREET', 'SEVEN', '7'))
    df = df.withColumn('STREET', f.regexp_replace('STREET', 'EIGHTH', '8'))
    df = df.withColumn('STREET', f.regexp_replace('STREET', 'NINTH', '9'))
    
    df = df.withColumn('STREET', f.when((df.STREET.startswith('CORP ')), f.regexp_replace('STREET', 'CORP ', 'CORPORAL ')).otherwise(f.col('STREET')))
    df = df.withColumn('STREET', f.when((df.STREET.endswith(' HY')), f.regexp_replace('STREET', ' HY', ' HWY')).otherwise(f.col('STREET')))
    df = df.withColumn('STREET', f.when((df.STREET.endswith(' BL')), f.regexp_replace('STREET', ' BL', ' BLVD')).otherwise(f.col('STREET')))
#     df = df.withColumn('STREET', when((df.STREET.contains(' PZ')), regexp_replace('STREET', ' PZ', ' PLZ')).otherwise(col('STREET')))
    return df

def street_add(df):
    df = df.withColumn('STREET_ADDRESS', f.concat_ws(' ', df.ADDRESS, df.STREET))
    return df.select('DATETIME', 'STREET_ADDRESS', 'PUFROM', 'STATE', 'SOURCE')

def airport(df, code, str_add, city, state):
    df = df.withColumn('CITY', f.when((df.STATE.startswith(code)), city).otherwise(f.col('CITY')))
    df = df.withColumn('STREET_ADDRESS', f.when(df.STATE.startswith(code), str_add).otherwise(f.col('STREET_ADDRESS')))
    df = df.withColumn('STATE', f.when(df.STATE.startswith(code), state).otherwise(f.col('STATE')))
    
    return df

def format_Table(df):
    df = df.select('DATETIME', 'STREET_ADDRESS', 'CITY', 'STATE', 'SOURCE')
    return df

spark = SparkSession.builder.appName('Dial7').getOrCreate()

path = "C:/ChloePark/Bootcamp/Capstone/"
fname = 'Dial7_B00887.csv'
dial7 = spark.read.csv(path + fname, inferSchema=True, header=True)

dial7 = remove_whitespaces(dial7)
dial7 = add_source(dial7, fname)
dial7 = format_Date(dial7)
dial7 = format_Timestamp(dial7)
dial7 = city_name(dial7)
dial7 = dial7.withColumn('STREET', streetnum_udf(dial7.STREET))
dial7 = streets(dial7)
dial7 = street_add(dial7)

dial7 = dial7.withColumnRenamed('PUFROM', 'CITY')

dial7 = airport(dial7, 'ISP', '100 ARRIVAL AVE', 'RONKONKOMA', 'NY')
dial7 = airport(dial7, 'TEB', '111 INDUSTRIAL AVE', 'TETERBORO', 'NJ')
dial7 = airport(dial7, 'HPN', '240 AIRPORT RD', 'WHITE PLAINS', 'NY')

dial7 = airport(dial7, 'NWK', '3 BREWSTER RD', 'NEWARK', 'NJ')
dial7 = airport(dial7, 'EWR', '3 BREWSTER RD', 'NEWARK', 'NJ')

dial7 = airport(dial7, 'JFK', '148-18 134TH ST', 'JAMAICA', 'NY')

dial7 = airport(dial7, 'LGA', '102-05 DITMARS BLVD', 'EAST ELMHURST', 'NY')
dial7 = airport(dial7, 'LAG', '102-05 DITMARS BLVD', 'EAST ELMHURST', 'NY')

dial7.repartition(1).write.csv(path+'Dial7_ETL', header=True, mode='overwrite')