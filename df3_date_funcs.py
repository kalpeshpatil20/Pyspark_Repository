from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master('local[*]').appName('spark_practice').getOrCreate()
data = 'E:\\spark_Big_Data\\drivers\\donations.csv'
df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(data)


# to_date() function converts string date eg( dd-MM-yyyy) into spark understandable date format i.e. yyyy-MM-dd
# current_date() function adds current date column in dataframe
# current_timestamp() function adds timestamp column in dataframe
# datediff() function gives difference between to dates in days
# date_add() function adds days in the date column
##.withColumn('dtadd', date_add(col('dt'), 100)) & .withColumn('dtsub', date_sub(col('dt'), -100)) Both are same
# date_sub() function substracts days from given date column
## .withColumn('dtsub', date_sub(col('dt'), 100)) & .withColumn('dtadd', date_add(col('dt'), -100)) Both are same
# last_day() function gives last day of given month eg.- Jan --> 31 , Feb 28 etc
# next_day() function gives the next date of the occurance of weekday
# date_format() function formats the date into the format we want (output of this fun is string dataType)
# date_trunc() function truncates date based on parameter passed i.e. 'year','month','day' etc
# months_between() function gives difference of months between two dates in months
# floor() --> eg. 10.3455 or 10.777 or whatever after decimal output will be --> 10 only
# ceil()  --> eg. 10.0342 or 10.999 or whatever after decimal output will be --> 11 only  it will roundoff
# round() --> it will round off if number after decimal is greater than .5
'''
df1 = df.withColumn('dt', to_date(col('dt'), 'd-M-yyyy'))\
        .withColumn('today', current_date())\
        .withColumn('ts', current_timestamp())\
        .withColumn('datediff', datediff(col('today'), col('dt')))\
        .withColumn('dtadd', date_add(col('dt'), 100))\
        .withColumn('dtsub', date_sub(col('dt'), 100))\
        .withColumn('lastdt', last_day(col('dt')))\
        .withColumn('nextday', next_day(col('dt'), 'Fri'))\
        .withColumn('dtforamt', date_format(col('dt'), 'EE dd-MMM-yyyy O z'))\
        .withColumn('trunc', date_trunc('month', col('dt')).cast(DateType()))\
        .withColumn('mon_bet', months_between(col('today'), col('dt')))\
        .withColumn('floor', floor(col('mon_bet')))\
        .withColumn('ceil', ceil(col('mon_bet')))\
        .withColumn('round', round(col('mon_bet')))
df1.show(truncate=False)
df1.printSchema()
'''

# year() function extracts year from date column
# month() function extracts month from date column
# dayofmonth() function extracts day from date column
'''
df = df.withColumn('dt', to_date(col('dt'), 'd-M-yyyy'))
df2 = df.withColumn('year', year(col('dt')))\
        .withColumn('month ', month(col('dt')))\
        .withColumn('day', dayofmonth(col('dt')))\
        .withColumn('dayofweek', dayofweek(col('dt')))\
        .withColumn('dayofyear', dayofyear(col('dt')))\
        .withColumn('weekofyear', weekofyear(col('dt')))
df2.show()
df2.printSchema()
'''
from math import *
def func(df):
    y = floor(df/365)
    ry = df%365
    m = floor(ry/30)
    rm = ry%30
    d = rm
    return f"{y} years {m} months {d} days"

#func(590)
uf = udf(func)
#spark.udf.register('dtdiff', uf)

df3 = df.withColumn('dt', to_date(col('dt'), 'd-M-yyyy'))\
        .withColumn('lastFri', next_day(date_sub(last_day(col('dt')), 7), 'Fri'))\
        .withColumn('dayon15', date_format(date_add(date_trunc('month', col('dt')).cast(DateType()), 14), 'EEEE dd-MM-yyyy'))\
        .withColumn('datediff', datediff(current_date(), col('dt')))\
        .withColumn('ndtdiff', uf(col('datediff')))\
        .withColumn('unixts', unix_timestamp(col('dt')))\
        .withColumn('ndt', to_timestamp(col('unixts')).cast(DateType()))\
        .withColumn('fromunixts', from_unixtime(col('unixts'), 'EE dd/MMM/yyyy'))

# unix_timestamp() function converts date column into unix timestamp
# from_unixtime() function converts unix timestamp into the format we want (o/p is in string dataType)
# to_timestamp() function converts date & unix_timestamp into timestamp
df3.show(truncate=False)
df3.printSchema()