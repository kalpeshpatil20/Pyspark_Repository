from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

spark = SparkSession.builder.master('local[*]').appName('practice').getOrCreate()
'''
rdd = spark.sparkContext.parallelize([3,4,52,254,22,11,66])
rdd.first()
#print(rdd.take(2))

data = 'E:\\spark_Big_Data\\drivers\\donations.csv'
rdd1 = spark.sparkContext.textFile(data)
#df = spark.read.format('csv').option('header',True).option('inferSchema',True).load(data) --> shows wrong data
skip = rdd1.first()
#print(skip)
odata = rdd1.filter(lambda x: x != skip)
skip1 = odata.first()
odata1 = odata.filter(lambda x:x!=skip1)
#print(odata.collect())
df = spark.read.csv(odata1, header=True, inferSchema=True)
df.show()
'''
'''
data = "E:\\spark_Big_Data\\drivers\\asl.csv"
rdd = spark.sparkContext.textFile(data)
#print(rdd.collect())
rdd1 = rdd.flatMap(lambda x : x.split(',')).map(lambda x: (x,1)).reduceByKey(lambda x,y:(x+y)).sortBy(lambda x: x[1],ascending=False)
rdd2 = rdd.filter(lambda x:'age' not in x)
rdd3 = rdd.filter(lambda x: 'hyd' in x)
print(rdd1.collect())
df = spark.read.format('csv').option('header',True).option('inferSchema',True).load(data)
df1 = df.groupby('city').agg(count(col('city')).alias('cnt'))
df2 = df.select(current_timestamp()).filter(col('city') == 'hyd')
#df2.show(truncate=False)

df.createOrReplaceTempView('tab')
df3 = spark.sql("select * from tab where city = 'hyd'")
df3.show()
'''
"""
data = 'E:\\bigdatafile\\us-500.csv'
df = spark.read.format('csv').option('header',True).option('inferSchema',True).load(data)
df2 = df.groupby('state').agg(count(col('city')).alias('city_count'), collect_set(col('city'))).orderBy(col('city_count').desc())
#df2.show()
df.createOrReplaceTempView('tab')
qry = '''with tmp as (select *, concat_ws(' ', first_name, last_name) fullname, substring_index(email, '@', -1) mail from tab)
         select mail, count(*) cnt from tmp group by mail order by cnt desc
      '''
dff = spark.sql(qry)
#dff.show()
"""
'''
df1 = df.withColumn('fullname', concat_ws(' ', col('first_name'), col('last_name')))\
        .withColumn('phone1', regexp_replace(col('phone1'), '-', '')).withColumn('phone2', regexp_replace(col('phone2'), '-', ''))\
        .withColumn('mail', substring(col('email'), 0, 5))\
        .withColumn('mail1', substring_index(col('email'),'@',1)).withColumn('mail2', substring_index(col('email'), '@', -1))\
        .withColumn('state', when(col('state')=='NJ','New Jersey').when(col('state')=='CA', 'California').otherwise(col('state')))\
        .withColumn('address1', when(col('address').contains('#'), '******').otherwise(col('address')))\
        .withColumn('address2', regexp_replace(col('address'), '#', '-'))
df1.show()
df1.printSchema()
'''
"""
data = 'E:\\bigdatafile\\10000Records.csv'
df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(data)
cols = [re.sub('[^a-zA-Z0-9]', '-', c) for c in df.columns]
dff = df.toDF(*cols)

dff1 = dff.select(col('first-name'),col('middle-initial'), col('last-name'), col('gender'), col('e-mail'), col('date-of-birth'), col('date-of-joining'))\
          .withColumn('fullname', concat_ws(' ', 'first-name','middle-initial','last-name'))\
          .withColumn('gender1', when(col('gender') == 'M', 'Male').when(col('gender') == 'F', 'Female').otherwise(col('gender')))\
          .withColumn('gender2', (regexp_replace('gender', 'M', 'Male')))\
          .withColumn('mail', substring(col('e-mail'), 0, 5))\
          .withColumn('mail1', substring_index(col('e-mail'), '@', 1))\
          .withColumn('mail2', substring_index(col('e-mail'), '@', -1))
dff1.show(truncate=False)
#dff.printSchema()
"""
'''
dff1 = df.withColumn('date of joining', to_date(col('date of joining'), 'M/d/yyyy'))\
         .withColumn('date of birth', to_date(col('date of birth'), 'M/d/yyyy'))

dff2 = dff1.filter((year(col('date of joining'))> 2015) & (month(col('date of joining'))>= 5) & (col('salary')> 100000) & (col('gender') == 'F'))\
           .select(col('first name'), col('gender'), col('date of joining'), col('salary'))
#dff2.show()
#dff2.printSchema()

dff3 = dff1.filter(col('salary') > 150000).groupby('gender').agg(count(col('gender')).alias('tot_person'))
dff3.show()
'''
'''
def func(dt):
        y = int(dt/365)
        m = int((dt%365)/30)
        d = int((dt%365)%30)
        return f"{y} years {m} months {d} days"

#func(999)

uf = udf(func)

data = 'E:\\spark_Big_Data\\drivers\\donations.csv'
df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(data)
df1 = df.withColumn('dt', to_date(col('dt'), 'd-M-yyyy'))\
        .withColumn('ts', to_timestamp(col('dt')))\
        .withColumn('today', current_date())\
        .withColumn('todayts', current_timestamp())\
        .withColumn('dtdiff', datediff(col('today'), col('dt')))\
        .withColumn('dtadd', date_add(col('dt'), 100))\
        .withColumn('dtsub', date_sub(col('dt'), 100))\
        .withColumn('last_day', last_day(col('dt')))\
        .withColumn('next_day', next_day(col('dt'), 'Wed'))\
        .withColumn('trunc_month', date_trunc('month', col('dt')).cast(DateType()))\
        .withColumn('year', year(col('dt')))\
        .withColumn('month', month(col('dt')))\
        .withColumn('day', dayofmonth(col('dt')))\
        .withColumn('dow', dayofweek(col('dt')))\
        .withColumn('doy', dayofyear(col('dt')))\
        .withColumn('mon_bet', months_between(col('today'), col('dt')))\
        .withColumn('floor', floor(col('mon_bet')))\
        .withColumn('ceil', ceil(col('mon_bet')))\
        .withColumn('round', round(col('mon_bet')))\
        .withColumn('unixts', unix_timestamp(col('dt')))\
        .withColumn('fromunixts', from_unixtime(col('unixts'), 'EE dd/MMM/yyyy'))\
        .withColumn('lastfriday', date_format(next_day(date_sub(last_day(col('dt')), 7), 'Fri'), 'EE dd-MMM-yyyy'))\
        .withColumn('dayon15', date_format(date_add(date_trunc('month', col('dt')), 14), 'EE dd-MMM-yyyy'))\
        .withColumn('ndtdiff', uf(col('dtdiff')))

df1.show(truncate=False)
df1.printSchema()
'''
data = 'E:\\spark_Big_Data\\drivers\\donations.csv'
df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(data)
df.createOrReplaceTempView('tab')
df1 = spark.sql("select *,to_date(dt, 'd-M-yyyy') ndt, current_date() today,current_timestamp() cts from tab")
df1.createOrReplaceTempView('tab1')
df2 = spark.sql("select *,date_add(ndt,100) dtadd,date_sub(ndt,100) dtsub,"
                "datediff(today,ndt) dtdiff,year(ndt) year,month(ndt) month,dayofmonth(ndt) day,"
                "last_day(ndt) lastdt,date_format(next_day(ndt,'Fri'),'EE dd-MMM-yyyy') dtformat,"
                "date_trunc('month',ndt) trunc,"
                "add_months(ndt,5) monadd,"
                "months_between(today,ndt) mon_bet,"
                "unix_timestamp(ndt) unixts,"
                "unix_timestamp(cts) unix from tab1")
df2.show(truncate=False)
df2.printSchema()


'''data = "E:\\spark_Big_Data\\drivers\\bank-full.csv"
rdd = spark.sparkContext.textFile(data)
rdd1 = rdd.map(lambda x: x.replace("\"","")).map(lambda x: x.split(';')) # use " " in split ";"gives wrong o/p
#print(rdd1.collect())
df = spark.read.csv(rdd1, header=True)
df.show()'''


