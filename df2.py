from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master('local[*]').appName('spark_practice').getOrCreate()
data = "E:\\bigdatafile\\us-500.csv"
df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(data)
#df.show()
#df.printSchema()
#ndf = df.groupby(col('state')).agg(count('state').alias('cnt')).orderBy(col('cnt').desc())
#ndf = df.withColumn('fullname',concat_ws(' ',col('first_name'),col('last_name')))
#ndf = df.withColumn('fullname', concat_ws('_', df.first_name, df.last_name))
#ndf = df.withColumn('state', when(col('state')=='CA','California').when(col('state')=='NY','NewYork').otherwise(col('state')))
#ndf = df.withColumn('phone1', regexp_replace('phone1', '-', '').cast(LongType()))\
#        .withColumn('phone2', regexp_replace('phone2', '-', '').cast(LongType()))
#ndf.show()
#ndf.printSchema()
#ddf = df.select('first_name', 'last_name', 'city', 'state').withColumn('fname', col('first_name'))
#ddf = df.withColumn('fullname', concat_ws(' ', df.first_name, df.last_name))\
#        .withColumn('lit', lit(18))\
#        .withColumnRenamed('first_name','fname').withColumnRenamed('last_name', 'lname')
#ddf1 = ddf.drop('fname', 'lname', 'web')
#ddf1.show()

#   collect_list & collect_set functions ( both are aggregate functions)
#ndf = df.groupby('state').agg(countDistinct(col('city')).alias('cnt')).orderBy(col('cnt').desc())
#ndf = df.groupby('state').agg(count(col('first_name')).alias('cnt'), collect_list('first_name')).orderBy(col('cnt'))
#ndf = df.groupby('state').agg(count(col('city')).alias('city_count'), collect_list('city')).orderBy(col('city_count').desc())
#ndf = df.groupby('state').agg(count(col('city')).alias('city_count'), collect_set('city')).orderBy(col('city_count').desc())
#ndf.show(truncate=False)

# Difference between when and regexp_replace
#res = df.withColumn('address1', when(col('address').contains('#'), '******').otherwise(col('address')))\
#        .withColumn('address2', regexp_replace('address', '#', '-'))
#res.show()

#     substring  & substring_index functions
res = df.select('email').withColumn('substr', substring('email', 0, 5))\
                        .withColumn('substr_index0', substring_index('email', '@', 0))\
                        .withColumn('substr_index1', substring_index('email', '@', 1))\
                        .withColumn('substr_index-1', substring_index('email', '@', -1))
#res.show()

df.createOrReplaceTempView('tab')
qry = """with tmp as (select *, concat_ws('_', first_name,last_name) fullname,substring_index(email,'@',-1) mail from tab)
         select mail, count(*) cnt from tmp group by mail order by cnt desc 
     """
dff = spark.sql(qry)
#dff.show()

def func(st):
    if st == 'NY':
        return '30% off'
    elif st == 'CA':
        return '40% off'
    elif st == 'OH':
        return '50% off'
    else:
        return '500/- off'

uf = udf(func)
spark.udf.register('offer',uf)
#ndf = spark.sql('select *,offer(state) todayoffer from tab')
ndf = df.withColumn('todayoffers', uf(col('state')))
ndf.show()