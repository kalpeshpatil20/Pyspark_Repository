from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.master('local[*]').appName('spark_window_func').getOrCreate()
data = "E:\\bigdatafile\\us-500.csv"
df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(data)
df1 = df.drop('web', 'email', 'phone1', 'phone2', 'company_name').withColumnRenamed('zip', 'sal')

'''
#df2 = df1.orderBy(col('sal').desc())
win = Window.partitionBy(col('state')).orderBy(col('sal').desc())
df2 = df1.withColumn('rnk', rank().over(win))\
         .withColumn('drnk', dense_rank().over(win))\
         .withColumn('rn', row_number().over(win))\
         .withColumn('lead', lead(col('sal')).over(win))\
         .withColumn('diff', col('sal')-col('lead'))\
         .withColumn('lag', lag('sal').over(win))\
         .withColumn('diff1', col('sal')-col('lag'))\
         .withColumn('prnk', percent_rank().over(win))\
         .withColumn('ntile', ntile(4).over(win))
#         .where(col('drnk')==2)
df2.show()
df2.printSchema()'''

df1.createOrReplaceTempView('tab')
win = Window.partitionBy(col('state')).orderBy(col('sal'))
df3 = spark.sql("select *,row_number() over(partition by state order by sal desc) rn,"
                "rank() over(partition by state order by sal desc) rnk,"
                "dense_rank() over(partition by state order by sal desc) drnk,"
                "lead(sal) over(partition by state order by sal desc) lead,"
                "lag(sal) over(partition by state order by sal desc) lag from tab")
#df3 = spark.sql("select * from(select *,row_number() over(partition by state order by sal desc) rn ,"
#                "rank() over(partition by state order by sal desc) rnk ,"
#                "dense_rank() over(partition by state order by sal desc) drnk from tab) where rnk=2")
df3.show()
df3.printSchema()