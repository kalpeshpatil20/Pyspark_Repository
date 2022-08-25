from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('spark_practice').getOrCreate()

data = "E:\\spark_Big_Data\\drivers\\donations.csv"
# df = spark.read.format('csv').load(data)

# If data contains header then use following syntax
#df = spark.read.format('csv').option('header',True).option('inferSchema',True).load(data)
#df.show()
#df.printSchema()

# If data contains more than one header line then use following syntax to clean the data first.
rdd = spark.sparkContext.textFile(data)
skip = rdd.first()
odata = rdd.filter(lambda x : x!=skip)
df1 = spark.read.csv(odata,header=True,inferSchema=True)
#df1.show()
#df1.printSchema()

data1 = "E:\\spark_Big_Data\\drivers\\bank-full.csv"
df2 = spark.read.format('csv').option('sep',';').option('header',True).option('inferSchema',True).load(data1)
#df2.show()
#df2.printSchema()
#df3 = df2.groupby(col('marital')).agg(count('marital').alias('cnt')).orderBy('cnt',ascending=False)
#df3.show()
#df3.printSchema()
#res = df2.where(col('age')>70)
#res = df2.where((col('age')>70) & (col('marital')!= 'married'))
#res = df2.where((col('age')>60) & (col('balance')>50000))
#res = df2.groupby(col('marital'),col('job')).agg(count('marital').alias('cnt')).orderBy('cnt',ascending=False)
#res = df2.select(col('age'),col('marital'),col('job'),col('balance')).where((col('age')>60) & (col('balance')>50000))
#res = df2.where((col('job')== 'self-employed') & (col('balance')>50000))
#res = df2.where((col('age')<40) & (col('balance')>50000))
df2.createOrReplaceTempView('tab')
#res = spark.sql('select * from tab')
#res = spark.sql("select * from tab where age>60 and marital=='single'")
#res = df2.where((col('age')>60) & (col('marital')=='single'))
#res = spark.sql("select marital,count(*) cnt from tab group by marital order by cnt desc")
#res = spark.sql("select marital,count(*) count,sum(balance) tot_balance,avg(balance) avg_balance from tab group by marital order by tot_balance desc")
#res = spark.sql("select avg(balance) avg_balance from tab where marital='married'")
#res.show()
#res.printSchema()

data11 = "E:\\bigdatafile\\10000Records.csv"
df22 = spark.read.format('csv').option('header',True).option('inferSchema',True).load(data11)
#df22.show()
#df22.printSchema()
import re
#num = int(df22.count())
cols = [re.sub('[^a-zA-Z0-9]','_',c.lower()) for c in df22.columns]
ndf = df22.toDF(*cols)
ndf.show()
ndf.printSchema()

