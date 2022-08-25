from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

spark = SparkSession.builder.master('local[*]').appName('spark_practice').getOrCreate()

host = "jdbc:mysql://mysqldb.cabu8chtejdr.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
user = "myuser"
pwd = "mypassword"
'''
df = spark.read.format('jdbc').option('url', host).option('user', user).option('password', pwd)\
          .option('dbtable', 'emp').option('driver', 'com.mysql.jdbc.Driver').load()
ndf = df.na.fill(0) # replaces nulls in whole table with '0'
#ndf = df.na.fill(0, ['comm'])  only replaces nulls in comm column with '0'
#ndf.show()
#ndf.printSchema()

ndf.write.mode('overwrite').format('jdbc').option('url', host).option('user', user).option('password', pwd)\
          .option('dbtable', 'empclean').option('driver', 'com.mysql.jdbc.Driver').save()
'''
data = "E:\\bigdatafile\\employees.csv"
df2 = spark.read.format('csv').option('header', True).option('inferSchema', True).load(data)
cols = [re.sub('[^a-zA-Z0-9]', '_', c.lower()) for c in df2.columns]
df3 = df2.toDF(*cols)
#df3.show()
#df3.printSchema()

df3.write.mode('overwrite').format('jdbc').option('url', host).option('user', user).option('password', pwd)\
         .option('dbtable', 'records1000').option('driver', 'com.mysql.jdbc.Driver').save()
