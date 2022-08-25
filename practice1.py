from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import re

spark = SparkSession.builder.master('local[*]').appName('spark_practice1').getOrCreate()
data = 'E:\\spark_Big_Data\\drivers\\donations.csv'
data1 = 'E:\\bigdatafile\\employees.csv'
df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(data1)
df1 = df = df.applymap(lambda x: x.replace("'", ""))
df1.show()
df1.printSchema()