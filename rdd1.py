from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
'''
rk = spark.sparkContext.parallelize([1,2,3,4,5])
print(rk.collect())
'''
data = "E:\\spark_Big_Data\\drivers\\asl.csv"

rdd1 = spark.sparkContext.textFile(data)
#rdd2 = rdd1.filter(lambda x:'age' not in x).map(lambda x:x.split(",")).map(lambda x:(x[2],1)).reduceByKey(lambda x,y:x+y)
#print(rdd2.collect())
rdd3 = rdd1.filter(lambda x : 'age' not in x).map(lambda x : x.split(",")).filter(lambda x: int(x[1])>30)
print(rdd3.collect())
rdd4 = rdd1.filter(lambda x: 'hyd' in x)
print(rdd4.collect())

