from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('spark_practice').getOrCreate()
sc = spark.sparkContext

#rdd3 = sc.parallelize([1,4,5,64,3])
#print(rdd3.collect())

data = 'E:\\spark_Big_Data\\drivers\\donations.csv'
drdd = sc.textFile(data)
#print(drdd.collect())
ndrdd = drdd.map(lambda x: x.split(","))
#print(ndrdd.collect())
ndrdd1 = drdd.filter(lambda x: 'kalpesh' in x)
#print(ndrdd1.collect())
don = drdd.filter(lambda x:'dt' not in x).map(lambda x: x.split(",")).filter(lambda x:int(x[2])>=3000)
#print(don.collect())
#don1 = drdd.filter(lambda x: 'dt' not in x).map(lambda x:x.split(","))\
#           .map(lambda x:(x[0],int(x[2]))).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1])
#print(don1.collect())

don2 = drdd.flatMap(lambda x: x.split(",")).map(lambda x:x.split(',')).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
print(don2.collect())