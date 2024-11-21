from pyspark import SparkContext
from pyspark.sql import SparkSession
sc= SparkContext.getOrCreate()
spark= SparkSession(sc)

rdd1=sc.parallelize([("Spark",1),("Hadooop",2)])
rdd2=sc.parallelize([("Spark",3),("Hadooop",6)])
rdd = sorted(rdd1.join(rdd2).collect())
print(rdd)