from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSVRDD").getOrCreate()

rdd= spark.read.format("csv").option("header","true").load("./Book.csv").rdd

print(rdd.take(5))
df= rdd.toDF()
df.describe().show()