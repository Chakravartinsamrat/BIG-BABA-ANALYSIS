from pyspark.sql import SparkSession
sc= SparkSession.builder.appName("Word_count").getOrCreate()
text_file= sc.read.text("./Context/text.txt")
words= text_file.rdd.flatMap(lambda x: x[0].split(" "))
word_count= words.count()

print(word_count)