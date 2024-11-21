from pyspark import SparkContext
sc= SparkContext()
rdd= sc.parallelize(["Tom","Jerry", "Tom & Jerry","Tom cat","Jerry rat"])
tc= rdd.count()
print(tc)
Tom_word= rdd.filter(lambda x: "Tom" in x)
Tom_words = Tom_word.collect()
print(Tom_words)
