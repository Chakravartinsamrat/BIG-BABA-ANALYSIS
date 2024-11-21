from pyspark import SparkContext
sc= SparkContext()

rdd= sc.parallelize({1,2,3},{3,4,5},{7,8,9})

acc= sc.accumulator(0)

def add_to_acc(x):
    global acc
    acc=0
    acc += sum(x)
    rdd.foreach(add_to_acc)
print("Sum oof numbers in RDD:",acc.value)
sc.stop()

