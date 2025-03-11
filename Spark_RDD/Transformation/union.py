from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("mapPartitionWithIndex") \
    .setMaster('local[*]') \
    .set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

rdd1 = sc.parallelize([1, 2, 3, 4, 5])
rdd2 = sc.parallelize([6, 7, 8, 9, 10])

rdd3 = rdd1.union(rdd2)
print(rdd3.collect())