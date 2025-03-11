from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("mapPartitionWithIndex") \
    .setMaster('local[*]') \
    .set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)


rdd1 = sc.parallelize([1,2,3,4,5])
rdd2 = sc.parallelize([1,233,3,433,5])

rdd3 = rdd1.intersection(rdd2)
print(rdd3.collect())