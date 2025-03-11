from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("mapPartitionWithIndex") \
    .setMaster('local[*]') \
    .set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)


rdd = sc.parallelize(
    [
        "one", 1, "two", 3, "three", "four", "two", "one", 2, 1
    ]
)

print(rdd.distinct().collect())