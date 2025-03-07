from pyspark import SparkContext, SparkConf

conf = SparkConf() \
    .setAppName("Spark_RDD") \
    .setMaster("local[*]") \
    .set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)


fileRdd = sc.textFile("../Data/data.txt")

flatMapRdd = fileRdd.flatMap(lambda line: line.split(" "))

print(flatMapRdd.collect())