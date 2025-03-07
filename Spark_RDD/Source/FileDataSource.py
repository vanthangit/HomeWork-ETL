from pyspark import SparkContext, SparkConf

conf = SparkConf() \
    .setAppName("Spark_RDD") \
    .setMaster("local[*]") \
    .set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)


fileRdd = sc.textFile("../Data/data.txt")
print(fileRdd.collect())
print(f"Number of data: {fileRdd.count()}")
print(f"First value data: {fileRdd.first()}")
