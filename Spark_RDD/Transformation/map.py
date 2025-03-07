from pyspark import SparkContext, SparkConf

conf = SparkConf() \
    .setAppName("Spark_RDD") \
    .setMaster("local[*]") \
    .set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

fileRdd = sc.textFile("../Data/data.txt")

mapRdd = fileRdd.map(lambda line : line.upper()) #Giống vòng For
for line in mapRdd.collect():
    print(line)

