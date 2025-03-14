from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("key-value-pair") \
    .setMaster('local[*]') \
    .set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)


rdd = sc.parallelize(["xin chao moi nguoi minh la nguyen van thang"])

rdd2 = rdd.flatMap(lambda x: x.split(" "))
# print(rdd2.collect())

pairRDD = rdd2.map(lambda x: (len(x), x))
for pair in pairRDD.collect():
    print(pair)