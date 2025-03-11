from os import remove

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("mapPartitionWithIndex") \
    .setMaster('local[*]') \
    .set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

text = sc.parallelize(["DIT me thAng nay noi ngU vaI lOn chung may a"]) \
    .flatMap(lambda line: line.split(" ")) \
    .map(lambda word: word.lower())

#print(text.collect())

removeText = sc.parallelize(["dit me ngu vai lon"]) \
    .flatMap(lambda line: line.split(" "))

#print(removeText.collect())

niceText = text.subtract(removeText)

print(niceText.collect())