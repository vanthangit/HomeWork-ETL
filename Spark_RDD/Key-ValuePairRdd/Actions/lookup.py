from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("LookUp") \
    .setMaster('local[*]') \
    .set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

data = sc.parallelize([('dat-debt', 5.0), ('tien-debt', 1.3),
                       ('dat-debt', 6.2), ('tien-debt', 8.1),
                       ('quanh-debt', 15.8)])

print(data.lookup("dat-debt"))
print(data.lookup("tien-debt"))