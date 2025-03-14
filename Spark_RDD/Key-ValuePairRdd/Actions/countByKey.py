from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("CountByKey") \
    .setMaster('local')

sc = SparkContext(conf=conf)

data = sc.parallelize([('dat-debt', 5.0), ('tien-debt', 1.3),
                       ('dat-debt', 6.2), ('tien-debt', 8.1),
                       ('quanh-debt', 15.8)]).countByKey()

print(dict(data))