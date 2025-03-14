from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("SortByKey") \
    .setMaster('local[*]') \
    .set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

data = sc.parallelize([('dat-debt', 5.0), ('tien-debt', 1.3),
                       ('dat-debt', 6.2), ('tien-debt', 8.1),
                       ('quanh-debt', 15.8)])

bill = data.reduceByKey(lambda x, y: x + y)

sortBill = bill.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
print(sortBill.collect())