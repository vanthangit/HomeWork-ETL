from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("ReduceByKey") \
    .setMaster('local[*]') \
    .set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

data = sc.parallelize([('dat-debt', 5.0), ('tien-debt', 1.3),
                       ('dat-debt', 6.2), ('tien-debt', 8.1),
                       ('quanh-debt', 15.8)])

bill = data.reduceByKey(lambda x, y: x + y) #Nó sẽ nhóm các phần tử cùng key lại với nhau và tính toán value theo hàm lambda
print(bill.collect())