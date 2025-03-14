from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Join") \
    .setMaster('local') \
    .set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

data1 = sc.parallelize([(110, 50.12), (127, 90.5), (126, 211.0), (105, 6.0), (165, 31.0), (110, 40.11)])

data2 = sc.parallelize([(110, 'dat'), (127, 'golden'), (126, 'heu kkk'), (105, 'phuc bo'), (165, 'dung ct')])


joinData = data1.join(data2).sortByKey()

for result in joinData.collect():
    print(result)