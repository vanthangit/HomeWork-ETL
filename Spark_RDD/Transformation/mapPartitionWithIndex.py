from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("mapPartitionWithIndex") \
    .setMaster('local[*]') \
    .set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)
#1, 2, 3, 4, 5 : partition1
#6, 7, 8, 9, 10 : partition2
'''
- idx : chi so cua partition
- itr :  vong lap qua tat ca phan tu trong phan vung
'''
result = rdd.mapPartitionsWithIndex(lambda idx, itr :  [(n,idx) for n in itr])

result1 = rdd.mapPartitionsWithIndex(lambda idx, itr : (idx if idx == 0 else n for n in itr))

print(result1.collect())