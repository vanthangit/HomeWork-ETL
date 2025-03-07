from pyspark import SparkContext, SparkConf
import random
conf = SparkConf() \
    .setAppName("Spark_RDD") \
    .setMaster("local[*]") \
    .set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)


data = ["Dat", "Golden", "Heu kkk", "Sami"]
rdd = sc.parallelize(data)

def numsPartition(iterator):
    #create 1 nums for map Partition data
    rand = random.randint(1, 1000)
    return [f"{item}:{rand}" for item in iterator] #iterator chứa tất cả các phần tử trong một phân vùng

result = rdd.mapPartitions(numsPartition) #mapPartitions áp dụng hàm lên từng phân vùng của rdd

result1 = rdd.mapPartitions(
    lambda item : map(
        lambda l: f"{l} : {random.randint(1, 1000)}",item #item chứa các phần tử của từng phân vùng
    )
)

print(result1.collect())