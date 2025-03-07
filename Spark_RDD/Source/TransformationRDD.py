from pyspark import SparkContext, SparkConf

conf = SparkConf() \
    .setAppName("Spark_RDD") \
    .setMaster("local[*]") \
    .set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

nums = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = sc.parallelize(nums)

#using transformation for create rdd
squaredRdd = rdd.map(lambda x : x * x) #Map chỉ trả về 1 giá trị duy nhất cho mỗi phần tử đầu vào
filterRdd = rdd.filter(lambda x : x > 4) #Hàm trong filter phải là boolean và rdd mới là các phần tử thỏa mãn (True)
flatMapRdd = rdd.flatMap(lambda x : [x, x*2]) #Có thể trả về nhiều kết quả cho mỗi phần tử, và sau đó "làm phẳng" (flatten) các kết quả thành một RDD duy nhất.
print(flatMapRdd.collect())