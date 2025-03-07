#import library
from pyspark import SparkContext

#create sparkContext
sc = SparkContext("local", "DE-ETL")

#create Object
data = [
    {"id": 1, "name": "thang"},
    {"id": 2, "name": "nvt"},
    {"id": 3, "name": "vt"}
]

#create rdd
rdd = sc.parallelize(data)
print(rdd.collect()) #print list format
print(f"Number of data: {rdd.count()}")
print(f"First value data: {rdd.first()}")