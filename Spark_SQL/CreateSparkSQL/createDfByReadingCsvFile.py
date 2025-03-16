from datetime import datetime

from pyspark.sql import SparkSession

from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName('CreateDfFromRdd') \
    .master('local') \
    .config('spark.executor.memory', '4g') \
    .getOrCreate()

csvFile = spark.read.option('header', 'true').option('inferSchema', 'true').csv(r"D:\ETL\Practice\data\vnm_men_2020.csv")
csvFile.show()