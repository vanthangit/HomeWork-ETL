from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('CreateDfFromRdd') \
    .master('local') \
    .config('spark.executor.memory', '4g') \
    .getOrCreate()

textFile = spark.read.text("D:\ETL\Practice\HomeWork-ETL\Spark_RDD\Data\data.txt")
textFile.show(5, truncate=False)