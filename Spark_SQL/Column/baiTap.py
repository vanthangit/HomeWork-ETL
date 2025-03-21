from pyspark.sql import SparkSession

from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName('Thang') \
    .master('local') \
    .config('spark.executor.memory', '4g') \
    .getOrCreate()

data = [("11/12/2025",),("27/02.2014",),("2023.01.09",),("28-12-2005",)]
df = spark.createDataFrame(data , ["date"])


df = df.withColumn(
    "date_new",
    coalesce(
        to_date(col("date"), "dd/MM/yyyy"),
        to_date(col("date"), "dd/MM.yyyy"),
        to_date(col("date"), "yyyy.MM.dd"),
        to_date(col("date"), "dd-MM-yyyy")
    )
)

df = df.withColumn('day', dayofmonth(col('date_new'))) \
    .withColumn('month', month(col('date_new'))) \
    .withColumn('year', year(col('date_new')))

df.show()
