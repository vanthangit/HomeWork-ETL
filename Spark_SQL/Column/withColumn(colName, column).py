from pyspark.sql import SparkSession

from pyspark.sql.types import *
from pyspark.sql.types import BooleanType, StructType

from pyspark.sql.functions import Column, col, upper, length, lit, struct, udf

spark = SparkSession.builder \
    .appName('Thang') \
    .master('local') \
    .config('spark.executor.memory', '4g') \
    .getOrCreate()

schema = StructType([
    StructField('id', StringType(), False),
    StructField('type', StringType(), True),
    StructField('actor', StructType([
        StructField('id', LongType(), False),
        StructField('login', StringType(), True),
        StructField('gravatar_id', StringType(), True),
        StructField('url', StringType(), True),
        StructField('avatar_url', StringType(), True)
    ]), True),
    StructField('repo', StructType([
        StructField('id', LongType(), False),
        StructField('name', StringType(), True),
        StructField('url', StringType(), True)
    ]), True),
    StructField('payload', StructType([
        StructField('action', StringType(), True),
        StructField('issue', StructType([
            StructField('url', StringType(), True),
            StructField('labels_url', StringType(), True),
            StructField('comments_url', StringType(), True),
            StructField('events_url', StringType(), True),
            StructField('html_url', StringType(), True),
            StructField('id', LongType(), False),
            StructField('number', LongType(), True),
            StructField('title', StringType(), True),
            StructField('user', StructType([
                StructField('login', StringType(), True),
                StructField('id', LongType(), False),
                StructField('avatar_url', StringType(), True),
                StructField('gravatar_id', StringType(), True),
                StructField('url', StringType(), True),
                StructField('html_url', StringType(), True),
                StructField('followers_url', StringType(), True),
                StructField('following_url', StringType(), True),
                StructField('gists_url', StringType(), True),
                StructField('starred_url', StringType(), True),
                StructField('subscriptions_url', StringType(), True),
                StructField('organizations_url', StringType(), True),
                StructField('repos_url', StringType(), True),
                StructField('events_url', StringType(), True),
                StructField('received_events_url', StringType(), True),
                StructField('type', StringType(), True),
                StructField('site_admin', BooleanType(), True)
            ]), True),
            StructField('labels', ArrayType(StructType([
                StructField('url', StringType(), True),
                StructField('name', StringType(), True),
                StructField('color', StringType(), True)
            ])), True),
            StructField('state', StringType(), True),
            StructField('locked', BooleanType(), True),
            StructField('assignee', StringType(), True),
            StructField('milestone', StringType(), True),
            StructField('comments', IntegerType(), True),
            StructField('created_at', StringType(), True),
            StructField('updated_at', StringType(), True),
            StructField('closed_at', StringType(), True),
            StructField('body', BooleanType(), True)
        ]), True),
    ]), True),
    StructField('public', BooleanType(), True),
    StructField('created_at', StringType(), True),
])

dataFile = spark.read.schema(schema).json(r"D:\ETL\Practice\HomeWork-ETL\Spark_SQL\data\2015-03-01-17.json")

'''
withColumn
    - colName: name of column
    - column: value of column
Features:
    - Them 1 cot moi neu colName chua ton tai
    - Ghi de len cot hien tai neu colName ton tai
    - col(), lit(), struct(): de xac dinh gia tri cua cot
        + struct(): Dùng để tạo một struct mới.
        + lit(): Dùng để tạo một giá trị hằng số (constant value).
        + col(): Dùng để truy xuất các cột trong DataFrame.
'''


# dataFile.withColumn('id2', lit('van_thang')).select(col('actor.id'), col('id2')).show()

#dataFile.withColumn('actor.id2', lit('van_thang')).select(col('actor.id'), col('actor.id2')).show()
#withColumn() chỉ hoạt động trên các cột cấp cao nhất (top-level columns), nên muốn sửa actor, ta phải thay cả struct actor.

dataFileStruct = dataFile.withColumn(
    "actor",
    struct(
        col("actor.*"),
        lit("van_thang").alias("id2")
    )
)

# dataFileStruct.select(col('actor.id'), col('actor.id2')) \
#     .orderBy([col('id'), col('actor.id2')], ascending=[True, False])


#Tạo hàm generate dãy số tăng dần
counter = 0
def generate(id):
    global counter
    counter += 1
    return counter

generate_udf = udf(generate, LongType())

df = dataFile.withColumn('generate', generate_udf(col('id'))).select('id', 'generate')

df.orderBy([col('id'), col('generate')], ascending=[True, False]).show()