from datetime import datetime

from pyspark.sql import SparkSession

from pyspark.sql.types import *
from pyspark.sql.types import BooleanType, StructType

spark = SparkSession.builder \
    .appName('CreateDfFromRdd') \
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
            StructField('assignee', StructType(), True),
            StructField('milestone', StructType(), True),
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

jsonFile = spark.read.schema(schema).json(r"D:\ETL\Practice\HomeWork-ETL\Spark_SQL\data\2015-03-01-17.json")
jsonFile.show(truncate=False)