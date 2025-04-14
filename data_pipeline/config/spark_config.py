from typing import Optional, List, Dict
from pyspark.sql import SparkSession
import os
from config.database_config import get_db_config


def create_spark_session(
    app_name: str,
    master_url: str = "local[*]", #Bắt buộc phải là kiểu str, không được gán None
    executor_memory: Optional[str] = "4g", #Optional[str] thì giá trị có thể là str or None
    executor_cores: Optional[int] = 2,
    driver_memory: Optional[str] = "2g",
    num_executors: Optional[int] = 3,
    jars: Optional[List[str]] = None,
    spark_conf: Optional[Dict[str, str]] = None,
    log_level: str = "WARN"
) -> SparkSession:

    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master_url)

    if executor_memory:
        builder.config("spark.executor.memory", executor_memory)
    if executor_cores:
        builder.config("spark.executor.cores", executor_cores)
    if driver_memory:
        builder.config("spark.driver.memory", driver_memory)
    if num_executors:
        builder.config("spark.executor.instances", num_executors)


    if jars:
        jars_path = ",".join([os.path.abspath(jar) for jar in jars])
        builder.config("spark.jars", jars_path)

    if spark_conf:
        for key, value in spark_conf.items():
            builder.config(key, value)

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel(log_level)

    return spark


# spark = create_spark_session(
#     app_name = "ThangDepZai",
#     master_url = "local[*]",
#     executor_memory = "4g",
#     executor_cores = 2,
#     driver_memory = "2g",
#     num_executors = 3,
#     jars = None,
#     spark_conf = None,
#     log_level = "WARN"
# )


# data = [['quan', 18],['dat', 16],['minh', 20]]
#
#
# df = spark.createDataFrame(data, ['name', 'age'])
# df.show()
# df.printSchema()


def connect_to_mysql(spark : SparkSession, config: dict[str, str], table_name: str):
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://127.0.0.1:3307/github_data") \
        .option("dbtable", table_name) \
        .option("user", config["user"]) \
        .option("password", config["password"]) \
        .option("driver", "com.mysql.jdbc.Driver") \
        .load()

    return df
jar_path = "../lib/mysql-connector-j-9.2.0.jar"
spark = create_spark_session(
    app_name = "ThangDepZai",
    master_url = "local[*]",
    executor_memory = "4g",
    jars = [jar_path],
    log_level = "WARN"
)

db_config = get_db_config()
table_name = "Repositories"

df = connect_to_mysql(spark, db_config, table_name)
df.show()
df.printSchema()