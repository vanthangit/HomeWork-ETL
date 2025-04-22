from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, LongType, StringType

from spark_write_data import SparkWriteDatabases
from config.database_config import get_database_config
from config.spark_config import SparkConfig
def main():
    db_config = get_database_config()
    jars = [
        "../../lib/mysql-connector-j-9.2.0.jar",
    ]

    spark_conf = {
        "spark.jar.package": (
            "mysql:mysql-connector-java:9.2.0"
        )
    }

    spark_write_database = SparkConfig(
        app_name="ThangDepZai",
        master_url = "local[*]",
        executor_memory = "4g",
        executor_cores = 2,
        driver_memory = "2g",
        num_executors = 3,
        jars = None,
        spark_conf = None,
        log_level = "WARN"
    ).spark


    schema = StructType([
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
        ]), True)
    ])

    df = spark_write_database.read.schema(schema).json("../../data/2015-03-01-17.json")

    df_write_table_Users = df.select(
        col("actor.id").alias("actor_id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.url").alias("url"),
        col("actor.avatar_url").alias("avatar_url")
    )

    df_wrire_table_Repositories = df.select(
        col("repo.id").alias("repo_id"),
        col("repo.name").alias("name"),
        col("repo.url").alias("url"),
    )

    df_write_table_Users = SparkWriteDatabases(spark_write_database, db_config)
    df_write_table_Users.spark_write_mysql(df_write_table_Users, "Users", mode="append")

if __name__ == "__main__":
    main()