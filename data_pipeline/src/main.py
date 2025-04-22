from database.mongodb_connect import MongoDBConnect
from database.mysql_connect import MySQLConnect
from database.redis_connect import RedisConnect
from database.schema_manager import create_mongodb_schema, validate_mongo_schema
from database.schema_manager import create_mysql_schema, validate_mysql_schema
from database.schema_manager import create_redis_schema, validate_redis_schema
from config.database_config import get_database_config


def main(config):
    #MongoDB
    # with MongoDBConnect(config["mongodb"].uri, config["mongodb"].db_name) as mongo_client:
    #     create_mongodb_schema(mongo_client.connect())
    #     mongo_client.db.Users.insert_one({
    #         "user_id":9614759,
    #         "login":"GoogleCodeExporter",
    #         "gravatar_id":"",
    #         "url":"https://api.github.com/users/GoogleCodeExporter",
    #         "avatar_url":"https://avatars.githubusercontent.com/u/9614759?"
    #     })
    #
    #     validate_mongo_schema(mongo_client.connect())

    #MySQL
    # with MySQLConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user, config["mysql"].password) as mysql_client:
    #     connection, cursor = mysql_client.connection, mysql_client.cursor
    #     create_mysql_schema(connection, cursor)
    #
    #     cursor.execute("INSERT INTO Users (user_id, login, gravatar_id, url, avatar_url) VALUES (%s, %s, %s, %s, %s)",
    #                    (9614759, "GoogleCodeExporter", "", "https://api.github.com/users/GoogleCodeExporter", "https://avatars.githubusercontent.com/u/9614759?"))
    #     connection.commit()
    #     print(f"-----------Inserted to table Users-----------")
    #
    #     validate_mysql_schema(cursor)


    #Redis
    with RedisConnect(config["redis"].host, config["redis"].port, config["redis"].user, config["redis"].password, config["redis"].db_name) as redis_client:
        create_redis_schema(redis_client.connect())
        validate_redis_schema(redis_client.connect())
if __name__ == '__main__':
    config = get_database_config()
    main(config)