from database.mongodb_connect import MongoDBConnect
from database.schema_manager import create_mongodb_schema, validate_schema
from config.database_config import get_database_config

def main():
    mongo_config = get_database_config()
    with MongoDBConnect(mongo_config["mongodb"].uri, mongo_config["mongodb"].db_name) as mongo_client:
        create_mongodb_schema(mongo_client.connect())
        mongo_client.db.Users.insert_one({
            "user_id":9614759,
            "login":"GoogleCodeExporter",
            "gravatar_id":"",
            "url":"https://api.github.com/users/GoogleCodeExporter",
            "avatar_url":"https://avatars.githubusercontent.com/u/9614759?"
        })

        validate_schema(mongo_client.connect())
if __name__ == '__main__':
    main()