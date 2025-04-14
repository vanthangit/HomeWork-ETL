from wsgiref.validate import validator

def create_mongodb_schema(db):
    db.drop_collection("Users")
    db.create_collection('Users', validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["user_id", "login", "gravatar_id", "avatar_url", "url"],
            "properties": {
                "user_id": {
                    "bsonType": "int",
                },
                "login": {
                    "bsonType": "string",
                },
                "gravatar_id": {
                    "bsonType": ["string", "null"],
                },
                "url": {
                    "bsonType": ["string", "null"],
                },
                "avatar_url": {
                    "bsonType": ["string", "null"],
                }
            }
        }
    })
    db.Users.create_index('user_id', unique=True)

def validate_schema(db):
    collections = db.list_collection_names()
    # print(collections)
    if "Users" not in collections:
        raise Exception("----------Users collection not found----------")

    user = db.Users.find_one({"user_id": 9614759})
    if not user:
        raise Exception("----------User not found----------")