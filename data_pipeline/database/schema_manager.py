from pathlib import Path
from wsgiref.validate import validator

from mysql.connector import Error


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

def validate_mongo_schema(db):
    collections = db.list_collection_names()
    # print(collections)
    if "Users" not in collections:
        raise Exception("----------Users collection not found----------")

    user = db.Users.find_one({"user_id": 9614759})
    if not user:
        raise Exception("----------User not found----------")

SQL_FILE_PATH = Path('D:\ETL\Practice\HomeWork-ETL\data_pipeline\sql\schema.sql')
DATABASE_NAME = "github_data"


def create_mysql_schema(connection, cursor):
    cursor.execute(f'DROP DATABASE IF EXISTS {DATABASE_NAME}')
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
    print(f"----------Database {DATABASE_NAME} created----------")
    connection.database = DATABASE_NAME
    try:
        with SQL_FILE_PATH.open('r') as sql_file:
            sql_script = sql_file.read()
            commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]
            for command in commands:
                try:
                    cursor.execute(command)
                    print(f"----------Command executed----------")
                except Error as e:
                    print(f"----------Error while executing command: {e}----------")
                connection.commit()
                print(f"----------Created MySQL schema----------")
    except Error as e:
        raise Exception(f"----------Error while executing command: {e}----------")

def validate_mysql_schema(cursor):
    cursor.execute('SHOW TABLES')
    # print(cursor.fetchall())
    tables = [row[0] for row in cursor.fetchall()]
    if "Users" not in tables or "Repositories" not in tables:
        raise Exception("----------Table not found----------")
    cursor.execute('SELECT * FROM Users WHERE user_id = 9614759')
    # print(cursor.fetchone())
    user = cursor.fetchone()
    if not user:
        raise Exception("----------User not found----------")


    print("----------Table validated----------")