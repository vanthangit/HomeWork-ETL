import mysql.connector
from mysql.connector import Error
from config.mysql_config import get_db_config
from pathlib import Path

DATABASE_NAME = "github_data"
SQL_FILE_PATH = Path('D:\ETL\Practice\HomeWork-ETL\data_pipeline\src\schema.sql')

def connect_to_mysql(config):
    try:
        conn = mysql.connector.connect(**config)
        return conn
    except Error as e:
        raise Exception(f"----------Can not connect to MySQL database: {e}----------")

def create_database(cur, db_name):
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    print(f"----------Database {db_name} created----------")

def execute_sql_file(cur, filepath):
    with filepath.open('r') as file:
        sql_script = file.read()
    commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]
    for command in commands:
        try:
            cur.execute(command)
            print(f"----------Command {command.strip()[::50]} executed----------")
        except Error as e:
            print(f"----------Error while executing command: {e}----------")

def main():
    try:
        #get db config
        config_db = get_db_config()

        #remove db name from config
        initial_config = {k: v for k, v in config_db.items() if k != "database"}

        connection = connect_to_mysql(initial_config)
        cursor = connection.cursor()

        #create db
        create_database(cursor, DATABASE_NAME)
        connection.database = DATABASE_NAME

        execute_sql_file(cursor, SQL_FILE_PATH)
        connection.commit()
        print(f"----------Successfully executed SQL file----------")
    except Error as e:
        print(f"----------Error: {e}----------")
        if connection and connection.is_connected():
            connection.rollback()

if __name__ == "__main__":
    main()