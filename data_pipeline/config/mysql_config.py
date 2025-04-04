from dotenv import load_dotenv
import os
from urllib.parse import urlparse

def get_db_config():
    load_dotenv()

    jdbc_url = os.getenv("DB_URL")

    parse_url = urlparse(jdbc_url.replace("jdbc:", "", 1))

    host = parse_url.hostname
    port = parse_url.port
    database_name = parse_url.path[1:]
    # print(host, port, database_name)

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    return{
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database_name
    }
