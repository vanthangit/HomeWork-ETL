import mysql.connector
from mysql.connector import Error


class MySQLConnect:
    def __init__(self, host, port, user, password):
        self.config = {"host" : host, "port": port, "user": user, "password": password}
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor()
            print(f"----------Connected to MySQL database----------")
            return self.connection, self.cursor
        except Error as e:
            raise Exception(f"----------Failed to connect to MySQL database: {e}----------")


    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
        print(f"----------MySQL connection is closed-----------")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()