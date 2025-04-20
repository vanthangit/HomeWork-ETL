from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

#step 1: def (get mongo config)
#step 2: def (connect)
#step 3: def (disconnect)
#step 4: def (reconnect)
#step 5: def (exit)

class MongoDBConnect:
#step 1
    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = MongoClient(self.mongo_uri)
            self.client.server_info() #test connection
            self.db = self.client[self.db_name]
            #print(self.client.server_info())
            print(f"----------Connected to {self.db_name}-----------")
            return self.db
        except ConnectionFailure as e:
            raise Exception(f"----------Failed to connect to MogoDB: {e}-----------") from e

    def close(self):
        if self.client:
            self.client.close()
        print(f"----------Closed {self.db_name}-----------")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

