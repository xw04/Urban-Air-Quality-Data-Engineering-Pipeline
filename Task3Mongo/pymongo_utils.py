# Author: Kam Win Ni
from pymongo import MongoClient

CONNECTION_STRING = "mongodb+srv://mongo:mongo12345@cluster0.kx5h57i.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

class PyMongoUtils:
    def __init__(self, uri: str = CONNECTION_STRING):
        self.uri = uri

    def client(self) -> MongoClient:
        return MongoClient(self.uri)

    def get_database(self, database_name: str):
        return self.client()[database_name]

    def get_collection(self, database_name: str, collection_name: str):
        return self.client()[database_name][collection_name]
