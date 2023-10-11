import json
from pymongo import MongoClient
import os

def connect_to_mongodb():
    client = MongoClient()
    return client

def close_mongodb_connection(client):
    client.close()

def insert_json_to_collection(collection, json_path):
    with open(json_path, 'r', encoding='utf-8') as json_file:
        json_content = json.load(json_file)
        collection.insert_one(json_content)

def insert_json_to_mongodb(json_dir):
    client = connect_to_mongodb()
    db = client['PharmRG']
    collection = db['wikipathway']

    for root, dirs, files in os.walk(json_dir):
        for json_file in files:
            if json_file.endswith('.json'):
                json_path = os.path.join(root, json_file)
                insert_json_to_collection(collection, json_path)

    close_mongodb_connection(client)





