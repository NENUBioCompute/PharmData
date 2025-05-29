from pymongo import MongoClient

class MongoSaver:
    def __init__(self):
        self.client = MongoClient("59.73.198.168", 27017, username="readwrite", password="readwrite")
        self.db = self.client["PharmRG"]
        self.collection = self.db["source_drugs"]

    def save_to_mongo(self, data):
        if data:
            self.collection.insert_one(data)
            print("Data saved to MongoDB:", data)

    def close(self):
        self.client.close()

if __name__ == '__main__':
    saver = MongoSaver()
    test_data = {'drug_name': 'ExampleDrug'}
    saver.save_to_mongo(test_data)
    saver.close()