"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2024/04/05 21:32
  @Email: 2665109868@qq.com
  @function
"""
import json
from pymongo import MongoClient

class JsonToMongoDB:
    def __init__(self, json_file, mongodb_uri, db_name, collection_name):
        self.json_file = json_file
        self.mongodb_uri = mongodb_uri
        self.db_name = db_name
        self.collection_name = collection_name

    def import_to_mongodb(self):
        # 连接 MongoDB
        client = MongoClient(self.mongodb_uri)
        db = client[self.db_name]
        collection = db[self.collection_name]

        # 读取 JSON 文件数据
        with open(self.json_file, 'r') as f:
            data = json.load(f)

        # 插入数据到集合中
        result = collection.insert_many(data)
        print(f"成功插入 {len(result.inserted_ids)} 条数据到 {self.collection_name} 集合")

# 创建类实例并调用方法
json_file = '9606.protein_chemical.links.detailed.v5.0.json'
mongodb_uri = 'mongodb://localhost:27017/'
db_name = 'mydatabase'
collection_name = 'mycollection'

json_to_mongodb = JsonToMongoDB(json_file, mongodb_uri, db_name, collection_name)
json_to_mongodb.import_to_mongodb()