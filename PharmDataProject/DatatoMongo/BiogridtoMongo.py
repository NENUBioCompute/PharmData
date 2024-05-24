import pymongo
import json  # Make sure to import json for reading data files

class MongoSave:
    def __init__(self, db_name, collection_name, host="localhost", port=27017):
        # 初始化MongoDB客户端
        self.mongo_client = pymongo.MongoClient(host=host, port=port)
        # 选择指定的数据库和集合
        self.db = self.mongo_client[db_name]
        self.collection = self.db[collection_name]

    def mongo(self, file_path):
        # 从文件中读取数据并插入到MongoDB
        with open(file_path, 'r') as file:
            data = json.load(file)  # Assuming the data is in JSON format
            if isinstance(data, list):
                self.collection.insert_many(data)  # 插入多条记录
            else:
                self.collection.insert_one(data)  # 插入单条记录

    def close(self):
        # 关闭MongoDB连接
        self.mongo_client.close()

if __name__ == "__main__":
    # 创建MongoSave实例，指定数据库和集合
    mongo_save = MongoSave('test', 'test_one')
    # 指定要处理的文件路径
    mongo_save.mongo('./Biogrid_data/test.json')
    # 完成操作后关闭数据库连接
    mongo_save.close()
