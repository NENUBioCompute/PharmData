import os
import pandas as pd
from pymongo import MongoClient

# MongoDB 连接信息
mongo_host = 'mongodb://readwrite:readwrite@59.73.198.168/?authMechanism=DEFAULT'
mongo_port = 27017
database_name = 'PharmRG'
collection_name = 'SMPDB_proteins'

# CSV 文件目录
csv_directory = 'C:/Users/win11/PycharmProjects/SMPDB/SMPDB_proteins'  # 替换为你的 CSV 文件所在的目录

# 连接 MongoDB
client = MongoClient(mongo_host, mongo_port)
db = client[database_name]
collection = db[collection_name]

# 遍历 CSV 文件并将数据存储到 MongoDB 中
for filename in os.listdir(csv_directory):
    if filename.endswith(".csv"):
        # 读取 CSV 文件数据
        file_path = os.path.join(csv_directory, filename)
        df = pd.read_csv(file_path)

        # 替换 NaN 值为空字符串
        df = df.fillna('')

        # 转换为字典列表
        data_dict = df.to_dict(orient='records')

        # 插入数据到 MongoDB 集合
        if data_dict:
            collection.insert_many(data_dict)
            print(f"成功插入 {filename} 的数据。")
        else:
            print(f"{filename} 的数据为空，跳过插入。")

print(f"所有数据成功插入到 MongoDB 的 {database_name}.{collection_name} 集合中。")
# 关闭 MongoDB 连接
client.close()
