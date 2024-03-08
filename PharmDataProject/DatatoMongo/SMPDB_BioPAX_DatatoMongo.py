import os
from rdflib import Graph
from rdflib.parser import Parser
from pymongo import MongoClient

# 连接 MongoDB
mongo_host = 'mongodb://readwrite:readwrite@59.73.198.168/?authMechanism=DEFAULT'
mongo_port = 27017
database_name = 'PharmRG'
collection_name = 'SMPDB_BioPAX'

# 连接MongoDB
client = MongoClient(mongo_host, mongo_port)
db = client[database_name]
collection = db[collection_name]

# 目录路径
directory_path = r'C:\Users\win11\PycharmProjects\SMPDB\smpdb_biopax'

# 遍历目录下的所有OWL文件
for filename in os.listdir(directory_path):
    if filename.endswith(".owl"):
        file_path = os.path.join(directory_path, filename)

        # 使用rdflib解析OWL文件
        g = Graph()
        g.parse(file_path, format="xml")

        # 提取三元组并存入MongoDB
        for s, p, o in g:
            triple = {
                'subject': str(s),
                'predicate': str(p),
                'object': str(o)
            }
            collection.insert_one(triple)
            print(f"Triple inserted successfully: {triple}")
# 关闭MongoDB连接
client.close()
