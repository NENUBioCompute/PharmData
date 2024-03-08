import pandas as pd
from pymongo import MongoClient
import zipfile
import os

# MongoDB连接信息
mongo_host = 'mongodb://readwrite:readwrite@59.73.198.168/?authMechanism=DEFAULT'
mongo_port = 27017
database_name = 'PharmRG'
collection_name = 'SMPDB_pathways'

# 压缩文件路径
zip_file_path = 'C:/Users/win11/PycharmProjects/SMPDB/SMPDBdownload/smpdb_pathways.csv.zip'
csv_file_name = 'smpdb_pathways.csv'

# 解压缩文件
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(os.path.dirname(zip_file_path))

# 读取CSV文件
csv_file_path = os.path.join(os.path.dirname(zip_file_path), csv_file_name)
df = pd.read_csv(csv_file_path)

# 连接MongoDB
client = MongoClient(mongo_host, mongo_port)
db = client[database_name]
collection = db[collection_name]

# 将数据插入MongoDB
data_dict = df.to_dict(orient='records')  # 将DataFrame转换为字典列表
collection.insert_many(data_dict)

# 删除解压缩后的CSV文件
os.remove(csv_file_path)

# 关闭MongoDB连接
client.close()

print(f"数据成功插入到MongoDB的 {database_name}.{collection_name} 集合中。")
