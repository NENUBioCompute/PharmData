import xml.etree.ElementTree as ET
from pymongo import MongoClient
import os

mongo_host = 'mongodb://readwrite:readwrite@59.73.198.168/?authMechanism=DEFAULT'
mongo_port = 27017
database_name = 'PharmRG'
collection_name = 'SMPDB_SBGN'
# 连接 MongoDB
client = MongoClient(mongo_host, mongo_port)
db = client[database_name]
collection = db[collection_name]

def parse_sbgn_and_store(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    # 创建存储 SBGN 数据的字典
    sbgn_data = {
        "file_path": file_path,
        "sbgn_elements": []
    }

    # 遍历 SBGN 文件，提取信息并添加到字典中
    for element in root.iter():
        element_data = {
            "tag": element.tag,
            "attributes": element.attrib
        }
        sbgn_data["sbgn_elements"].append(element_data)

    # 将数据存入 MongoDB
    collection.insert_one(sbgn_data)
    print(f"SBGN data from {file_path} successfully stored in MongoDB.")

def parse_and_store_sbgn_files(directory_path):
    # 遍历目录下的所有文件
    for filename in os.listdir(directory_path):
        if filename.endswith(".sbgn"):
            file_path = os.path.join(directory_path, filename)
            parse_sbgn_and_store(file_path)

# 指定文件夹路径并调用解析函数
directory_path = r"C:\Users\win11\PycharmProjects\SMPDB\smpdb_sbgn"
parse_and_store_sbgn_files(directory_path)
