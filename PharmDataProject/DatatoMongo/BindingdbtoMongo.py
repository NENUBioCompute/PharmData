import os
import pandas as pd
from pymongo import MongoClient
from rdflib import Graph
import zipfile
import xml.etree.ElementTree as ET
import libsbml
import math

# MongoDB 连接信息
mongo_host = 'mongodb://readwrite:readwrite@59.73.198.168/?authMechanism=DEFAULT'
mongo_port = 27017
database_name = 'PharmRG'

# 初始化 MongoDB 客户端
client = MongoClient(mongo_host, mongo_port)
db = client[database_name]

def parse_biopax_and_store_to_mongo(directory_path, collection_name='SMPDB_BioPAX'):
    """
    解析BioPAX格式文件并存储到MongoDB。此处集成了BioPAX文件的自定义解析逻辑。
    """
    collection = db[collection_name]
    for filename in os.listdir(directory_path):
        if filename.endswith(".owl"):
            file_path = os.path.join(directory_path, filename)
            with open(file_path, 'r', encoding='utf-8') as file:
                file_content = file.read()

            # 可能的文本处理逻辑
            updated_content = re.sub(r'rdf:ID="([^"]*)"', lambda m: m.group(0).replace('/', '_').replace('-', '_').replace(',', '_'), file_content)

            # 使用rdflib处理更新后的内容
            g = Graph()
            g.parse(data=updated_content, format="xml")
            for s, p, o in g:
                triple = {'subject': str(s), 'predicate': str(p), 'object': str(o)}
                collection.insert_one(triple)
def parse_csv_and_store_to_mongo(zip_file_path, collection_name, csv_file_name=None):
    """
    解压缩CSV文件并存储到MongoDB。
    """
    if not csv_file_name:
        csv_file_name = zip_file_path.split('/')[-1].replace('.zip', '.csv')
    collection = db[collection_name]
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(os.path.dirname(zip_file_path))
    csv_file_path = os.path.join(os.path.dirname(zip_file_path), csv_file_name)
    df = pd.read_csv(csv_file_path)
    data_dict = df.to_dict(orient='records')
    collection.insert_many(data_dict)
    os.remove(csv_file_path)

def parse_xml_and_store_to_mongo(directory_path, collection_name, file_extension):
    """
    解析XML或SBGN文件并存储到MongoDB。
    """
    collection = db[collection_name]
    for filename in os.listdir(directory_path):
        if filename.endswith(file_extension):
            file_path = os.path.join(directory_path, filename)
            if file_extension == ".sbgn":
                tree = ET.parse(file_path)
                root = tree.getroot()
                sbgn_data = {"file_path": file_path, "sbgn_elements": []}
                for element in root.iter():
                    element_data = {"tag": element.tag, "attributes": element.attrib}
                    sbgn_data["sbgn_elements"].append(element_data)
                collection.insert_one(sbgn_data)

def parse_sbml_and_store_to_mongo(directory_path, collection_name='SMPDB_SBML'):
    """
    解析SBML文件并存储到MongoDB。
    """
    collection = db[collection_name]
    for filename in os.listdir(directory_path):
        if filename.endswith(".sbml"):
            sbml_file_path = os.path.join(directory_path, filename)
            reader = libsbml.SBMLReader()
            document = reader.readSBMLFromFile(sbml_file_path)
            if document.getNumErrors() > 0:
                continue
            model = document.getModel()
            if model:
                model_info = {...} # 根据需求处理并收集模型信息
                collection.insert_one(model_info)
