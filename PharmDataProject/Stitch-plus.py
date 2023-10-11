import requests
import gzip
from pymongo import MongoClient
from threading import Thread

def download_and_extract_gz(url, filename):
    response = requests.get(url, stream=True)
    with open(filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

def insert_data_to_mongodb(database, collection, data):
    # 连接 MongoDB 数据库
    client = MongoClient('mongodb://readwrite:readwrite@59.73.198.168:27017')
    db = client[database]
    col = db[collection]
    col.insert_many(data)
    client.close()

class DataProcessor:
    def __init__(self, url, filename, database, collection):
        self.url = url
        self.filename = filename
        self.database = database
        self.collection = collection

    def download_and_process_data(self):
        download_thread = Thread(target=self.download_file)
        download_thread.start()
        download_thread.join()
        data = self.extract_data_from_file()
        self.insert_data_to_mongodb(data)

    def download_file(self):
        download_and_extract_gz(self.url, self.filename)

    def extract_data_from_file(self):
        data = []
        with gzip.open(self.filename, 'rt') as f:
            for line in f:
                # 处理文件的每一行并提取数据
                # 在这里，您可以定义自己的逻辑来提取所需的字段
                # 并创建一个表示数据的字典或对象
                # 例如：
                fields = line.strip().split('\t')
                record = {
                    'field1': fields[0],
                    'field2': fields[1],
                    # 根据需要添加更多字段
                }
                data.append(record)
        return data

    def insert_data_to_mongodb(self, data):
        insert_data_to_mongodb(self.database, self.collection, data)

if __name__ == '__main__':
    url = "http://stitch.embl.de/download/protein_chemical.links.v5.0.tsv.gz"
    filename = "protein_chemical.links.v5.0.tsv.gz"
    database = "PharmRG"
    collection = "stitch-protein-chemical"

    processor = DataProcessor(url, filename, database, collection)
    processor.download_and_process_data()