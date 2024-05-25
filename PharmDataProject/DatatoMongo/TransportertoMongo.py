# @Author：huqianxiang
# @Time：2023/10/9
# @FileName：Transporter.py
# @Software:PyCharm

import requests
from bs4 import BeautifulSoup
#from urllib.parse import urlparse, urljoin
import json
from pymongo import MongoClient

class TransportertoMongo:
    def __init__(self):
        pass

    def save_data(self, url, data):
        filename = url.split("/")[-2] + ".json"
        with open(filename, "w") as json_file:
            json.dump(data, json_file, indent=4)

        client = MongoClient('mongodb://readwrite:readwrite@59.73.198.168/?authMechanism=DEFAULT')
        db = client['PharmRG']
        collection = db['transporter']
        with open(filename, "r") as json_file:
            json_data = json.load(json_file)
        collection.insert_one({'url': url, 'data': json_data})
        client.close()

        print(f"Data scraped and saved for {url}")