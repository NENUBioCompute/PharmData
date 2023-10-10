from wsgiref import headers

import pymongo
import json

import requests
from Bio.KEGG import REST
from pymongo.collection import Collection
from pymongo.database import Database

from PharmDataProject.DataParsers.KEGGParsers import Parse
from PharmDataProject.DataSources.getId import GetId


class dataSave:
    def save(info,db_name,host,port,collection_name, username, password):
        # username = "user"
        # password = "1234567890"

        client = pymongo.MongoClient(f"mongodb://{username}:{password}@{host}:{port}", connectTimeoutMS=90000)
        db = client[db_name]

        collection = db[collection_name]
        collection.insert_many(info)

if __name__ == "__main__":
    # with open('../../json/H01476.json') as f:
    #     data=json.load(f)
    # dataSave.save(data,"PharmRG","117.73.10.251",27017,"KEGG_Disease","readwrite","readwrite")
    items = GetId.get_id("compound")
    i=415
    for item in items[415:]:
        url='https://rest.kegg.jp/get/'+item
        s = requests.session()
        s.headers = headers
        record = s.get(url)
        # record = REST.kegg_get(item)
        data = record.read()
        entries = data.strip().split("\n///\n")
        #     # 装到一个JSON格式
        json_data = Parse.convert_to_json(entries)

        dataSave.save(json_data, "PharmRG", "59.73.198.168", 27017, "KEGG_Compound", "readwrite", "readwrite")
        i+=1
        print(i)

