from wsgiref import headers

import pymongo
import json

import requests
from Bio.KEGG import REST
from pymongo.collection import Collection
from pymongo.database import Database

from PharmDataProject.DataParsers.KEGGParsers import Parse
from PharmDataProject.DataSources.KEGGDownloader import GetId


class dataSave:
    def save(info,db_name,host,port,collection_name, username, password):
        # username = "user"
        # password = "1234567890"

        client = pymongo.MongoClient(f"mongodb://{username}:{password}@{host}:{port}", connectTimeoutMS=90000)
        db = client[db_name]

        collection = db[collection_name]
        collection.insert_many(info)

if __name__ == "__main__":
    with open('../../json/H01476.json') as f:
        data=json.load(f)
    dataSave.save(data,"PharmRG","117.73.10.251",27017,"KEGG_Disease","readwrite","readwrite")


