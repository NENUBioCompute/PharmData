
from PharmDataProject.DataParsers.DGIDBParser import DGIDBParser
import os
import sys
import json


class DGItoMongo:
    def __init__(self):
        pass

    def to_mongo(self, db):
        json_file_path = DGIDBParser.mkpath(DGIDBParser.json_path) + DGIDBParser.json_path.split('/')[-2] + '.json'

        if not os.path.exists(json_file_path):
            print("dgidb file not find !")
            sys.exit()
        else:
            with open(json_file_path, 'r', encoding="utf-8") as jf:
                data = [value for value in json.load(jf).values()]
                db.collection.insert_many(data)
