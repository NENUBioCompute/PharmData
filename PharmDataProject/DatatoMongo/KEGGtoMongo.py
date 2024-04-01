"""
  -*- encoding: utf-8 -*-
  @Author: evie
  @Time  : ${DATA}
  @Email: 2762376919@qq.com
  @function
"""
import pymongo
import json

import requests
from Bio.KEGG import REST
from pymongo.collection import Collection
from pymongo.database import Database
import configparser
from PharmDataProject.Utilities.Database.dbutils import DBconnection
import os
import json
from pymongo import MongoClient

class KeggtoMongo:
    def __init__(self, folder_path, db):
        self.folder_path = folder_path

        self.db = db
        self.collection = self.db.collection

    def read_json_file(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data

    def insert_into_mongodb(self, data):
        self.collection.insert_one(data)

    def process_json_files(self):
        for root, dirs, files in os.walk(self.folder_path):
            for file_name in files:
                if file_name.endswith('.json'):
                    file_path = os.path.join(root, file_name)
                    json_data = self.read_json_file(file_path)
                    self.insert_into_mongodb(json_data)
                    print(f'Inserted data from {file_path} into MongoDB')


if __name__ == "__main__":


    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)
    for i in range(2, int(config.get('kegg', 'data_path_num'))-2):
        db = DBconnection(cfgfile, config.get('kegg', 'db_name'),
                          config.get('kegg', 'col_name_' + str(i + 1)))
        # database_name = config.get('kegg', 'source_url_' + str(i + 1))
        json_data_path = config.get('kegg','data_path_'+str(i+1))

        json_processor = KeggtoMongo(json_data_path, db)
        json_processor.process_json_files()


