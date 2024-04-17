"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 4/16/2024 14:12 PM
  @Email: deepwind32@163.com
"""
import json
from pymongo import MongoClient
from tqdm import tqdm

from PharmDataProject.DataParsers.StitchParser import StitchParser
from PharmDataProject.DataSources.StitchDownloader import StitchDownloader
from PharmDataProject.Utilities.Database.dbutils_v2 import DBConnection
from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


class StitchtoMongo:
    def __init__(self, config):
        self.config = config
        self.buffer_data_size = 10000
        self.buffer_data = []
        self.db = None
        self.prefix = config.get("collection_name_prefix")

    def __insert(self):
        self.db.insert(self.buffer_data)
        self.buffer_data = []
    def start(self, collection_name, data):
        print(self.prefix + collection_name)
        self.db = DBConnection(config.get("db_name"), self.prefix + collection_name, config=config)
        self.db.insert(data, accelerate=True, counter=True)
        print("ok")
        # for file_data in tqdm(data, desc=f"{collection_name} data saving"):
        #     for line in file_data:
        #         if len(self.buffer_data) >= self.buffer_data_size:
        #             self.__insert()
        #         self.buffer_data.append(line)
        # if len(self.buffer_data) > 0:
        #     self.__insert()



if __name__ == "__main__":
    cfg = "/home/zhaojingtong/tmpcode/PharmData/PharmDataProject/conf/drugkb.config"
    config = ConfigParser(cfg)
    config.set_section("stitch")

    to_mongo = StitchtoMongo(config)
    # StitchDownloader(config).start()

    for collection_name, data in StitchParser(config).start():
        to_mongo.start(collection_name, data)
