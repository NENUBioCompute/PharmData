"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 4/16/2024 14:12 PM
  @Email: deepwind32@163.com
"""
from PharmDataProject.DataParsers.StitchParser import StitchParser
from PharmDataProject.DataSources.StitchDownloader import StitchDownloader
from PharmDataProject.Utilities.Database.dbutils_v2 import DBConnection
from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


class StitchtoMongo:
    def __init__(self, cfgfile):
        self.config = ConfigParser(cfgfile)
        self.config.set_section("stitch")

        self.db = None
        self.prefix = self.config.get("collection_name_prefix")

    def start(self, collection_name, data):
        self.db = DBConnection(self.config.get("db_name"), self.prefix + collection_name, config=self.config)
        for i in data:
            self.db.insert(i, accelerate=True, buffer_size=100000)
    def save_to_mongo(self):
        for collection_name, data in StitchParser(self.config).start():
            to_mongo.start(collection_name, data)


if __name__ == "__main__":
    cfgfile = "../conf/drugkb_test.config"
    to_mongo = StitchtoMongo(cfgfile)
    to_mongo.save_to_mongo()

