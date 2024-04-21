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
    def __init__(self, config):
        self.config = config
        self.db = None
        self.prefix = config.get("collection_name_prefix")

    def start(self, collection_name, data):
        self.db = DBConnection(config.get("db_name"), self.prefix + collection_name, config=config)
        for i in data:
            self.db.insert(i, accelerate=True, buffer_size=100000)


if __name__ == "__main__":
    cfg = "/home/zhaojingtong/tmpcode/PharmData/PharmDataProject/conf/drugkb.config"
    config = ConfigParser(cfg)
    config.set_section("stitch")

    to_mongo = StitchtoMongo(config)
    StitchDownloader(config).start()

    for collection_name, data in StitchParser(config).start():
        to_mongo.start(collection_name, data)
