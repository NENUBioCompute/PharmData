"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 4/14/2024 11:18 AM
  @Email: deepwind32@163.com
"""

from tqdm import tqdm

from PharmDataProject.DataParsers.SMPDBParser import SMPDBParser
from PharmDataProject.DataSources.SMPDBDownloader import SMPDBDownloader
from PharmDataProject.Utilities.Database.dbutils_v2 import DBConnection
from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


class SMPDBtoMongo:
    def __init__(self, config):
        self.config = config
        self.buffer_data_size = int(config.get("smpdb", "data_buffer_size"))
        self.buffer_data = []
        self.db = None
        self.saved_data_counter = 0

    def __insert(self):
        self.saved_data_counter += len(self.buffer_data)
        self.db.insert(self.buffer_data)
        self.buffer_data = []

    def start(self, dir_name: str, data):
        self.db = DBConnection(config.get("smpdb", "db_name"), config.get("smpdb", "collection_name_prefix") + dir_name,
                               config=config)
        if dir_name == 'smpdb_sbml':
            for model in tqdm(data, desc=f"{dir_name} data saving"):
                if len(self.buffer_data) >= self.buffer_data_size:
                    self.__insert()
                self.buffer_data.append(model)
        else:
            for file_data in tqdm(data, desc=f"{dir_name} data saving"):
                for line in file_data:
                    if len(self.buffer_data) >= self.buffer_data_size:
                        self.__insert()
                    self.buffer_data.append(line)

        # insert the left data
        if len(self.buffer_data) > 0:
            self.__insert()


if __name__ == "__main__":
    cfg = "/home/zhaojingtong/tmpcode/PharmData/PharmDataProject/conf/drugkb.config"
    config = ConfigParser.GetConfig(cfg)

    to_mongo = SMPDBtoMongo(config)
    SMPDBDownloader(config).start()

    for dir_name, data in SMPDBParser(config).start():
        to_mongo.start(dir_name, data)
