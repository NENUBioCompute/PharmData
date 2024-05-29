"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 4/14/2024 11:18 AM
  @Email: deepwind32@163.com
"""

from tqdm import tqdm
import configparser
from PharmDataProject.DataParsers.SMPDBParser import SMPDBParser
from PharmDataProject.DataSources.SMPDBDownloader import SMPDBDownloader
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


class SMPDBtoMongo:
    def __init__(self, cfg):
        self.config = configparser.ConfigParser()
        self.cfgfile = cfg
        self.config.read(self.cfgfile)
        self.buffer_data_size = int(self.config.get("smpdb", "data_buffer_size"))
        self.buffer_data = []
        self.db = None
        self.saved_data_counter = 0




    def __insert(self):
        self.saved_data_counter += len(self.buffer_data)
        self.db.collection.insert_many(self.buffer_data)
        self.buffer_data = []

    def start(self, dir_name: str, data):
        self.db = DBconnection(cfg, self.config.get("smpdb", "db_name"),
                       self.config.get("smpdb", "collection_name_prefix") + dir_name,
                       )
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
    cfg = "../conf/drugkb_test.config"


    to_mongo = SMPDBtoMongo(cfg)

    for dir_name, data in SMPDBParser(cfg).start():
        to_mongo.start(dir_name, data)
