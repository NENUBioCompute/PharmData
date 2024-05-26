"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2024/05/26 17:54
  @Email: 2665109868@qq.com
  @function
"""
import configparser
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.EnsemblParser import EnsemblParser
import pandas as pd

class EnsembltoMongo:
    def __init__(self):
        self.config = configparser.ConfigParser()

        self.cfgfile = '../conf/drugkb.config'
        self.config.read(self.cfgfile)

    def save_to_Mongodb(self):
        db = DBconnection(self.cfgfile, self.config.get('ensembl', 'db_name'),self.config.get('ensembl', 'col_name_1'))
        faersparser = EnsemblParser().parse_ensembl()
        collection = db.collection
        data_to_insert = []
        for merged_data in faersparser:

            batch_size = 10000

            data_to_insert.append(merged_data)

            if len(data_to_insert) >= batch_size:
                collection.insert_many(data_to_insert)
                data_to_insert = []
            # Insert remaining data if any
        if data_to_insert:
            collection.insert_many(data_to_insert)

if __name__ == '__main__':
    ensembltomongo = EnsembltoMongo()
    ensembltomongo.save_to_Mongodb()