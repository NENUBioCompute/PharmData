"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2024/04/01 12:53
  @Email: 2665109868@qq.com
  @function
"""
import configparser
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.FAERSParser import FAERSParser
import pandas as pd
class FAERStoMongo:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('../conf/drugkb.config')
        self.save_path = self.config.get('faers', 'data_path_1')
        self.cfgfile = '../conf/drugkb.config'
        self.config.read(self.cfgfile)

    def save_to_Mongodb(self):
        db = DBconnection(self.cfgfile, self.config.get('faers', 'db_name'),self.config.get('faers', 'col_name_1'))
        faersparser = FAERSParser()
        collection = db.collection
        for merged_data in faersparser.parser_all_data():

            for key, group in merged_data:
                data = {k: '' if pd.isna(v) else v for k, v in group.to_dict(orient='records')[0].items()}
                collection.insert_one(data)

if __name__ == '__main__':
    faerstomongo = FAERStoMongo()
    faerstomongo.save_to_Mongodb()

