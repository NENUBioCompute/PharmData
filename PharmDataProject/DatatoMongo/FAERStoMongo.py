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
        self.cfgfile = '../conf/drugkb_test.config'
        self.config.read(self.cfgfile)
        self.save_path = self.config.get('faers', 'data_path_1')

        self.config.read(self.cfgfile)

    def save_to_Mongodb(self):
        db = DBconnection(self.cfgfile, self.config.get('faers', 'db_name'),self.config.get('faers', 'col_name_1'))
        faersparser = FAERSParser()
        collection = db.collection
        for merged_data in faersparser.parser_all_data():

            data_to_insert = []
            batch_size = 10000

            for key, group in merged_data:
                data = {k: '' if pd.isna(v) else v for k, v in group.to_dict(orient='records')[0].items()}
                data_to_insert.append(data)

                if len(data_to_insert) >= batch_size:
                    collection.insert_many(data_to_insert)
                    data_to_insert = []

            # Insert remaining data if any
            if data_to_insert:
                collection.insert_many(data_to_insert)
if __name__ == '__main__':
    faerstomongo = FAERStoMongo()
    faerstomongo.save_to_Mongodb()

