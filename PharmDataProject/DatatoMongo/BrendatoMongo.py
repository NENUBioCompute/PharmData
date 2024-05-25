"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2023/10/22 13:23
  @Email: 2665109868@qq.com
  @function
"""
import configparser
from PharmDataProject.Utilities.Database.dbutils import DBconnection
import json
class BRENDAtoMongo:
    def to_mongo(self):
        config = configparser.ConfigParser()
        cfgfile = '../conf/drugkb.config'
        config.read(cfgfile)
        db = DBconnection('../conf/drugkb.config', config.get('brenda', 'db_name'),config.get('brenda', 'col_name_1'))
        json_file_path = config.get('brenda', 'json_path_1')
        with open(json_file_path, 'r', encoding="utf-8") as jf:
            data = [value for value in json.load(jf).values()]
            db.collection.insert_many(data)

if __name__ == '__main__':
    d = BRENDAtoMongo()
    d.to_mongo()