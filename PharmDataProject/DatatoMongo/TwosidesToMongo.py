"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2024/05/29 20:13
  @Email: 2665109868@qq.com
  @function
"""
import logging
import configparser
from tqdm import tqdm
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.TwosidesParser import TwosidesParser


class TwosidesToMongo:
    def __init__(self, config_path='../conf/drugkb_test.config'):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

    def run(self):
        data_path_num = int(self.config.get('twosides', 'data_path_num'))
        print(f"Number of data paths: {data_path_num}")
        data_name = ['TWOSIDES.csv','OFFSIDES.csv']
        for i in range(data_path_num):
            db_name = self.config.get('twosides', 'db_name')
            col_name = self.config.get('twosides', f'col_name_{i + 1}')
            data_path = self.config.get('twosides', f'data_path_{i + 1}')+data_name[i+1]

            print(f"Processing file {i + 1}/{data_path_num}: {data_path}")
            print(f"Database: {db_name}, Collection: {col_name}")

            db = DBconnection(self.config_path, db_name, col_name)
            parser = TwosidesParser(data_path)

            print(f"Parsing and inserting file: {data_path}")

            for index, item in enumerate(tqdm(parser.parse(), desc="Inserting entries")):
                db.collection.insert_one(item)
                if index % 10000 == 0 and index != 0:
                    print(f"{index} entries inserted.")


        print("All entries have been inserted successfully.")


if __name__ == "__main__":
    twosides_to_mongo = TwosidesToMongo()
    twosides_to_mongo.run()