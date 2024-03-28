"""
  -*- encoding: utf-8 -*-
  @Author: wangyang
  @Time  : 2023/09/24 19:38
  @Email: 2168259496@qq.com
  @function
"""
from ..DataParsers import CsvParser

class GenebankMongo:

    def __init__(self, db_name: str = "PharmGB", csv_path: dict = None, db_url: str = "localhost", port: int = 27017,
                 user_name: str = None, password: str = None):
        self.db_name = db_name
        self.csv_path = csv_path
        self.db_url = db_url
        self.db_name = db_name
        self.port = port
        self.user_name = user_name
        self.password = password
        self.genbank = CsvParser.CsvParser(db_name=self.db_name, csv_path=self.csv_path, db_url=self.db_url,port=self.port,
                          user_name=self.user_name, password=self.password,db_check=True)
    def genebank_mongo(self):
        self.genbank.insert_data("Genebank",buffer_size=100000)
