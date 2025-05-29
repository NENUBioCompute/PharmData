"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 5/6/2025 10:49 PM
  @Email: deepwind32@163.com
"""
import logging

from tqdm import tqdm

from PharmDataProject.DataParsers.ChemblParser import ChemblParser
from PharmDataProject.Utilities.Database.dbutils_v2 import DBConnection
from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


class ChembltoMongo:
    def __init__(self, config):
        self.config = config
        self.db = DBConnection(self.config.get("db_name"), self.config.get("col_name"), config=self.config)
    def start(self, parser, use_progress_bar=True):
        self.db.add_index("chembl_id")
        for data in tqdm(parser.start(), total=parser.get_batch_num(), disable=not use_progress_bar):
            self.db.insert(data, accelerate=True, buffer_size=10000)


if __name__ == "__main__":
    cfg = "/home/zhaojingtong/tmpcode/PharmData/PharmDataProject/conf/drugkb.config"
    config = ConfigParser(cfg)
    config.set_section("chembl")
    to_mongo = ChembltoMongo(config)
    parser = ChemblParser(config)
    ChembltoMongo(config).start(parser)
