"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 4/15/2024 11:44 PM
  @Email: deepwind32@163.com
"""
import os
import pickle
import pprint

import pandas as pd

from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


class StitchParser:
    def __init__(self, config):
        self.config = config
        self.data_path = config.get("data_path")

    def __read(self, filepath):
        return pd.read_csv(filepath.replace(".gz", ""), delimiter='\t')

    def __concat(self, detailed_data, transfer_data):
        return pd.concat([transfer_data, detailed_data[["experimental", "prediction", "database", "textmining"]]], axis=1)

    def start(self):
        filenames = [self.config.get("cc_links_filename"),
                     (self.config.get("pc_links_detailed_filename"), self.config.get("pc_links_transfer_filename")),
                     self.config.get("actions_filename")]
        collections = [self.config.get("cc_links_collection"),
                       self.config.get("pc_links_collection"),
                       self.config.get("actions_collection")]
        for i, filename in enumerate(filenames):
            if i == 0:
                pass
                # with open("./data.b","rb") as f:
                #     yield self.config.get("cc_links_collection"), pickle.load(f)
                # yield (collections[0],
                #        self.__read(os.path.join(self.data_path + filename)).to_dict(orient='records'))
            elif i == 1:
                pass
                # yield (collections[1],
                #        self.__concat(self.__read(os.path.join(self.data_path, filename[0])),
                #                      self.__read(os.path.join(self.data_path, filename[1]))).to_dict(orient='records'))
            elif i == 2:
                yield (collections[2],
                       self.__read(os.path.join(self.data_path, filename)).to_dict(orient='records'))


if __name__ == "__main__":
    cfg = "/home/zhaojingtong/tmpcode/PharmData/PharmDataProject/conf/drugkb.config"
    config = ConfigParser(cfg)
    config.set_section("stitch")
    for i in StitchParser(config).start():
        pprint.pprint(i)
