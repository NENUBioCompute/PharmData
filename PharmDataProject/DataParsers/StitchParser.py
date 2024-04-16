"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2024/04/05 21:31
  @Email: 2665109868@qq.com
  @function
"""
import gzip
import os
import pprint

import pandas as pd

from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


class StitchParser:
    def __init__(self, config):
        self.config = config
        self.data_path = config.get("smpdb", "data_path")

    def __gunzip(self, filepath):
        with gzip.open(filepath, 'rb') as f_in:
            content = f_in.read().decode('utf-8')

        return pd.read_csv(filepath, delimiter='\t')

    def __concat(self, data1, data2):
        return pd.concat([data1, data2])

    def start(self):
        for dir_name in os.listdir(self.data_path):
            if dir_name.endswith('.gz'):
                yield self.__gunzip(os.path.join(self.data_path + dir_name))

if __name__ == "__main__":
    cfg = "/home/zhaojingtong/tmpcode/PharmData/PharmDataProject/conf/drugkb.config"
    config = ConfigParser.GetConfig(cfg)
    for i in StitchParser(config).start():
        pprint.pprint(i)