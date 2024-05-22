"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 4/15/2024 11:44 PM
  @Email: deepwind32@163.com
"""
import multiprocessing
import os
import pprint
import time

import pandas as pd

from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


class StitchParser:
    def __init__(self, config):
        self.config = config
        self.data_path = config.get("data_path")

    def __read(self, filepath):
        return pd.read_csv(filepath.replace(".gz", ""), delimiter='\t')

    def __concat(self, detailed_data, transfer_data):
        return pd.concat([transfer_data, detailed_data[["experimental", "prediction", "database", "textmining"]]],
                         axis=1)

    def __fast_todict(self, data, process_num=10):
        """
        多线程调用data.odict()
        """

        def todict(data, q):
            q.put(data.to_dict(orient='records'))

        # print("fast todict boot")
        start = time.time()
        process_list = []
        lines = data.shape[0]
        is_divisible = True if lines % process_num == 0 else False

        step = lines // process_num if is_divisible else lines // process_num + 1

        left = step if is_divisible else lines % step
        split_index = [(i, i + step) for i in range(0, lines, step)][:-1]
        data_splits = [data.iloc[i:j, ] for i, j in split_index]
        data_splits.append(data.iloc[lines - left:, ])

        q = multiprocessing.Queue()
        for i in range(process_num):
            p = multiprocessing.Process(target=todict, args=(data_splits[i], q))
            p.start()
            process_list.append(p)

        # print(f"todict operation finish in {time.time() - start:.2f}s")
        return [q.get() for _ in range(process_num)]

    def start(self):
        filenames = [self.config.get("cc_links_filename"),
                     (self.config.get("pc_links_detailed_filename"), self.config.get("pc_links_transfer_filename")),
                     self.config.get("actions_filename")]
        collections = [self.config.get("cc_links_collection"),
                       self.config.get("pc_links_collection"),
                       self.config.get("actions_collection")]
        for i, filename in enumerate(filenames):
            if i == 0:
                yield (collections[0],
                       self.__fast_todict(self.__read(os.path.join(self.data_path + filename))))
            elif i == 1:
                yield (collections[1],
                       self.__fast_todict(self.__concat(self.__read(os.path.join(self.data_path, filename[0])),
                                                        self.__read(os.path.join(self.data_path, filename[1])))))
            elif i == 2:
                yield (collections[2],
                       self.__fast_todict(self.__read(os.path.join(self.data_path, filename))))


if __name__ == "__main__":
    cfg = "/home/zhaojingtong/tmpcode/PharmData/PharmDataProject/conf/drugkb.config"
    config = ConfigParser(cfg)
    config.set_section("stitch")
    for i in StitchParser(config).start():
        pprint.pprint(i)
