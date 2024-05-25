# -*- encoding: utf-8 -*-
# @Author: Deepwind
# @Time  : 4/15/2024 11:44 PM
# @Email: deepwind32@163.com

import multiprocessing
import os
import pprint
import time
import pandas as pd
import gzip
from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


class StitchParser:
    def __init__(self, config):
        self.config = config
        self.data_path = config.get("data_path")

    def __read(self, filepath):
        print(f"Reading file: {filepath}")
        start_time = time.time()
        with gzip.open(filepath, 'rt') as f:
            data = pd.read_csv(f, delimiter='\t')
        print(f"Finished reading file: {filepath} in {time.time() - start_time:.2f}s")
        return data

    def __concat(self, detailed_data, transfer_data):
        print("Concatenating data")
        return pd.concat([transfer_data, detailed_data[["experimental", "prediction", "database", "textmining"]]],
                         axis=1)

    @staticmethod
    def todict(data, q):
        q.put(data.to_dict(orient='records'))

    def __fast_todict(self, data, process_num=10):
        """
        多线程调用data.to_dict()
        """
        start = time.time()
        print("Starting __fast_todict")
        process_list = []
        lines = data.shape[0]
        print(f"Data has {lines} lines")
        is_divisible = True if lines % process_num == 0 else False

        step = lines // process_num if is_divisible else lines // process_num + 1
        print(f"Step size: {step}")

        left = step if is_divisible else lines % step
        split_index = [(i, i + step) for i in range(0, lines, step)][:-1]
        data_splits = [data.iloc[i:j, ] for i, j in split_index]
        data_splits.append(data.iloc[lines - left:, ])

        q = multiprocessing.Queue()
        for i in range(process_num):
            print(f"Starting process {i}")
            p = multiprocessing.Process(target=self.todict, args=(data_splits[i], q))
            p.start()
            process_list.append(p)

        for p in process_list:
            p.join()

        print(f"Finished __fast_todict in {time.time() - start:.2f}s")
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
                print(f"Processing file: {filename}")
                yield (collections[0],
                       self.__fast_todict(self.__read(os.path.join(self.data_path, filename))))
            elif i == 1:
                print(f"Processing files: {filename}")
                yield (collections[1],
                       self.__fast_todict(self.__concat(self.__read(os.path.join(self.data_path, filename[0])),
                                                        self.__read(os.path.join(self.data_path, filename[1])))))
            elif i == 2:
                print(f"Processing file: {filename}")
                yield (collections[2],
                       self.__fast_todict(self.__read(os.path.join(self.data_path, filename))))

if __name__ == "__main__":
    cfg = "../conf/drugkb_test.config"
    config = ConfigParser(cfg)
    config.set_section("stitch")
    parser = StitchParser(config)
    for i in parser.start():
        pprint.pprint(i)
        break
