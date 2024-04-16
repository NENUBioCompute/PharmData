"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2023/10/15 18:41
  @Email: 2665109868@qq.com
  @function
"""

# -*- coding: utf-8 -*-
""" Index dgidb dataset with MongoDB"""
import argparse
import os
import sys
import json
from PharmDataProject.Utilities.Database.dbutils import DBconnection
import configparser


class DGIDBParser:

    def __init__(self, data_path, json_path):
        self.data_path = data_path
        self.json_path = json_path

    def mkpath(self, path):
        if not os.path.exists(path):
            os.mkdir(path)
            return path
        else:
            return path

    def parse(self):
        file_path = os.path.join(self.data_path, os.listdir(self.data_path)[0])
        with open(file_path, 'r') as file:
            lines = file.readlines()

        keys = lines[0].strip().split('\t')
        parsed_data = []
        for line in lines[1:]:
            values = line.strip().split('\t')
            # 确保每个键都有值，使用 None 填充缺失的值
            data_dict = {keys[i]: (values[i] if i < len(values) else None) for i in range(len(keys))}
            parsed_data.append(data_dict)

        return parsed_data  # 返回解析的数据字典列表

    def to_mongo(self, db):
        json_file_path = self.mkpath(self.json_path) + self.json_path.split('/')[-2] + '.json'

        if not os.path.exists(json_file_path):
            print("dgidb file not find !")
            sys.exit()
        else:
            with open(json_file_path, 'r', encoding="utf-8") as jf:
                data = [value for value in json.load(jf).values()]
                db.collection.insert_many(data)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)

    for i in range(int(config.get('dgidb', 'data_path_num'))):
        data_path = config.get('dgidb', 'data_path_' + str(i + 1))
        json_path = config.get('dgidb', 'json_path_' + str(i + 1))
        db = DBconnection(cfgfile, config.get('dgidb', 'db_name'), config.get('dgidb', 'col_name_' + str(i + 1)))

        parser = DGIDBParser(data_path, json_path)
        data = parser.parse()

        if data:
            print(data[0])  # 打印第一个元素
            break  # 处理完第一组数据后停止循环
