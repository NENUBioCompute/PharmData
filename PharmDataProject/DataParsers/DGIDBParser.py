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
def mkpath(path):
    if not os.path.exists(path):
        os.mkdir(path)
        return path
    else:
        return path

def parse(data_path, json_path):
    file = os.path.join(data_path, os.listdir(data_path)[0])
    read_file = open(file, 'r').readlines()

    # parse dgi
    get_dict = {}
    keys = read_file[0].strip('\n').split('\t')
    for index, line in enumerate(read_file[1:]):
        values = line.strip('\n').split('\t')
        # get_dict[index] = {keys[i]: values[i] for i in range(len(keys))}
        data = {keys[i]: values[i] for i in range(len(keys))}
        # print(data)
        db.collection.insert_one(data)

    # write to json
    # json_file_path = mkpath(json_path) + json_path.split('/')[-2] + '.json'
    # with open(json_file_path, "w", encoding="utf-8") as jf:
    #     jf.write(json.dumps(get_dict, indent=4))


def to_mongo(json_path, db):
    json_file_path = mkpath(json_path) + json_path.split('/')[-2] + '.json'

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

    # parse
    for i in range(0, int(config.get('dgidb', 'data_path_num'))):  # (0, 4)
        db = DBconnection(cfgfile, config.get('dgidb', 'db_name'),config.get('dgidb', 'col_name_' + str(i + 1)))
        parse(config.get('dgidb', 'data_path_' + str(i + 1)),config.get('dgidb', 'json_path_' + str(i + 1)))

    # to_mongo
    # for i in range(0, int(config.get('dgidb', 'json_path_num'))):  # (0, 4)
    #     db = DBconnection(cfgfile, config.get('dgidb', 'db_name'),
    #                       config.get('dgidb', 'col_name_' + str(i + 1)))
    #     print(db.collection)
    #     to_mongo(config.get('dgidb', 'json_path_' + str(i + 1)), db)
    #
