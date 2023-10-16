# -*- coding: utf-8 -*-
""" Index genbank dataset with MongoDB"""
import argparse
import os
import sys
import json
import ijson
import gzip
from PharmDataProject.Utilities.Database.dbutils import DBconnection
import configparser



def mkpath(path):
    if not os.path.exists(path):
        os.makedirs(path)
        return path
    else:
        return path

def un_gz(file_name):
    f_name = file_name.replace(".gz", "")
    g_file = gzip.GzipFile(file_name)
    open(f_name, "wb+").write(g_file.read())
    g_file.close()

def parse_to_mongo(data_path, json_path, db):

    file = os.path.join(data_path, os.listdir(data_path)[0])
    read_file = open(file, 'r', encoding='utf-8').readlines()

    # parse gene
    output = []
    keys = read_file[0].lstrip('#').rstrip('\n').split('\t')
    for index, line in enumerate(read_file[1:-1]):
        values = line.strip('\n').split('\t')
        # print(values)
        for i in range(0, len(values)):
            if '|' in values[i]:
                values[i] = values[i].split('|')
        one_data = {keys[i].replace('.', ' '):values[i].replace('-','') if type(values[i])==type("") else values[i]
                         for i in range(len(keys))}
        output.append(one_data)

        # to_mongo

        db.collection.insert_one(one_data)

    print('ok!')
    # write to json
    # json_file_path = mkpath(json_path) + json_path.split('/')[-2] + '.json'
    # with open(json_file_path, "w", encoding="utf-8") as jf:
    #     json.dump(output, jf, indent=4, ensure_ascii=False)


if __name__ == '__main__':

    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)

    # parse_to_mongo
    for i in range(0, int(config.get('genebank', 'data_path_num'))):  # (0, 4)
        db = DBconnection(cfgfile, config.get('genebank', 'db_name'),
                          config.get('genebank', 'col_name_' + str(i + 1)))

        print(db.collection)

        parse_to_mongo(config.get('genebank', 'data_path_' + str(i + 1)),
                       config.get('genebank', 'json_path_' + str(i + 1)),
                       db)


