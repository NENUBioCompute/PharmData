"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2023/09/21 21:15
  @Email: 2665109868@qq.com
  @function
"""
# -*- coding: utf-8 -*-
""" Index genbank dataset with MongoDB"""
import argparse
import os
import sys
import json
import ijson
import gzip
import collections
from PharmDataProject.Utilities.Database.dbutils import DBconnection
import configparser

config = configparser.ConfigParser()
cfgfile = "../../conf/drugkb.config"
config.read(cfgfile)


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

def parse_to_mongo(data_path, db):


    with gzip.open(data_path, 'rt') as gz_file:
        output = []
        keys = gz_file.readline().lstrip('#').rstrip('\n').split('\t')
        for index, line in enumerate(gz_file):
            values = line.strip('\n').split('\t')
            for i in range(0, len(values)):
                if '|' in values[i]:
                    values[i] = values[i].split('|')
            one_data = {keys[i].replace('.', ''): values[i].replace('-','')
                             for i in range(len(keys))}
            output.append(one_data)

            # to_mongo
            gene_ids = db.collection.find({}, {"GeneID": -1})
            if one_data['GeneID'] not in gene_ids:
                db.collection.insert_one(one_data)
            else:
                continue
        print('ok!')
        # write to json
        # json_file_path = mkpath(json_path) + json_path.split('/')[-2] + '.json'
        # with open(json_file_path, "w", encoding="utf-8") as jf:
        #     json.dump(output, jf, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    # d = os.path.dirname(os.path.abspath(__file__))
    #
    # parser = argparse.ArgumentParser(
    #     description='Index drugs.com dataset with MongoDB, '
    # )
    # parser.add_argument('-infile', '--infile',
    #                     # required=True,
    #                     default=config.get('genbank', 'json_path_1'),
    #                     help='Input file name')
    # parser.add_argument('--index',
    #                     default=config.get('genbank', 'db_name'),
    #                     help='Name of the MongoDB database index')
    # parser.add_argument('--mdbcollection',
    #                     default=config.get('genbank', 'col_name_1'),
    #                     help='MongoDB collection name')
    # parser.add_argument('--allfields', default=False, action='store_true',
    #                     help="By default sequence fields"
    #                          " and the patents field is not indexed."
    #                          " Select this option to index all fields")
    # args = parser.parse_args()

    # parse_to_mongo
    for i in range(1, int(config.get('genebank', 'data_path_num'))+1):  # (0, 4)
        db = DBconnection(cfgfile, config.get('genebank', 'db_name'), config.get('genebank', 'col_name_'+str(i)))
        db.collection.delete_many({})
        print(db.collection)
        if i==1:
            parse_to_mongo(data_path='/PharmRG/GeneBank/gene_info.gz', db=db)
        elif i==2:
            parse_to_mongo(data_path='/PharmRG/GeneBank/gene2pubmed.gz', db=db)


