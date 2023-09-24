"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2023/09/23 19:57
  @Email: 2665109868@qq.com
  @function
"""
# ! /usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2020/8/15
# @Author  : Yuanzhao Guo
# @Email   : guoyz@nenu.edu.cn
# @File    : Nsides.py
# @Software: PyCharm

import argparse
import os
import sys
import json
import csv
from progress.bar import Bar
from PharmDataProject.Utilities.Database.dbutils import DBconnection

import json
import cobra
from cobra.io import read_sbml_model, write_sbml_model,model_to_dict
import configparser

# config = configparser.ConfigParser()
# cfgfile = '../../../conf/drugkb.config'
# config.read(cfgfile)


#########################################################################################################################################

def parse_all(data_path, json_path, name):
    for (root, dirs, files) in os.walk(data_path):
        print(data_path)
        for filename1 in files:
            print(filename1)
            with open(os.path.join(root, filename1), 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for each in reader:
                    fw = open(json_path + each[name] + ".json", "w")
                    json.dump(each, fw, indent=4)
                    fw.close()


def parse_all1(data_path, json_path, name, name1):
    for (root, dirs, files) in os.walk(data_path):
        for filename1 in files:
            with open(os.path.join(root, filename1), 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for each in reader:
                    # print(each)
                    fw = open(json_path + each[name] + "_" + each[name1] + ".json", "w")
                    json.dump(each, fw, indent=4)
                    fw.close()


def sbml_to_cobra_json(inf):
    c = read_sbml_model(inf)
    r =model_to_dict(c)
    r = cobra.io.model_to_dict(c)
    return r


def parse_sbml(data_path, json_path):
    for (root, dirs, files) in os.walk(data_path):
        for filename1 in files:
            r = sbml_to_cobra_json(data_path + filename1)
            filename = filename1.replace(".sbml", "")
            fw = open(json_path + filename + ".json", "w")
            json.dump(r, fw, indent=4)
            fw.close()


def parse(data_path, json_path):
    if data_path == "../../../data/smpdb/pathway/":
        data_path1 = data_path + "pathway/"
        name = "SMPDB ID"
        parse_all(data_path1, json_path, name)
    if data_path == "../../../data/smpdb/metabolite/":
        data_path1 = data_path + "metabolite/"
        name = "SMPDB ID"
        name1 = "Metabolite ID"
        parse_all1(data_path1, json_path, name, name1)
    if data_path == "../../../data/smpdb/protein/":
        data_path1 = data_path + "protein/"
        name = "SMPDB ID"
        name1 = "Uniprot ID"
        parse_all1(data_path1, json_path, name, name1)
    if data_path == "/PharmRG/SMPDB/sbml/":
        print("yes")
        data_path1 = data_path
        parse_sbml(data_path1, json_path)


def to_mongo(json_path, db):
    for root, dirs, files in os.walk(json_path):
        for ff in files:
            with open(os.path.join(root, ff), "r", encoding='utf-8') as f:
                json_f = json.load(f, encoding='utf-8')
                db.collection.insert_one(json_f)


#########################################################################################################################################

#########################################################################################################################################
if __name__ == '__main__':
    # d = os.path.dirname(os.path.abspath(__file__))
    #
    # parser = argparse.ArgumentParser(
    #     description='Index drugs.com dataset with MongoDB, '
    # )
    # parser.add_argument('-infile', '--infile',
    #                     # required=True,
    #                     default=config.get('smpdb', 'json_path_1'),
    #                     help='Input file name')
    # parser.add_argument('--index',
    #                     default=config.get('smpdb', 'db_name'),
    #                     help='Name of the MongoDB database index')
    # parser.add_argument('--mdbcollection',
    #                     default=config.get('smpdb', 'col_name_1'),
    #                     help='MongoDB collection name')
    # parser.add_argument('--allfields', default=False, action='store_true',
    #                     help="By default sequence fields"
    #                          " and the patents field is not indexed."
    #                          " Select this option to index all fields")
    # args = parser.parse_args()

    # parse
    # for i in range(0, int(config.get('smpdb', 'data_path_num'))): #(0, 4)
    #      parse(config.get('smpdb', 'data_path_' + str(i + 1)), config.get('smpdb', 'json_path_' + str(i + 1)))
    # to_mongo
    parse('/PharmRG/SMPDB/sbml/','/PharmRG')
    # for i in range(3, int(config.get('smpdb', 'json_path_num'))):  # (0, 4)
    #     db = DBconnection(cfgfile, config.get('twosides', 'db_name'), config.get('smpdb', 'col_name_' + str(i + 1)))
    #     print(db.collection)
    #     to_mongo(config.get('smpdb', 'json_path_' + str(i + 1)), db)



