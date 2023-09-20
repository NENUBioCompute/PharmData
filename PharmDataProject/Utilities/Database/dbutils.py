"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2023/09/18 19:17
  @Email: 2665109868@qq.com
  @function
"""
import configparser
import pymongo
import json
import logging
import os
import sys

from pymongo import MongoClient

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)


class DBconnection(object):

    def __init__(self, cfgfile, mydb, myset):
        # TODO: option to specify config file
        config = configparser.ConfigParser()
        self.cfgfile = cfgfile
        config.read(self.cfgfile)


        client = MongoClient(host=config.get('dbserver', 'host'), port=int(config.get('dbserver', 'port')))
        # db = client.admin

        # db.authenticate(config.get('dbserver', 'user'), config.get('dbserver', 'password'))
        #pymongod 4.5
        client = pymongo.MongoClient(host="127.0.0.1", port=27017, username="readwrite", password="readwrite")
        self.mydb = mydb
        self.myset = myset

        self.my_db = client[mydb]
        self.collection = self.my_db[myset]


if __name__ == '__main__':

    # usage
    cfgfile = '../conf/drugkb.config'
    db = DBconnection(cfgfile, 'DrugKB', 'source_drugbank')
    # print('connected!')
    # db.collection.insert_one({"name":"zhangsan","age":18})
    for i in db.collection.find({"_id": "DB00001"}):
        print(i)
