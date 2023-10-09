"""
  -*- encoding: utf-8 -*-
  @Author: yuziyang
  @Time  : 2023/10/09
  @Email: 2601183959@qq.com
  @function
"""
import logging

from pymongo import MongoClient


class MongoDBHandler:
    def __init__(self, user, dataSet, collections):
        self.user = user
        self.dataSet = dataSet
        self.collections = collections
        self.client = MongoClient(user)
        self.db = self.client[dataSet]
        self.collection = self.db[collections]

    def insert_data(self, data):
        try:
            self.collection.insert_one(data)
            logging.log("数据插入成功！")
        except Exception as e:
            logging.log(f"数据插入失败：{e}")

    def delete_all_data(self):
        try:
            self.collection.delete_many({})
            logging.log("清除成功！")
        except Exception as e:
            logging.log(f"清除失败:{e}")

    def insert_many_data(self, data):
        try:
            self.collection.insert_many(data)
            logging.log("数据插入成功！")
        except Exception as e:
            logging.log(f"数据插入失败：{e}")