# -*- coding: utf-8 -*-
# @Time : 2023/10/15 14:40
# @Author : zhudexu
# @FileName: PubChemDatatoMongo.py
# @Software: PyCharm

from pymongo import MongoClient


class PubChemDatatoMongo:
    def __init__(self, mongodb_url, database_name, collection_name):
        self.client = MongoClient(mongodb_url)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]

    def save_compound_data(self, info_json, cid):
        try:
            if "Record" in info_json:
                compound_data = info_json["Record"]
                self.collection.insert_one(compound_data)
                print(f'{cid}数据保存成功！')
            else:
                print('未找到相关化合物信息。')
        except Exception as e:
            print(f'{cid}数据保存出错：{str(e)}')
