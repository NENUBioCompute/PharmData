# -*- coding: utf-8 -*-
"""
# @Time        : 2023/10/8 9:27
# @Author      : tanliqiu
# @FileName    : BiogridtoMongo.py
# @Software    : PyCharm
# @ProjectName : Biogrid
# @Description : null
"""
import pymongo

class MongoSave:
    def mongo(filePath):
        mongoClient = pymongo.MongoClient(host="localhost", port=27017)
        Biogrid = mongoClient.Biogrid
        all = Biogrid['all']
        # data={'static':'123'}
        with open(filePath,'rb') as file:
            all.insert_many(file)
        mongoClient.close()

if __name__ == "__main__":
    MongoSave().mongo('./Biogrid_data/static.json')