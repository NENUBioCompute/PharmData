# -*- coding: utf-8 -*-
"""
# @Time        : 2023/10/8 
# @Author      : yvshilin
# @Email       : 3542804395@qq.com
# @FileName    : ChEMBLtoMongo.py
# @Software    : PyCharm
# @ProjectName : ChEMBL
"""
import pymongo

class FileStore:
    def ImportCsvToMongodb(self, mongodbUrl, csvFile, dbName, collectionName):
        client = pymongo.MongoClient(mongodbUrl)
        database = client[dbName]
        collection = database[collectionName]
        with open(csvFile, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                collection.insert_one(row)
        client.close()
