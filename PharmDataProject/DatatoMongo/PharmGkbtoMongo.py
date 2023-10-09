"""
  -*- encoding: utf-8 -*-
  @Author: yuziyang
  @Time  : 2023/10/09
  @Email: 2601183959@qq.com
  @function
"""
import csv
import pandas as pd

from PharmDataProject.Utilities.Database.pharmGKBdb import MongoDBHandler


class PharmGkbtoMongo:
    def __init__(self, data_dir):
        self.data_dir = data_dir

    # 解析TSV文件  一条一条插入
    def solve(self, fN, collections, clomNum):
        handler = MongoDBHandler(user="mongodb://readwrite:readwrite@59.73.198.168:27017/",
                                 dataSet="PharmRG",
                                 collections="pharmGKB_" + collections)
        flag = 0
        fileName = self.data_dir + fN
        with open(fileName, 'r') as f:
            reader = csv.reader(f, delimiter='\t')
            rowName = [''] * clomNum
            for row in reader:
                if flag == 0:
                    for i in range(1, clomNum):
                        rowName[i] = row[i - 1]
                    flag = 1
                else:
                    # 将空数据转换为空字符串
                    row = [cell if cell else '' for cell in row]
                    # 将数据存储到MongoDB数据库中
                    data = {}
                    for i in range(1, clomNum):
                        data[rowName[i]] = row[i - 1]
                    handler.insert_data(data)

    # insert_many插入
    def quick_solve(self, fN, collections):
        handler = MongoDBHandler(user="mongodb://readwrite:readwrite@59.73.198.168:27017/",
                                 dataSet="PharmRG",
                                 collections="pharmGKB_" + collections)
        fileName = self.data_dir + fN
        data = pd.read_csv(fileName, sep='\t')
        data.replace(to_replace=[None, 'null', ''], value='', inplace=True)
        data_dict = data.to_dict('records')
        handler.insert_many_data(data_dict)
