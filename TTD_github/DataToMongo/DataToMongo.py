

from pymongo import MongoClient
from DataParsers import *


def DataToMongo(signal, list1, host, port, name, collection, return_data):
    # 连接数据库
    client = MongoClient(host=host, port=port)
    db = client.local
    collection = collection

    # 读取Excel文件
    data = xlrd2.open_workbook(name)
    table = data.sheets()[0]

    # 读取excel第一行数据作为存入mongodb的字段名
    row_stag = table.row_values(0)
    n_rows = table.nrows
    # 原始数据

    k = 0
    now = 'qwq'
    # 获取现在数据的唯一标识

    save_collection = []
    # 缓冲list
    return_data2 = {}
    # 上传数据
    for i in range(1, n_rows):
        return_data2[k + 1] = {}

        if now != return_data[i][signal] and return_data[i][signal] != '':
            now = return_data[i][signal]
            k += 1
            # return_data2[k]['TARGETID'] = now
        for j in list1:
            if return_data[i]['B'] == 'SYNONYMS':
                return_data2[k]['SYNONYMS'] = return_data[i]['C']
            else:
                return_data2[k][return_data[i][j]] = return_data[i]['C']


    for t in range(1, k):
        save_collection.insert(t, return_data2[t])
        if t % 1000 == 0:
            db[collection].insert_many(save_collection)
            save_collection.clear()
    db[collection].insert_many(save_collection)
