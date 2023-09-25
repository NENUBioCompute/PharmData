import xlrd2
import json
from pymongo import MongoClient
import DatatoMongdb

class DataParsers:
    def __init__(self,signal,list1,list2,file_url,db:DatatoMongdb):
        self.signal=signal
        self.list1=list1
        self.list2=list2
        self.file_url=file_url
        self.db=db
    def import_inline(self):
        # 连接数据库
        client = MongoClient(host=self.db.host, port=self.db.port,username=self.db.username,password=self.db.password)
        db = client[self.db.dbname]
        collection = self.db.collection

        # 读取Excel文件
        data = xlrd2.open_workbook(self.file_url)
        table = data.sheets()[0]

        # 读取excel第一行数据作为存入mongodb的字段名
        row_stag = table.row_values(0)
        n_rows = table.nrows
        return_data = {}

        save_size = 0
        save_collection = []
        for i in range(1, n_rows):
            # 将字段名和excel数据存储为字典形式，并转换为json格式
            return_data[i] = json.dumps(dict(zip(row_stag, table.row_values(i))))
            # 通过编解码还原数据
            return_data[i] = json.loads(return_data[i])
            # 转换表头为所需字段名
            # del return_data[i]['序号']
            for j in self.list1:
                 return_data[i][j] = return_data[i].pop(j)

            save_collection.insert(save_size, return_data[i])
            if save_size >= 1000:
                db[collection].insert_many(save_collection)
                save_size = 0
                save_collection.clear()
            else:
                save_size += 1

        db[collection].insert_many(save_collection)
    def import_list(self):
        # 连接数据库
        client = MongoClient(host=self.db.host, port=self.db.port, username=self.db.username, password=self.db.password)
        db = client[self.db.dbname]
        collection = self.db.collection

        # 读取Excel文件
        data = xlrd2.open_workbook(self.file_url)
        table = data.sheets()[0]

        # 读取excel第一行数据作为存入mongodb的字段名
        row_stag = table.row_values(0)
        n_rows = table.nrows
        return_data = {}
        # 原始数据
        #数据集合计数器
        k = 0

        save_collection = []
        # 缓冲list
        return_data2 = {}
        # 上传数据
        for i in range(1, n_rows):
            # 将字段名和excel数据存储为字典形式，并转换为json格式
            return_data[i] = json.dumps(dict(zip(row_stag, table.row_values(i))))
            # 通过编解码还原数据
            return_data[i] = json.loads(return_data[i])
            return_data2[k + 1] = {}

            # 对于唯一标识的数据进行归纳
            if return_data[i][self.signal] == '':
                k += 1
                for j in self.list1:
                    return_data2[k][j] = ''
                for j in self.list2:
                    return_data2[k][j] = []
            for j in self.list1:
                if j == return_data[i]['KEY']:
                    return_data2[k][j] = return_data[i].pop('VALUE')
            for j in self.list2:
                if j == return_data[i]['KEY']:
                    # 对于特殊数据的处理
                    if j=='DRUGINFO':
                        return_data2[k][j].append(return_data[i].pop('VALUE')+' '+return_data[i].pop('DRUGNAME')+' '+return_data[i].pop('Highest Clinical Status'))
                    elif j == 'INDICATI':
                        return_data2[k][j].append(return_data[i].pop('VALUE') + ' ' + return_data[i].pop('ADD'))
                        # return_data2[k][j].append(return_data[i].pop('VALUE'))
                    else:
                        return_data2[k][j].append(return_data[i].pop('VALUE'))
        for t in range(1, k):
            save_collection.insert(t, return_data2[t])
            if t % 1000 == 0:
                db[collection].insert_many(save_collection)
                save_collection.clear()
        db[collection].insert_many(save_collection)
