# -*- coding: utf-8 -*-
# developer   ：zhangkeming
# date   ：2023/9/25  16:31
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

    # this function is used when every line data is a data collection
    def import_inline(self):
        # connect the database
        client = MongoClient(host=self.db.host, port=self.db.port,username=self.db.username,password=self.db.password)
        db = client[self.db.dbname]
        collection = self.db.collection

        # read Excel file
        data = xlrd2.open_workbook(self.file_url)
        table = data.sheets()[0]

        # read the first line data of Excel the as field name of mongodb
        row_stag = table.row_values(0)
        n_rows = table.nrows
        return_data = {}

        save_size = 0
        save_collection = []
        for i in range(1, n_rows):
            # deposit field name and excel data as dictionary type and transform into json type
            return_data[i] = json.dumps(dict(zip(row_stag, table.row_values(i))))
            # Restore the data by encoding it
            return_data[i] = json.loads(return_data[i])
            # transform tabel head into field name
            for j in self.list1:
                 return_data[i][j] = return_data[i].pop(j)
            # Whenever the data volume reaches 1,000 insert them into database
            save_collection.insert(save_size, return_data[i])
            if save_size >= 1000:
                db[collection].insert_many(save_collection)
                save_size = 0
                save_collection.clear()
            else:
                save_size += 1

        db[collection].insert_many(save_collection)
    # this function is used when field need to deposit list type data
    def import_list(self):
        # connect database
        client = MongoClient(host=self.db.host, port=self.db.port, username=self.db.username, password=self.db.password)
        db = client[self.db.dbname]
        collection = self.db.collection

        # read Excel file
        data = xlrd2.open_workbook(self.file_url)
        table = data.sheets()[0]

        row_stag = table.row_values(0)
        n_rows = table.nrows
        return_data = {}
        # data connection count
        k = 0

        save_collection = []
        # buffer list
        return_data2 = {}

        for i in range(1, n_rows):
            # deposit field name and excel data as dictionary type and transform into json type
            return_data[i] = json.dumps(dict(zip(row_stag, table.row_values(i))))
            # Restore the data by encoding it
            return_data[i] = json.loads(return_data[i])
            return_data2[k + 1] = {}

            # Generinduction for the unique identity of the data

            # every Excel data collection has a line of null in front of it,so the count add 1 when read null
            if return_data[i][self.signal] == '':
                k += 1
                # initialize the field
                for j in self.list1:
                    return_data2[k][j] = ''
                for j in self.list2:
                    return_data2[k][j] = []
                # if read the field name of our list,deposit the data into the field
            for j in self.list1:
                if j == return_data[i]['KEY']:
                    return_data2[k][j] = return_data[i].pop('VALUE')
            for j in self.list2:
                if j == return_data[i]['KEY']:
                    # Some fields need to be trimmed or spliced,do special treatment according to the case
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
