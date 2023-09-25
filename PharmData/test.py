# -*- coding: utf-8 -*-
# developer   ：zhangkeming
# date   ：2023/9/25  16:31
import xlrd2
import json
from pymongo import MongoClient
from DataParsers import DataParsers as DP
from DatatoMongdb import DatatoMongdb as DM
if __name__ == '__main__':
    #Variable initialization
        #local connection

    host="localhost"
    port=27017
    username=''
    password=''
    dbname = "PharmRG"
    collection='Test'
        #remote connection

    # host = "59.73.198.168"
    # port = 27017
    # username = 'readwrite'
    # password = 'readwrite'
    # dbname = "PharmRG"
    # collection = 'Test'

        # variable of dataparsers
        # list1 is used for function import_inline in package DataParsers
        # list1,list2 is used for function import_inline in package DataParsers
        # list1 deposit variable in string type
        # list2 deposit variable in list type

    list1=['TARGETID','FORMERID','UNIPROID','TARGNAME','GENENAME','TARGTYPE','SYNONYMS',
           'FUNCTION','PDBSTRUC','BIOCLASS','ECNUMBER','SEQUENCE']
    list2=['DRUGINFO','KEGGPATH','WIKIPATH','WHIZPATH','REACPATH','NET_PATH',
           'INTEPATH','PANTPATH','BIOCPATH']
    signal='KEY'
    file_url='D:\桌面\学校\PharmRG科研\TTD_data\处理表格\P1-01-TTD_target_download.xlsx'

        # Object initialization
    connect = DM.DatatoMongdb(host,port,username,password,dbname,collection)
    importer = DP.DataParsers(signal,list1,list2,file_url,connect)

    # the area for using function in DataParsers
    # importer.import_inline()
    importer.import_list()
