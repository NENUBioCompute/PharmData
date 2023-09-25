import xlrd2
import json
from pymongo import MongoClient
from DataParsers import DataParsers as DP
from DatatoMongdb import DatatoMongdb as DM
if __name__ == '__main__':
    #变量初始化
        #本地变量
    host="localhost"
    port=27017
    username=''
    password=''
    dbname = "PharmRG"
    collection='Test'
        #远程连接
    # host = "59.73.198.168"
    # port = 27017
    # username = 'readwrite'
    # password = 'readwrite'
    # dbname = "PharmRG"
    # collection = 'Test'

        #解析文件相关变量
    list1=['TARGETID','FORMERID','UNIPROID','TARGNAME','GENENAME','TARGTYPE','SYNONYMS',
           'FUNCTION','PDBSTRUC','BIOCLASS','ECNUMBER','SEQUENCE']
    list2=['DRUGINFO','KEGGPATH','WIKIPATH','WHIZPATH','REACPATH','NET_PATH',
           'INTEPATH','PANTPATH','BIOCPATH']
    signal='KEY'
    file_url='D:\桌面\学校\PharmRG科研\TTD_data\处理表格\P1-01-TTD_target_download.xlsx'
        #初始化对象
    connect = DM.DatatoMongdb(host,port,username,password,dbname,collection)
    importer = DP.DataParsers(signal,list1,list2,file_url,connect)

    #调用导入数据库的方法
    # importer.import_inline()
    importer.import_list()
