# Title     : to_mongo.py
# Created by: julse@qq.com
# Created on: 2021/6/23 14:00
import configparser
import json
import time
import os
from PharmDataProject.Utilities.Database.dbutils import DBconnection

def result(dic):
    '''
    :param iter: csv parsed into one dict
    :return: list of the dict value
    '''
    if isinstance(dic,list):
        for v in dic:
            yield v
    else:
        for v in dic.values():
            yield v
def saveto_mongo(fin_json,db):
    dics = result(json.load(open(fin_json,'r')))
    for dic in dics:
        db.collection.insert_one(dic)
    # if indexs != None: do.createIndex(indexs)
def to_mongo(cfgfile = '../conf/drugkb.config'):
    config = configparser.ConfigParser()
    config.read(cfgfile)
    section = 'ttd'
    for idx in range(0,int(config.get(section, 'col_num'))):
        db = DBconnection(cfgfile, config.get(section, 'db_name'),
                          config.get(section, 'col_name_' + str(idx + 1)))
        table_name = config.get(section, 'col_name_%d'%(idx + 1))
        fin_json = os.path.join(config.get(section, 'json_path_%d' % (idx + 1)))
        print('strore',idx,table_name,fin_json)
        saveto_mongo(fin_json, db)
if __name__ == '__main__':
    print('start', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    start = time.time()
    to_mongo()
    print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('time', time.time() - start)

