# encoding: utf-8
'''
@Author  : julse@qq.com Wenjie.Lee
@Time    : 2022/7/25 21:36
@desc    : 
'''

import configparser
import json
import os
import time
from PharmDataProject.Utilities.Database.dbutils import DBconnection
# from DatabaseOperation2 import DataOperation

class Pharmgkb_Mongo:
    def __to_mongo(self, cfgfile):
        config = configparser.ConfigParser()
        config.read(cfgfile)

        section = 'pharmgkb'
        db_name = config.get(section, 'db_name')
        table_drug = config.get(section, 'col_name_1')
        table_genes = config.get(section, 'col_name_2')
        table_relationships = config.get(section, 'col_name_3')
        table_pathways = config.get(section, 'col_name_4')

        # index of the tables
        table_drug_index = ['PharmGKB-Accession-Id', 'Name']
        table_genes_index = ['PharmGKB-Accession-Id', 'Name']
        table_relationship_index = ['Entity1_name', 'Entity2_name']
        table_pathways_index = ['Genes', 'Drugs', 'Diseases']
        indexs = [table_drug_index, table_genes_index, table_relationship_index, table_pathways_index]

        tables = config.get(section, 'tables')[1:-1].split(',')
        tables_name = [table_drug, table_genes, table_relationships, table_pathways]



        for idx in range(int(config.get(section, 'col_num'))):
            db = DBconnection(cfgfile, config.get('pharmgkb', 'db_name'),
                              config.get('pharmgkb', 'col_name_' + str(idx + 1)))
            fin_json = os.path.join(config.get(section, 'json_path_%d' % (idx + 1)), '%s.json' % tables[idx])
            self.__saveto_mongo(fin_json, db)

    def __result(self, dic):
        '''
        :param iter: csv parsed into one dict
        :return: list of the dict value
        '''
        if isinstance(dic, list):
            for v in dic:
                yield v
        else:
            for v in dic.values():
                # if 'PharmGKB-Accession-Id' in v.keys(): v['_id'] = v['PharmGKB-Accession-Id']
                yield v

    def __saveto_mongo(self, fin_json, db):
        dics = self.__result(json.load(open(fin_json, 'r')))
        for dic in dics:
            db.collection.insert_one(dic)


    def start(self):
        print('start', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        start_time = time.time()

        self.__to_mongo('../conf/drugkb.config')
        print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        print('time', time.time() - start_time)

class Pharmgkb_Mongo_Factory:
    @classmethod
    def create_Pharmgkb_Mongo(self):
        return Pharmgkb_Mongo()

    pharmrg_mongo = Pharmgkb_Mongo()
    pharmrg_mongo.start()


if __name__ == "__main__":
    # 创建 Pharmgkb_Mongo 类的实例
    pharmgkb_mongo = Pharmgkb_Mongo_Factory.create_Pharmgkb_Mongo()

    # 开始执行数据导入
    pharmgkb_mongo.start()
