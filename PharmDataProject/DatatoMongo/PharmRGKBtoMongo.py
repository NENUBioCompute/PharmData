# encoding: utf-8
'''
@Author  : julse@qq.com Wenjie.Lee
@Time    : 2022/7/25 21:36
@desc    : 
'''

import configparser
import time
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmRGKBParser import PharmGKBParser


class Pharmgkb_Mongo:
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

    def __to_mongo(self):
        section = 'pharmgkb'
        tables = self.config.get(section, 'tables')[1:-1].split(',')

        # Initialize the parser
        pharmGKBParser = PharmGKBParser()
        parsed_data = pharmGKBParser.start(self.config)

        for idx in range(int(self.config.get(section, 'col_num'))):
            db = DBconnection(self.config_path, self.config.get('pharmgkb', 'db_name'),
                              self.config.get('pharmgkb', 'col_name_' + str(idx + 1)))

            # Insert parsed data into MongoDB
            table_name = tables[idx]
            data = parsed_data[table_name]

            if isinstance(data, dict):
                first_item = next(iter(data.values()))
            elif isinstance(data, list):
                first_item = data[0]
            else:
                first_item = None

            if first_item:
                self.__saveto_mongo(first_item, db)
                print(f"Inserted data into collection: {self.config.get('pharmgkb', 'col_name_' + str(idx + 1))}")
                # 注释掉以下的 break 语句以插入每个文件夹中的所有字典
                # break

    def __saveto_mongo(self, data, db):
        db.collection.insert_one(data)

    def start(self):
        print('start', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        start_time = time.time()

        self.__to_mongo()
        print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        print('time', time.time() - start_time)


class Pharmgkb_Mongo_Factory:
    @classmethod
    def create_Pharmgkb_Mongo(cls, config_path):
        return Pharmgkb_Mongo(config_path)


if __name__ == "__main__":
    # 配置文件路径
    config_path = '../conf/drugkb_test.config'

    # 创建 Pharmgkb_Mongo 类的实例
    pharmgkb_mongo = Pharmgkb_Mongo_Factory.create_Pharmgkb_Mongo(config_path)

    # 初始化解析类
    pharmGKBParser = PharmGKBParser()
    config = configparser.ConfigParser()
    config.read(config_path)
    parsed_data = pharmGKBParser.start(config)

    # 获取配置中的表名
    section = 'pharmgkb'
    tables = config.get(section, 'tables')[1:-1].split(',')

    # 循环每个文件夹，入库第一个字典
    for idx in range(int(config.get(section, 'col_num'))):
        db = DBconnection(config_path, config.get('pharmgkb', 'db_name'),
                          config.get('pharmgkb', 'col_name_' + str(idx + 1)))

        # 插入解析的数据到MongoDB
        table_name = tables[idx]
        data = parsed_data[table_name]

        if isinstance(data, dict):
            first_item = next(iter(data.values()))
        elif isinstance(data, list):
            first_item = data[0]
        else:
            first_item = None

        if first_item:
            db.collection.insert_one(first_item)
            print(f"Inserted data into collection: {config.get('pharmgkb', 'col_name_' + str(idx + 1))}")
            break  # 插入第一个后退出循环
