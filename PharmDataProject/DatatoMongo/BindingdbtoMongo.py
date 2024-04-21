'''
@Author : gaoqingshan
@Time   : 2023/10/8 17:33
@Email  : 519546702@qq.com
@function
'''
import json
import configparser

from PharmDataProject.DataParsers.BindingdbParser import parse_bindingdb
from PharmDataProject.Utilities.Database.dbutils import DBconnection

config = configparser.ConfigParser()
cfgfile = "../conf/drugkb.config"
config.read(cfgfile)


def save_json(path, save_path):
    """

    :param path:parse file path
    :param save_path:
    """
    from PharmDataProject.DataParsers.BindingdbParser import parse_bindingdb
    bindingdb_generator = parse_bindingdb(path=path)
    datas_dict_lists = []
    for i, row in enumerate(bindingdb_generator):
        datas_dict_lists.append(row)
    with open(save_path, "w") as json_file:
        json.dump(datas_dict_lists, json_file, indent=4)


def save_mongodb(path, db):
    """
    :param path: parse file path
    :param db: db connection
    """
    # clear the database
    db.collection.delete_many({})
    bindingdb_generator = parse_bindingdb(path=path)
    datas_dict_lists = []
    for i, row in enumerate(bindingdb_generator):
        datas_dict_lists.append(row)
        db.collection.insert_one(row)


if __name__ == '__main__':
    path = config.get('bindingdb', 'path')
    db = DBconnection(cfgfile, config.get('bindingdb', 'db_name'), config.get('bindingdb', 'col_name_1'))
    save_mongodb(path, db)
    # save_json(path=path, save_path=config.get('bindingdb', 'data_path_1'))