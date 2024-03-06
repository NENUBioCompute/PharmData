# encoding: utf-8
'''
@Author  : Qiufen.Chen Wenjie.Lee
@Time    : 2022/7/25 21:36
@desc    : Uniprot parse
暂时不使用
'''

import xmltodict
import json
from collections import OrderedDict
import os
import configparser
from PharmDataProject.Utilities.Database.dbutils import DBconnection
uniprot = []
save_path = '../../../parsed/uniprot/'


class Uniprot_Parser:
    uniprot = []
    save_path = '../../../parsed/uniprot/'

    def __check_path(self, in_dir):
        if '.' in in_dir:
            in_dir, _ = os.path.split(in_dir)
        if not os.path.exists(in_dir):
            os.makedirs(in_dir)
            print('make dir ', in_dir)

    def __remover(self, x):
        if isinstance(x, list):
            return [self.__remover(y) for y in x]
        elif isinstance(x, OrderedDict):
            for ky in list(x.keys()):
                if ky[0] in ["@", "#"]:
                    x[ky[1:]] = self.__remover(x[ky])
                    del x[ky]
                else:
                    x[ky] = self.__remover(x[ky])
            return x
        else:
            return x

    def __updategene(self, e):
        if 'gene' in e.keys():
            _gene = []
            if isinstance(e['gene'], list):
                for k in e['gene']:
                    if isinstance(k['name'], dict):
                        _gene.append(k['name'])

                    elif isinstance(k['name'], list):
                        for i in k['name']:
                            _gene.append(i)
            elif isinstance(e['gene']['name'], dict):
                _gene = [e['gene']['name']]
            elif isinstance(e['gene']['name'], list):
                _gene = [i for i in e['gene']['name']]
            return _gene

    def __updateprotein(self, g):
        protein = g['protein']
        dic = dict()
        if isinstance(protein['recommendedName'], str):
            dic.update({'recommendedName': protein['recommendedName']})

        if isinstance(protein['recommendedName'], dict):
            if 'fullName' in protein['recommendedName'].keys():
                if isinstance(protein['recommendedName']['fullName'], str):
                    dic.update({'recommendedName': protein['recommendedName']['fullName']})

                elif isinstance(protein['recommendedName']['fullName'], dict):
                    dic.update({'recommendedName': protein['recommendedName']['fullName']['text']})

            if 'ecNumber' in protein['recommendedName'].keys():
                if isinstance(protein['recommendedName']['ecNumber'], str):
                    dic.update({'ecNumber': protein['recommendedName']['ecNumber']})

                elif isinstance(protein['recommendedName']['ecNumber'], dict):
                    dic.update({'ecNumber': protein['recommendedName']['ecNumber']['text']})

            if 'shortName' in protein['recommendedName'].keys():
                if isinstance(protein['recommendedName']['shortName'], list):
                    aa = []
                    for i in protein['recommendedName']['shortName']:
                        if isinstance(i, dict):
                            aa.append(i['text'])
                        elif isinstance(i, str):
                            aa.append(i)
                    dic.update({'shortName': aa})

                elif isinstance(protein['recommendedName']['shortName'], str):
                    dic.update({'shortName': [protein['recommendedName']['shortName']]})

        if 'alternativeName' in protein.keys() and isinstance(protein['alternativeName'], dict):
            if isinstance(protein['alternativeName']['fullName'], str):
                dic.update({'alternativeName': [protein['alternativeName']['fullName']]})

            elif isinstance(protein['alternativeName']['fullName'], dict):
                dic.update({'alternativeName': [protein['alternativeName']['fullName']['text']]})

            elif isinstance(protein['alternativeName']['fullName'], list):
                bb = []
                for i in protein['alternativeName']['fullName']:
                    if isinstance(i, str):
                        bb.append(i)
                    elif isinstance(i, dict):
                        bb.append(i['text'])
                dic.update({'alternativeName': bb})

        if 'alternativeName' in protein.keys() and isinstance(protein['alternativeName'], list):
            cc = []
            for i in protein['alternativeName']:
                if isinstance(i['fullName'], str):
                    cc.append(i['fullName'])
                elif isinstance(i['fullName'], dict):
                    cc.append(i['fullName']['text'])
            dic.update({'alternativeName': cc})
        return dic

    def __updatesequence(self, f):
        if 'text' in f['sequence'].keys():
            sequence = f['sequence']['text']
            return sequence

    def __handle_artist(self, _, artist):
        config = configparser.ConfigParser()
        cfgfile = '../conf/drugkb.config'
        config.read(cfgfile)
        db = DBconnection(cfgfile, config.get('uniprot', 'db_name'),
                          config.get('uniprot', 'col_name_1'))
        res = self.__remover(artist)
        if isinstance(res, dict):
            uniprot.append(res)
            db.collection.insert_one(res)
            print(res)
        return True

    def __main(self, infile, save_path):
        with open(infile, 'rb') as inf:
            print("point 01")
            xmltodict.parse(inf, item_depth=2,
                            item_callback=self.__handle_artist)
        print('test')
        self.__check_path(save_path)
        with open(save_path + 'TrEMBL.json', "w") as file_out:
            json.dump(uniprot, file_out, indent=4)

    def start(self):
        config = configparser.ConfigParser()
        cfgfile = '../conf/drugkb.config'
        config.read(cfgfile)
        infile = config.get('uniprot', 'data_path_1')
        save_path = config.get('uniprot', 'json_path_1')
        print(next(os.walk(infile)))
        for root, dirs, files in os.walk(infile):
            # root 表示当前正在访问的文件夹路径
            # dirs 表示该文件夹下的子目录名list
            # files 表示该文件夹下的文件list

            # 遍历文件
            for f in files:
                fi = os.path.join(root, f)
                self.__main(fi, save_path)


class Uniprot_Parser_Factory():
    @classmethod
    def create_Uniprot_Parser(self):
        return Uniprot_Parser()


if __name__ == '__main__':
    uniprot_parser = Uniprot_Parser_Factory.create_Uniprot_Parser()
    uniprot_parser.start()
