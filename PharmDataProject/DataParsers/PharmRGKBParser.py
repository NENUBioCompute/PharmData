# encoding: utf-8
'''
@Author  : Wenjie.Lee
@Time    : 2022/7/25 21:36
@desc    : pharmGKB parse
'''
import configparser
import json
import os
import pandas as pd
import sys


class PharmGKBParser:

    def __parse(self, config):
        section = 'pharmgkb'
        tables = config.get(section, 'tables')[1:-1].split(',')
        for idx in range(int(config.get(section, 'col_num'))):
            if idx == 3:
                fin_csv = config.get(section, 'data_path_%d' % (idx + 1)).replace('.zip', '')
            else:
                fin_csv = os.path.join(config.get(section, 'data_path_%d' % (idx + 1)).replace('.zip', ''),
                                       '%s.tsv' % tables[idx])
            fout_json = os.path.join(config.get(section, 'json_path_%d' % (idx + 1)), '%s.json' % tables[idx])

            self.__csv2json(fin_csv, fout_json, sep='\t')

    def replace_nan_with_empty(self, data):
        for key, value in data.items():
            if isinstance(value, dict):
                self.replace_nan_with_empty(value)  # 递归处理嵌套的字典
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self.replace_nan_with_empty(item)  # 递归处理嵌套的字典
            elif pd.isna(value):  # 检查是否为 NaN
                data[key] = ''  # 将 NaN 替换为空值

    def __csv2json(self, fin_csv, fout_json, sep='\t'):
        '''
        1.stroe csv file to mongodb
        2.replace ' ' to '_',replace '. to '__'
        3.create index
        :param fin_csv:
        :param firstname: db name in mongodb
        :param secondname: table name in mongodb
        :param sep: \t , and so on
        :return:
        '''
        if '.tsv' in fin_csv:
            t = self.__parseCsv(fin_csv, sep=sep)
            # 将 NaN 替换为空字符串
            for key, value in t.items():
                t[key] = {k: v if pd.notna(v) else '' for k, v in value.items()}
        else:
            t = self.__parsePathway(fin_csv, sep=sep)
            # 将 NaN 替换为空字符串
            for i in t:
                self.replace_nan_with_empty(i)

        with open(fout_json, 'w') as json_f:
            json.dump(t, json_f, indent=4)

    def __parseCsv(self, fin_csv, sep='\t'):
        df = pd.read_csv(fin_csv, sep=sep)
        titles = []
        for head in df.columns:
            titles.append(head.replace(' ', '-').replace('.', '--'))
        df.columns = titles
        dicts = df.T.to_dict()
        return dicts

    def __result(self, dic):
        '''
        :param iter: csv parsed into one dict
        :return: list of the dict value
        '''
        for v in dic.values():
            yield v

    def __parsePathway(self, dirin, sep='\t'):
        dics = []
        for eachfile in os.listdir(dirin):
            if 'PA' not in eachfile: continue
            fin_csv = os.path.join(dirin, eachfile)
            df = pd.read_csv(fin_csv, sep=sep)
            t = df.T.to_dict()
            dic = {}
            dic['pathway_id'], dic['name'] = eachfile.split('-')
            dic['name'] = dic['name'].split('.')[0].replace('_', ' ')
            dic['content'] = []
            for x in self.__result(t):
                for key in x.keys():
                    if isinstance(x[key], str) and ',' in x[key]:
                        x[key] = x[key].split(',')
                dic['content'].append(x)
            dics.append(dic)
        return dics

    def start(self, config):
        self.__parse(config)


if __name__ == '__main__':
    pharmGKBParser = PharmGKBParser()
    config = configparser.ConfigParser()
    config.read('../conf/drugkb.config')
    pharmGKBParser.start(config)
