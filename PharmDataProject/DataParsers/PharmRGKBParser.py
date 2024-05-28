# encoding: utf-8
'''
@Author  : Wenjie.Lee
@Time    : 2022/7/25 21:36
@desc    : pharmGKB parse
'''

import configparser
import os
import pandas as pd

class PharmRGKBParser:
    def __init__(self, config_path='../conf/drugkb_test.config'):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.section = 'pharmgkb'
        self.tables = self.config.get(self.section, 'tables')[1:-1].split(',')
        self.data_paths = [self.config.get(self.section, f'data_path_{i + 1}') for i in range(int(self.config.get(self.section, 'col_num')))]

    def __parse(self):
        parsed_data = {}
        for idx, data_path in enumerate(self.data_paths):
            if idx == 3:
                data = self.__parsePathway(data_path, sep='\t')
            else:
                data = self.__parseFilesInDirectory(data_path, sep='\t')
            parsed_data[self.tables[idx]] = data
        return parsed_data

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

    def __csv2dict(self, fin_csv, sep='\t'):
        '''
        Parse CSV/TSV file to dictionary
        :param fin_csv: Input file path
        :param sep: Separator
        :return: Parsed dictionary
        '''
        df = pd.read_csv(fin_csv, sep=sep)
        titles = [head.replace(' ', '-').replace('.', '--') for head in df.columns]
        df.columns = titles
        dicts = df.T.to_dict()
        # 将 NaN 替换为空字符串
        for key, value in dicts.items():
            dicts[key] = {k: v if pd.notna(v) else '' for k, v in value.items()}
        return dicts

    def __result(self, dic):
        '''
        :param dic: csv parsed into one dict
        :return: list of the dict value
        '''
        for v in dic.values():
            yield v

    def __parsePathway(self, dirin, sep='\t'):
        dics = []
        for eachfile in os.listdir(dirin):
            if 'PA' not in eachfile:
                continue
            fin_csv = os.path.join(dirin, eachfile)
            df = pd.read_csv(fin_csv, sep=sep)
            t = df.T.to_dict()
            dic = {}
            dic['pathway_id'], dic['name'] = eachfile.split('-')[:2]
            dic['name'] = dic['name'].split('.')[0].replace('_', ' ')
            dic['content'] = []
            for x in self.__result(t):
                for key in x.keys():
                    if isinstance(x[key], str) and ',' in x[key]:
                        x[key] = x[key].split(',')
                dic['content'].append(x)
            dics.append(dic)
        return dics

    def __parseFilesInDirectory(self, dirin, sep='\t'):
        all_data = []
        for eachfile in os.listdir(dirin):
            if eachfile.endswith('.tsv'):
                fin_csv = os.path.join(dirin, eachfile)
                data = self.__csv2dict(fin_csv, sep)
                all_data.extend(data.values())
        return all_data

    def start(self):
        return self.__parse()


if __name__ == '__main__':
    pharmGKBParser = PharmRGKBParser()
    parsed_data = pharmGKBParser.start()

    # 测试每个文件的第一个字典数据
    for table, data in parsed_data.items():
        if isinstance(data, list) and data:
            first_item = data[0]
        elif isinstance(data, dict):
            first_item = next(iter(data.values()))
        else:
            first_item = None

        print(f"Table: {table}, First item: {first_item}")
