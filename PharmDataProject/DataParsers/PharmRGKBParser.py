# encoding: utf-8
'''
@Author  : Wenjie.Lee
@Time    : 2022/7/25 21:36
@desc    : pharmGKB parse
'''
import configparser
import os
import pandas as pd


class PharmGKBParser:
    def __parse(self, config):
        section = 'pharmgkb'
        tables = config.get(section, 'tables')[1:-1].split(',')
        parsed_data = {}
        for idx in range(int(config.get(section, 'col_num'))):
            if idx == 3:
                fin_csv = config.get(section, 'data_path_%d' % (idx + 1)).replace('.zip', '')
            else:
                fin_csv = os.path.join(config.get(section, 'data_path_%d' % (idx + 1)).replace('.zip', ''),
                                       '%s.tsv' % tables[idx])
            data = self.__csv2dict(fin_csv, sep='\t')
            parsed_data[tables[idx]] = data
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
        return t

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
        return self.__parse(config)


if __name__ == '__main__':
    pharmGKBParser = PharmGKBParser()
    config = configparser.ConfigParser()
    config.read('../conf/drugkb_test.config')
    parsed_data = pharmGKBParser.start(config)

    # 测试每个文件的第一个字典数据
    for table, data in parsed_data.items():
        if isinstance(data, dict):
            first_item = next(iter(data.values()))
        elif isinstance(data, list):
            first_item = data[0]
        else:
            first_item = None

        print(f"Table: {table}, First item: {first_item}")

        # 注释掉下面的 break 语句以解析每个文件夹中的所有字典
        # break

