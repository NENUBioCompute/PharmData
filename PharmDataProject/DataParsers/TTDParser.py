# Title     : parse.py
# Created by: julse@qq.com
# Created on: 2021/6/23 14:00
import configparser
import copy
import time
import os
import json
import sys
import pandas as pd


class TtdParser:
    def __init__(self, config_path='../conf/drugkb_test.config'):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.section = 'ttd'
        self.tables = self.config.get(self.section, 'tables')[1:-1].split(',')
        self.data_paths = [self.config.get(self.section, f'data_path_{i + 1}') for i in
                           range(int(self.config.get(self.section, 'col_num')))]
        self.KEYS = {}

    def handleKey(self, fo, line):
        dic = {}
        IDField = ''
        while (line):
            line = fo.readline()
            if '\t' in line:
                elem = line[:-1].split('\t')
                key = elem[0]
                if IDField == '':
                    IDField = key
                dic[key] = {}
                otherkeys = []
                for idx, el in enumerate(elem):
                    if idx == 0:
                        continue
                    elif idx == 1:
                        dic[key]['des'] = elem[idx]
                    else:
                        if elem[idx] != '':
                            otherkeys.append(elem[idx])
                self.KEYS[key] = otherkeys
            if '----------------------' in line or '____________' in line: break
        return fo, dic, IDField

    def ttdTxt2Dict(self, fin):
        '''
        :param fin:
        :return: dict
        '''
        with open(fin, 'r', encoding='utf-8') as fo:
            current_section = ''
            seq = ''
            line = fo.readline()
            commentline = 0
            dic = {}
            secElem = {}
            IDField = ''
            while (line):
                line = fo.readline()
                if '\n' == line: continue
                if '----------------------' in line or '____________' in line:
                    commentline += 1
                    continue
                if commentline == 1:
                    fo, dic, IDField = self.handleKey(fo, line)
                    secElem = copy.deepcopy(dic)
                    commentline += 1
                    continue
                if commentline < 2: continue
                if '\t' in line:
                    line = line.replace('\n', '')
                    if line.replace('\t', '') == '': continue
                    elem = line.split('\t')
                    key = elem[1]
                    if IDField == key and secElem != dic:
                        secElem = self.filter_empty_description(secElem)
                        yield self.check_dic(secElem, fin.replace('.', '/').split('/')[-2])
                        secElem = copy.deepcopy(dic)

                    current_section = elem[1]
                    if 'value' in secElem[current_section].keys():
                        if isinstance(secElem[current_section]['value'], str):
                            secElem[current_section]['value'] = []
                        secElem[current_section]['value'].append(elem[2].strip())
                    else:
                        secElem[current_section]['value'] = elem[2].strip()
                    if len(elem) > 3:
                        for i in range(3, len(self.KEYS[key]) + 3):
                            try:
                                secElem[key][self.KEYS[key][i - 3]] = elem[i].strip()
                            except Exception as e:
                                print(elem, e, secElem)
            if secElem:  # Ensure the last section is yielded
                secElem = self.filter_empty_description(secElem)
                yield self.check_dic(secElem, fin.replace('.', '/').split('/')[-2])

    def filter_empty_description(self, dic):
        """Filter out entries with empty descriptions."""
        return {k: v for k, v in dic.items() if v.get('des')}

    def ttdTxt2Dict_2(self, fin):
        '''
        for crossmatcing.tsv
        :param fin:
        :return: dict
        '''
        with open(fin, 'r', encoding='utf-8') as fo:
            current_section = ''
            seq = ''
            line = fo.readline()
            commentline = 0
            dic = {}
            secElem = {}
            IDField = ''
            while line:
                line = fo.readline()
                if '\n' == line:
                    continue
                if '----------------------' in line or '____________' in line:
                    commentline += 1
                    continue
                if commentline == 1:
                    fo, dic, IDField = self.handleKey(fo, line)
                    secElem = copy.deepcopy(dic)
                    commentline += 1
                    continue
                if commentline < 2:
                    continue
                if '\t' in line:
                    line = line.replace('\n', '')
                    if line.replace('\t', '') == '':
                        continue
                    elem = line.split('\t')
                    key = elem[1]
                    if IDField == key and secElem != dic:
                        yield self.check_dic(secElem, fin.replace('.', '/').split('/')[-2])
                        secElem = copy.deepcopy(dic)
                        # 初始化 ChEBI_ID 键
                        secElem['ChEBI_ID'] = secElem.pop('CHEBI_ID', {'des': '', 'value': ''})
                    current_section = elem[1]
                    if current_section not in secElem:
                        secElem[current_section] = {'value': ''}
                    if 'value' in secElem[current_section]:
                        if isinstance(secElem[current_section]['value'], str):
                            secElem[current_section]['value'] = []
                        secElem[current_section]['value'].append(elem[2].strip())
                    else:
                        secElem[current_section]['value'] = elem[2].strip()
                    if len(elem) > 3:
                        for i in range(3, len(self.KEYS.get(key, [])) + 3):
                            try:
                                if key not in secElem:
                                    secElem[key] = {}
                                secElem[key][self.KEYS[key][i - 3]] = elem[i].strip()
                            except Exception as e:
                                print(elem, e, secElem)
            if secElem:
                yield self.check_dic(secElem, fin.replace('.', '/').split('/')[-2])

    def read_abbr(self, tsv_file_name):
        """
        Read the Abbreviation section in the. tsv file
        @param tsv_file_name:
        @return: fp, dic of row_title and row_content(stored in dictionary form
        """
        fp = open(tsv_file_name, 'r', encoding='UTF-8')
        interval_num = 0  # Marks "------" or "_____" The number of entries encountered when reading the current file
        certain_title = ''
        certain_content = ''
        row_title = []  # Some elements are initialized for verification
        row_content = []
        dic = {}
        line = fp.readline()
        row_num = 1
        while line:
            line = fp.readline()
            row_num = row_num + 1
            if '\n' == line:
                continue
            elif '-' * 5 in line or '_' * 5 in line:
                interval_num = interval_num + 1
            elif interval_num == 0:
                continue
            elif interval_num < 2:
                if 'Abbreviation' in line:
                    continue
                elif '\t' in line:
                    certain_title = line.strip().split('\t')[0]
                    certain_content = line.strip().split('\t')[1]
                    row_title.append(certain_title)
                    for index, value in enumerate(row_title):
                        if value == 'CHEBI_ID':
                            row_title[index] = 'ChEBI_ID'
                    row_content.append(certain_content)
                else:
                    pass
            else:
                pass
            if interval_num == 2:
                break
        row_splice = zip(row_title, row_content)  # Match two elements one by one
        dic = dict((key, value) for key, value in row_splice)  # Convert the list in the previous row to dictionary form
        return fp, dic

    def tsv_to_dict_generator(self, tsv_file_name):
        """
        Parse drug2disease.tsv and return a generator of dictionaries.
        @param tsv_file_name: The TSV file name.
        @return: A generator of dictionaries.
        """
        with open(tsv_file_name, 'r', encoding='UTF-8') as fk:
            fk, dic = self.read_abbr(tsv_file_name)
            value_list = []
            dic2 = {}
            line = fk.readline()
            head = ["Disease entry", "ICD-11", "Clinical status"]

            while line:
                line = fk.readline()

                if line == '\n' or line == '' or line == '\t\n':
                    if dic2:  # Check if dic2 is not empty
                        yield copy.deepcopy(dic2)
                    dic2.clear()
                    value_list.clear()
                    continue
                else:
                    key = line.strip().split('\t')[0]
                    des = dic.get(key, "")
                    if des == "Indication":  # Solve multiple values in value
                        true_value = line.strip().split('\t')[1] if len(line.strip().split('\t')) > 1 else ""
                        one = (true_value.split(": ")[0]).split(" [")[0] if ": " in true_value else true_value
                        two = (true_value.split(": ")[1]).split("]")[0] if ": " in true_value and "]" in \
                                                                           true_value.split(": ")[1] else ""
                        three = (true_value.split(": ")[1]).split("] ")[1] if ": " in true_value and "] " in \
                                                                              true_value.split(": ")[1] else ""
                        value = [one, two, three]
                    else:
                        value = line.strip().split('\t')[1] if len(line.strip().split('\t')) > 1 else ""

                    if des == "Indication":
                        dic2[key] = {}
                        dic2[key]['des'] = des
                        dic2[key]['value'] = {}
                        row_splice = zip(head, value)  # Match two elements one by one
                        dic3 = dict(
                            (ky, val) for ky, val in
                            row_splice)  # Convert the list in the previous row to dictionary form
                        dic2[key]['value'] = value_list
                        value_list.append(dic3)
                    else:
                        dic2[key] = {}
                        dic2[key]['des'] = des
                        dic2[key]['value'] = value

            if dic2:  # Ensure the last segment is yielded
                yield copy.deepcopy(dic2)

    def excel2dict(self, fin_csv, table='target2drug'):
        '''
        1.store csv file to mongodb
        2.replace ' ' to '_',replace '. to '__'
        3.create index

        :param fin_csv:
        :param sep: \t, , and so on
        :return:
        '''
        df = pd.read_excel(fin_csv)
        titles = []
        for head in df.columns:
            titles.append(head.replace(' ', '-').replace('.', '--'))
        df.columns = titles
        t = df.T.to_dict()
        for v in t.values():
            # Ensure 'Target_Name' key exists before calling check_dic_targetName
            if 'Target_Name' in v:
                self.check_dic_targetName('Target_Name', table, v)
            yield v

    def csv2dict(self, fin_csv, sep='\t', skiprows=0):
        '''
        1.stroe csv file to mongodb
        2.replace ' ' to '_',replace '. to '__'
        3.create index

        :param fin_csv:
        :param sep: \t, , and so on
        :return:
        '''
        df = pd.read_csv(fin_csv, sep=sep, encoding='utf-8', skiprows=skiprows)
        titles = []
        for head in df.columns:
            titles.append(head.replace(' ', '-').replace('.', '--'))
        df.columns = titles
        t = df.T.to_dict()
        for v in t.values():
            self.part_dic(v)
            yield v

    def check_dic(self, dic, table):
        for key in list(dic.keys()):
            if key == '_id':
                continue
            # 如果 'value' 键不存在，设置一个默认的空值
            if 'value' not in dic[key]:
                dic[key]['value'] = ""

            self.check_dic_targetName(key, table, dic)
            self.check_dic_CHEBI_ID(key, table, dic)

            if isinstance(dic[key]['value'], str) and ';' in dic[key]['value']:
                dic[key]['value'] = [x.strip() for x in dic[key]['value'].split(';')]

            if key in ['UNIPROID']:
                if isinstance(dic[key]['value'], str):
                    if '-' in dic[key]['value'] or '/' in dic[key]['value']:
                        dic[key]['value'] = dic[key]['value'].replace('/', '-')
                        dic[key]['value'] = [x.strip() for x in dic[key]['value'].split('-')]
                    key = 'GENENAME'
                    value = dic.get(key, {}).get('value', "")
                    if '-' in value and ' ' not in value:  # 现在使用变量value而不是直接访问dic[key]['value']
                        dic[key]['value'] = dic[key]['value'].replace('/', '-')
                        dic[key]['value'] = [x.strip() for x in dic[key]['value'].split('-')]

            if key == 'INDICATI' and 'TARGETID' in dic:
                if 'Disease Entry [ICD-11]' in dic[key]:
                    if '[ICD-11' in dic[key]['Disease Entry [ICD-11]']:
                        elem = {}
                        elem['id'] = dic[key]['Disease Entry [ICD-11]'][
                                     dic[key]['Disease Entry [ICD-11]'].index('[ICD-11'):]
                        elem['name'] = dic[key]['Disease Entry [ICD-11]'].replace(elem['id'], '').strip()
                        elem['id'] = elem['id'][8:-1].strip()
                        if '-' in elem['id'] or ',' in elem['id']:
                            elem['id'] = elem['id'].replace(',', '-')
                            elem['id'] = [x.strip() for x in elem['id'].split('-')]
                        if '/' in elem['name']:
                            elem['name'] = [x.strip() for x in elem['name'].split('/')]
                        dic[key]['Disease Entry [ICD-11]'] = elem

            # drug2disease
            if key == 'INDICATI' and 'TTDDRUID' in dic:
                if isinstance(dic[key]['value'], list):
                    for x in dic[key]['value']:
                        if '[ICD-11:' in x:
                            dic[key]['value'] = {}
                            dic[key]['value']['name'] = x.split('[', 1)[0]
                            dic[key]['value']['ICD-11'] = x[x.index('[ICD-11:') + 8:x.index(']')].strip()
                            dic[key]['value']['state'] = x.split(']', 1)[1].strip()
                else:
                    x = dic[key]['value']
                    dic[key]['value'] = {}
                    dic[key]['value']['name'] = x.split('[', 1)[0]
                    dic[key]['value']['ICD-11'] = x[x.index('[ICD-11:') + 8:x.index(']')].strip()
                    dic[key]['value']['state'] = x.split(']', 1)[1].strip()
                    if '-' in dic[key]['value']['ICD-11']:
                        dic[key]['value']['ICD-11'] = [x.strip() for x in dic[key]['value']['ICD-11'].split('-')]
                    elif ',' in dic[key]['value']['ICD-11']:
                        dic[key]['value']['ICD-11'] = [x.strip() for x in dic[key]['value']['ICD-11'].split(',')]
                    if '/' in dic[key]['value']['name']:
                        dic[key]['value']['name'] = [x.strip() for x in dic[key]['value']['name'].split('/')]

            if key in ['KEGGPATH', 'WIKIPATH', 'NET_PATH', 'INTEPATH', 'PANTPATH', 'REACPATH', 'WHIZPATH']:
                if isinstance(dic[key]['value'], list):
                    for idx, x in enumerate(dic[key]['value']):
                        if ':' not in x: continue
                        dic[key]['value'][idx] = {}
                        dic[key]['value'][idx]['id'], dic[key]['value'][idx]['name'] = x.split(':', 1)
                else:
                    x = dic[key]['value']
                    if ':' in x:
                        dic[key]['value'] = {}
                        dic[key]['value']['id'], dic[key]['value']['name'] = x.split(':', 1)
        return dic

    def check_dic_targetName(self, key, table, dic):
        for t, k in [('target', 'TARGNAME'), ('target2disease', 'TARGNAME'), ('target2drug', 'Target_Name')]:
            if table == t and k == key:
                if key in dic:  # 确保键存在
                    x = dic[key]['value'] if t != 'target2drug' else dic[key]
                    if '(' in x and ')' in x:
                        vs = [
                            x[:x.index('(')].strip(),
                            x[x.index('(') + 1:x.index(')')].strip()
                        ]
                    else:
                        vs = [x.strip()] if isinstance(x, str) else x
                    if t == 'target2drug':
                        dic[key] = vs
                    else:
                        dic[key]['value'] = vs

    def check_dic_CHEBI_ID(self, key, table, dic):
        if table != 'crossmatching': return
        if key != 'CHEBI_ID': return
        if ':' in dic[key]['value']: dic[key]['value'] = dic[key]['value'].split(':')[1]

    def part_dic(self, dic):
        for key in dic:
            if key in ['ICD11', 'ICD10', 'ICD9']:
                if ':' in dic[key]:
                    dic[key] = dic[key].split(':')[1].strip()
                    if '-' in dic[key]:
                        dic[key] = [x.strip() for x in dic[key].split('-')]
                    if ',' in dic[key]:
                        dic[key] = [x.strip() for x in dic[key].split(',')]

    def parse(self, table, cfgfile='../conf/drugkb_test.config'):
        config = configparser.ConfigParser()
        config.read(cfgfile)
        section = 'ttd'
        tables = config.get(section, 'tables')[1:-1].split(',')
        data_paths = [config.get(section, f'data_path_{i + 1}') for i in range(len(tables))]

        if table not in tables:
            raise ValueError(f"Table {table} is not in the configuration.")

        idx = tables.index(table)
        fin_csv = os.path.join(config.get(section, f'data_path_{idx + 1}'))

        if idx == 2:
            return self.ttdTxt2Dict_2(fin_csv)
        elif idx == 4:
            return self.tsv_to_dict_generator(fin_csv)
        elif idx == 6:  # xlsx  col_name_6 = source_ttd_target2drug
            return self.excel2dict(fin_csv, table)
        elif idx == 7:
            return self.csv2dict(fin_csv, skiprows=15)
        elif idx in [8, 9]:
            return self.csv2dict(fin_csv, skiprows=17)
        else:
            return self.ttdTxt2Dict(fin_csv)


if __name__ == '__main__':
    ttd_parser = TtdParser()
    data_paths = ttd_parser.data_paths
    for idx, data_path in enumerate(data_paths):
        table_name = ttd_parser.tables[idx]
        print(f"正在解析表: {table_name} 文件: {data_path}")
        try:
            for parsed_data in ttd_parser.parse(table_name):
                print(parsed_data)
                break
        except ValueError as e:
            print(f"错误: {e}")