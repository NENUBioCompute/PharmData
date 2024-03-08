
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

KEYS = {}
def handleKey(fo, line):
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
            KEYS[key] = otherkeys
        if '----------------------' in line or '____________' in line: break
    return fo, dic, IDField

def ttdTxt2Dict(fin):
    '''
    :param fin:
    :return: dict
    '''
    with open(fin, 'r') as fo:
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
                fo, dic, IDField = handleKey(fo, line)
                secElem = copy.deepcopy(dic)
                commentline += 1
                continue
            if commentline < 2: continue
            if '\t' in line:
                line = line.replace('\n', '')
                if line.replace('\t', '') == '': continue
                elem = line.split('\t')
                key = elem[1]
                # if elem[0] == elem[2] and secElem!=dic and IDField==key:
                if IDField == key and secElem != dic:
                    yield check_dic(secElem, fin.replace('.', '/').split('/')[-2])
                    # yield check_dic(copy.deepcopy(secElem))
                    secElem = copy.deepcopy(dic)
                    # secElem['_id'] = elem[2]

                current_section = elem[1]
                if 'value' in secElem[current_section].keys():
                    if isinstance(secElem[current_section]['value'], str):
                        secElem[current_section]['value'] = []
                    secElem[current_section]['value'].append(elem[2].strip())
                else:
                    secElem[current_section]['value'] = elem[2].strip()
                if len(elem) > 3:
                    for i in range(3, len(KEYS[key]) + 3):
                        try:
                            secElem[key][KEYS[key][i - 3]] = elem[i].strip()
                        except Exception as e:
                            print(elem, e, secElem)

def ttdTxt2Dict_2(fin):
    '''
    for crossmatcing.tsv
    :param fin:
    :return: dict
    '''
    with open(fin, 'r') as fo:
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
                fo, dic, IDField = handleKey(fo, line)
                secElem = copy.deepcopy(dic)
                commentline += 1
                continue
            if commentline < 2: continue
            if '\t' in line:
                line = line.replace('\n', '')
                if line.replace('\t', '') == '': continue
                elem = line.split('\t')
                key = elem[1]
                # if elem[0] == elem[2] and secElem!=dic and IDField==key:
                if IDField == key and secElem != dic:
                    yield check_dic(secElem, fin.replace('.', '/').split('/')[-2])
                    # yield check_dic(copy.deepcopy(secElem))
                    secElem = copy.deepcopy(dic)
                    # secElem['_id'] = elem[2]
                    secElem['ChEBI_ID'] = secElem.pop('CHEBI_ID')
                current_section = elem[1]
                if 'value' in secElem[current_section].keys():
                    if isinstance(secElem[current_section]['value'], str):
                        secElem[current_section]['value'] = []
                    secElem[current_section]['value'].append(elem[2].strip())
                else:
                    secElem[current_section]['value'] = elem[2].strip()
                if len(elem) > 3:
                    for i in range(3, len(KEYS[key]) + 3):
                        try:
                            secElem[key][KEYS[key][i - 3]] = elem[i].strip()
                        except Exception as e:
                            print(elem, e, secElem)
    def read_abbr(tsv_file_name):
        """
        Read the Abbreviation section in the. tsv file
        @param tsv_file_name:
        @return: fp, dic of row_title and row_content(stored in dictionary form
        """
        fp = open(tsv_file_name, 'r', encoding='UTF-8')
        # row_num = 0
        interval_num = 0  # Marks "------" or "_____" The number of entries encountered when reading the current file
        certain_title = ''
        certain_content = ''
        row_title = []  # Some elements are initialized for verification
        row_content = []
        dic = {}
        line = fp.readline()
        row_num = 1
        # row_title = line.strip().split("\t")  # Organize titles into lists
        while line:
            line = fp.readline()
            row_num = row_num + 1
            # if interval_num > 1:
            #     break
            if '\n' == line:
                continue
            elif '-' * 5 in line or '_' * 5 in line:
                interval_num = interval_num + 1
                # print(row_num)
            elif interval_num == 0:
                continue
            elif interval_num < 2:
                if 'Abbreviation' in line:
                    continue
                elif '\t' in line:
                    certain_title = line.strip().split('\t')[0]
                    # print(certain_title)  # Index part title read from test output
                    certain_content = line.strip().split('\t')[1]
                    # print(certain_content)  # Index part title read from test output
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
def read_abbr(tsv_file_name):
        """
        Read the Abbreviation section in the. tsv file
        @param tsv_file_name:
        @return: fp, dic of row_title and row_content(stored in dictionary form
        """
        fp = open(tsv_file_name, 'r', encoding='UTF-8')
        # row_num = 0
        interval_num = 0  # Marks "------" or "_____" The number of entries encountered when reading the current file
        certain_title = ''
        certain_content = ''
        row_title = []  # Some elements are initialized for verification
        row_content = []
        dic = {}
        line = fp.readline()
        row_num = 1
        # row_title = line.strip().split("\t")  # Organize titles into lists
        while line:
            line = fp.readline()
            row_num = row_num + 1
            # if interval_num > 1:
            #     break
            if '\n' == line:
                continue
            elif '-' * 5 in line or '_' * 5 in line:
                interval_num = interval_num + 1
                # print(row_num)
            elif interval_num == 0:
                continue
            elif interval_num < 2:
                if 'Abbreviation' in line:
                    continue
                elif '\t' in line:
                    certain_title = line.strip().split('\t')[0]
                    # print(certain_title)  # Index part title read from test output
                    certain_content = line.strip().split('\t')[1]
                    # print(certain_content)  # Index part title read from test output
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
        # print(row_title)  # Verify title list
        # print(row_content)  # Validate Data Rows
        # print(row_num)  # What line is currently read in the printout
        # print(interval_num)
        # print(dic_all)
        # print(dic)
        return fp, dic
def tsv_to_json6(tsv_file_name, json_file_name):
    """
    # drug2disease.tsv parsing
    @param tsv_file_name:
    @param json_file_name:
    @return:
    """
    with open(tsv_file_name, 'r', encoding='UTF-8') as fk:
        fk, dic = read_abbr(tsv_file_name)
        # print(dic)  # Test dictionary content
        dic_all = []
        dic2 = {}
        value_list = []
        line = fk.readline()
        head = ["Disease entry", "ICD-11", "Clinical status"]
        while line:
            line = fk.readline()

            if line == '\n' or line == '' or line=='	\n':
                pass
            else:
                key = line.strip().split('\t')[0]
                des = dic[key]
                if des == "Indication":  # Solve multiple values in value
                    true_value = line.strip().split('\t')[1]
                    one = (true_value.split(": ")[0]).split(" [")[0]
                    two = (true_value.split(": ")[1]).split("]")[0]
                    three = (true_value.split(": ")[1]).split("] ")[1]
                    value = [one, two, three]
                else:
                    value = line.strip().split('\t')[1]
                if des == "Indication":
                    dic2[key] = {}
                    dic2[key]['des'] = des
                    dic2[key]['value'] = {}
                    row_splice = zip(head, value)  # Match two elements one by one
                    dic3 = dict(
                        (ky, val) for ky, val in row_splice)  # Convert the list in the previous row to dictionary form
                    dic2[key]['value'] = value_list
                    value_list.append(dic3)
                else:
                    dic2[key] = {}
                    dic2[key]['des'] = des
                    dic2[key]['value'] = value
            if line == '\n' or line == ''or line=='	\n':  # Here we judge the segmentation, that is, data with different IDs
                elem = copy.deepcopy(dic2)
                dic_all.append(elem)
                dic2.clear()
                value_list.clear()
        # print(dic_all)
    with open(json_file_name, 'w', encoding='utf-8') as fp:
        json.dump(dic_all, fp,indent=4)

def excel2dict(fin_csv, table='target2drug'):
    '''
    1.stroe csv file to mongodb
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
        check_dic_targetName('Target_Name', table, v)
        yield v


def csv2dict(fin_csv, sep='\t', skiprows=0):
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
        part_dic(v)
        yield v


def check_dic(dic, table):
    # if dic['TARGETID']['value']=='T00033':
    #     print()
    for key in list(dic.keys()):
        if key == '_id': continue
        if 'value' not in dic[key].keys():
            del dic[key]
        else:
            check_dic_targetName(key, table, dic)
            check_dic_CHEBI_ID(key, table, dic)
            if ';' in dic[key]['value']: dic[key]['value'] = [x.strip() for x in dic[key]['value'].split(';')]
            # if isinstance(dic[key]['value'],str):dic[key]['value']=dic[key]['value'].strip()
            # if key in ['UNIPROID','GENENAME']:
            if key in ['UNIPROID']:
                if '-' in dic[key]['value'] or '/' in dic[key]['value']:
                    dic[key]['value'] = dic[key]['value'].replace('/', '-')
                    dic[key]['value'] = [x.strip() for x in dic[key]['value'].split('-')]
                    key = 'GENENAME'
                    if '-' in dic[key]['value'] and ' ' not in dic[key]['value']:
                        dic[key]['value'] = dic[key]['value'].replace('/', '-')
                        dic[key]['value'] = [x.strip() for x in dic[key]['value'].split('-')]
                        # print(dic['TARGETID']['value'],dic[key]['value'],dic['UNIPROID']['value'])

                # if '/' in dic[key]['value']:
                #     dic[key]['value']=[x.strip() for x in dic[key]['value'].split('/')]
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

                # global count
                # count += 1
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


def check_dic_targetName(key, table, dic):
    # Fibroblast growth factor receptor 1 (FGFR1) == > [Fibroblast growth factor receptor 1, FGFR1]
    for t, k in [('target', 'TARGNAME'), ('target2disease', 'TARGNAME'), ('target2drug', 'Target_Name')]:
        if table == t and k == key:
            x = dic[key]['value'] if t != 'target2drug' else dic[key]
            if '(' in x and ')' in x:
                vs = [
                    x[:x.index('(')].strip(),
                    x[x.index('(') + 1:x.index(')')].strip()
                ]
            if t == 'target2drug':
                dic[key] = vs
            else:
                dic[key]['value'] = vs

def check_dic_CHEBI_ID(key, table, dic):
    if table != 'crossmatching': return
    if key != 'CHEBI_ID': return
    if ':' in dic[key]['value']: dic[key]['value'] = dic[key]['value'].split(':')[1]
    # print(dic[key]['value'])


def part_dic(dic):
    for key in dic:
        if key in ['ICD11', 'ICD10', 'ICD9']:
            if ':' in dic[key]:
                dic[key] = dic[key].split(':')[1].strip()
                if '-' in dic[key]:
                    dic[key] = [x.strip() for x in dic[key].split('-')]
                if ',' in dic[key]:
                    dic[key] = [x.strip() for x in dic[key].split(',')]


def toJson(dics, fout_json):
    with open(fout_json, 'w') as json_f:
        json.dump(dics, json_f, indent=4)


def parse(cfgfile='../conf/drugkb.config'):
    config = configparser.ConfigParser()
    # cfgfile = '../../../conf/drugkb.config'
    config.read(cfgfile)
    section = 'ttd'
    tables = config.get(section, 'tables')[1:-1].split(',')
    for idx in range(4, int(config.get(section, 'col_num'))-5):
        fin_csv = os.path.join(config.get(section, 'data_path_%d' % (idx + 1)))
        fout_json = os.path.join(config.get(section, 'json_path_%d' % (idx + 1)))
        # if '/crossmatching.json' not in fout_json:continue
        print(idx, fin_csv, fout_json)
        if idx == 2:
            dics = [x for x in ttdTxt2Dict_2(fin_csv)]

        elif idx ==4:
            tsv_to_json6(fin_csv, fout_json)
            # dics = [x for x in tsv_to_json6(fin_csv,fout_json)]
            # for i in ttdTxt2Dict_3(fin_csv):
            #     print(i)
            break
        elif idx == 6:  # xlsx  col_name_6 = source_ttd_target2drug
            dics = [x for x in excel2dict(fin_csv, tables)]
        elif idx == 7:
            dics = [x for x in csv2dict(fin_csv, skiprows=15)]
        elif idx in [8, 9]:
            dics = [x for x in csv2dict(fin_csv, skiprows=17)]
        else:
            dics = [x for x in ttdTxt2Dict(fin_csv)]
        toJson(dics, fout_json)


if __name__ == '__main__':
    print('start', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    start = time.time()
    count = 0

    parse()
    # print(count)
    print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('time', time.time() - start)
