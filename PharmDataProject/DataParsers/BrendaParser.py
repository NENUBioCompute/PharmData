# @Author :F
# @Time :2022/10/30
import os
import json
import configparser
from PharmDataProject.Utilities.FileDealers import FileSystem
import re
import sys


class BRENDAParser:

    def __init__(self):
        pass

    def brendaTxt2Dict(self, fin):
        infos = []
        secElem = {}
        with open(fin, 'r') as fo:
            current_section = ''
            seq = ''
            line = fo.readline()
            while (line):
                if '///' in line and secElem != {}:
                    if current_section != '':
                        if current_section not in secElem.keys():
                            secElem[current_section] = []
                        secElem[current_section].append(seq.replace('\n', ' ').strip())
                    yield secElem
                    secElem = {}
                    current_section = ''
                    seq = ''
                elif '\t' in line[:6]:
                    secs = line.split('\t', 1)
                    section = secs[0]
                    if section != '':
                        if current_section != '':
                            if current_section == 'ID':
                                secElem['ID'] = seq.replace('\n', ' ').strip()
                                pass
                            elif current_section in ['RN', 'SN']:
                                secElem[current_section] = seq.replace('\n', ' ').strip()
                            else:
                                if current_section not in list(secElem.keys()):
                                    secElem[current_section] = []
                                secElem[current_section].append(seq.replace('\n', ' ').strip())
                        current_section = section
                        seq = secs[1]
                    else:
                        seq += secs[1]
                line = fo.readline()

    def support_handleComment(self, comment):
        commentary = {}
        commentary['PR'], commentary['commentary'], commentary['RF'] = self.re_match(comment)
        return commentary

    def support_handelRE(self, item, dic, subkey='commentary'):
        if '(#' in item:
            commentaryList = []
            if not ('(#' in item and '>)' in item):
                return item, dic
            commentStr = item[item.index('(#') + 1:item[item.index('(#') + 1:].index('>)') + 1 + item.index('(#') + 1]
            if '>;' in commentStr:
                commentStr = commentStr.replace('>;', '>')
                for com in commentStr.split('>'):
                    if com == '': continue
                    com = '%s>' % com.strip()
                    commentaryList.append(self.support_handleComment(com))
                dic[subkey] = commentaryList
            else:
                dic[subkey] = self.support_handleComment(commentStr)
            item = item.replace(item[item.index('(#'):item.index('>)') + 2], '')
        return item, dic

    def support_handelNSP(self, item, dic, subkey='products'):
        if '|#' in item:
            commentaryList = []
            commentStr = item[item.index('|#') + 1:item.index('>|') + 1]
            if '>;' in commentStr:
                commentStr = commentStr.replace('>;', '>')
                for com in commentStr.split('>'):
                    if com == '': continue
                    com = '%s>' % com.strip()
                    commentaryList.append(self.support_handleComment(com))
                dic[subkey] = commentaryList
            else:
                dic[subkey] = self.support_handleComment(commentStr)
            item = item.replace(item[item.index('|#'):item.index('>|') + 2], '')
        return item, dic

    def handleNSP(self, elem, key='NSP', subkey='des'):
        for idx, item in enumerate(elem[key]):
            dic = {}
            dic['substrates'] = {}
            dic['products'] = {}
            item, dic['substrates'] = self.support_handelRE(item, dic['substrates'])  # handle (#...>)
            item, dic['products'] = self.support_handelNSP(item, dic['products'])  # handle |#...>|
            dic['PR'], dic[subkey], dic['RF'] = self.re_match(item)  # handle #...#...<>
            if '{' in dic[subkey] and '}' in dic[subkey]:  # handle '{...}'
                reversibilty = dic[subkey][dic[subkey].index('{') + 1:dic[subkey].index('}')]
                dic['reversibilty'] = reversibilty
                dic[subkey] = dic[subkey].replace('{%s}' % reversibilty, '').strip()
            if ' =' in dic[subkey]: dic['substrates']['des'], dic['products']['des'] = dic[subkey].split(' =', 1)
            elem[key][idx] = dic
        return elem

    def handlePR(self, elem):
        term = elem['PR']
        for idx, item in enumerate(term):
            dic = {}
            item, dic = self.support_handelRE(item, dic)
            # result = re.search('#(\d+)#(.*?)<(.*?)>', item)
            dic['id'], dic['organism'], dic['RF'] = self.re_match(item)
            dic['organism'] = dic['organism'].strip()
            if 'UniProt' in dic['organism']:
                result = re.search('(.*?)(\S+) UniProt', dic['organism'])
                if result: dic['organism'], dic['UniProt'] = result.groups()
            elif 'SwissProt' in dic['organism']:
                result = re.search('(.*?)(\S+) SwissProt', dic['organism'])
                if result: dic['organism'], dic['SwissProt'] = result.groups()
            dic['organism'] = dic['organism'].strip()
            if ',' in dic['RF']:
                dic['RF'] = dic['RF'].split(',')
            elem['PR'][idx] = dic
        return elem

    def handleTN(self, elem, key='TN', subkey='des', infokey='substrate'):
        for idx, item in enumerate(elem[key]):
            dic = {}
            item, dic = self.support_handelRE(item, dic)
            dic['PR'], dic[subkey], dic['RF'] = self.re_match(item)
            if '{' in dic[subkey] and '}' in dic[subkey]:
                reversibilty = dic[subkey][dic[subkey].index('{') + 1:dic[subkey].index('}')]
                dic[infokey] = reversibilty
                dic[subkey] = dic[subkey].replace('{%s}' % reversibilty, '').strip()
            elem[key][idx] = self.check_dic(dic)
        return elem

    def handleID(self, elem):
        str = elem['ID']
        if '(' in str:
            elem['ID'] = re.search('(\d+\.\d+\.\d+\.\d+)', str).groups()[0]
            elem['ID'] = {}
            elem['ID']['id'] = re.findall('(\d+\.\d+\.\d+\.\d+)', str)
            elem['ID']['des'] = str
        return elem

    def check_dic(self, dic):
        for key in list(dic.keys()):
            if dic[key] == '': del dic[key]
        return dic

    def re_match(self, str):
        PR = RF = des = ''
        if '#' == str[0] and '>' == str[-1]:
            result = re.search('#(.*?)#(.*)<(.*?)>', str)
            if result: PR, des, RF = result.groups()
        elif '#' == str[0] and '>' != str[-1]:
            PR, des = re.search('#(.*?)#(.*?)', str).groups()
        elif '#' != str[0]:
            des = str
        if ',' in PR: PR = PR.split(',')
        if ',' in RF: RF = RF.split(',')
        des = des.strip()
        return PR, des, RF

    def doParse(self, fin):
        for elem in self.brendaTxt2Dict(fin):
            try:
                for key in list(elem.keys()):
                    if key in ['ID', 'RN', 'SN']:
                        if key == 'ID': elem = self.handleID(elem)
                    elif key in ['PR']:
                        elem = self.handlePR(elem)
                    elif key in ['NSP', 'SP']:
                        elem = self.handleNSP(elem, key=key, subkey='des')
                    elif key in ['KI']:
                        self.handleTN(elem, key=key, infokey='inhibitor')
                    else:
                        elem = self.handleTN(elem, key=key)
            except:
                print('wrong when it comes to ', elem['ID'], key)
                sys.exit()
            yield elem

    def parse(self):
        config = configparser.ConfigParser()
        cfgfile = '../conf/drugkb.config'
        config.read(cfgfile)
        fin_txt = os.path.join(config.get('brenda', 'data_path_1').replace('.tar.gz', ''))
        fout_json = os.path.join(config.get('brenda', 'json_path_1'))
        dics = [x for x in self.doParse(fin_txt)]
        get_dict = {}
        for i in range(len(dics)):
            get_dict[i] = dics[i]
        with open(fout_json, 'w') as json_f:
            json.dump(get_dict, json_f, indent=4)


if __name__ == '__main__':
    d = BRENDAParser()
    d.parse()
