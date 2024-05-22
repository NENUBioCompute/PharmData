#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import csv
import os
import configparser
from tqdm import tqdm

class BiogridParser:
    def __init__(self):
        pass

    def parse(self, data_path):
        txt_file = os.path.join(data_path, "BIOGRID-ALL-4.4.226.mitab.txt")
        with open(txt_file, 'r', encoding='utf-8') as txtfile:
            file_list = txtfile.read().split('\n')
            bar = tqdm(total=len(file_list), desc='Parsing TXT')  # 加入进度条功能
            for row in file_list:
                if not row.strip():  # 跳过空行
                    continue
                ss = row.replace("\t", "!!")
                snew = ss.split("!!")
                each = {f'column_{i}': snew[i] for i in range(len(snew))}
                yield self.process_row(each)  # 使用生成器逐行返回字典
                bar.update()
            bar.close()

    def process_row(self, each):
        A = {}
        if "column_4" in each and each["column_4"] is not None:  # Alt IDs Interactor A
            if "|" in each["column_4"]:
                tag = 1
                for i in each["column_4"].split("|"):
                    if ":" not in i:
                        continue
                    if i.split(":")[0] in A.keys():
                        if tag == 1:
                            exp = []
                            exp.append(A[i.split(":")[0]])
                            tag = 0
                        else:
                            exp.append(i.split(":")[1])
                        A[i.split(":")[0]] = exp
                    else:
                        A[i.split(":")[0]] = i.split(":")[1]
        B = {}
        if "column_5" in each and each["column_5"] is not None:  # Alt IDs Interactor B
            if "|" in each["column_5"]:
                tag = 1
                for i in each["column_5"].split("|"):
                    if ":" not in i:
                        continue
                    if i.split(":")[0] in B.keys():
                        if tag == 1:
                            exp = []
                            exp.append(B[i.split(":")[0]])
                            tag = 0
                        else:
                            exp.append(i.split(":")[1])
                        B[i.split(":")[0]] = exp
                    else:
                        B[i.split(":")[0]] = i.split(":")[1]
        AA = {}
        if "column_6" in each and each["column_6"] is not None:  # Aliases Interactor A
            if "|" in each["column_6"]:
                tag = 1
                for i in each["column_6"].split("|"):
                    if ":" not in i:
                        continue
                    if i.split(":")[0] in AA.keys():
                        if tag == 1:
                            exp = []
                            exp.append(AA[i.split(":")[0]])
                            tag = 0
                        else:
                            exp.append(i.split(":")[1])
                        AA[i.split(":")[0]] = exp
                    else:
                        AA[i.split(":")[0]] = i.split(":")[1]
        BB = {}
        if "column_7" in each and each["column_7"] is not None:  # Aliases Interactor B
            if "|" in each["column_7"]:
                tag = 1
                for i in each["column_7"].split("|"):
                    if ":" not in i:
                        continue
                    if i.split(":")[0] in BB.keys():
                        if tag == 1:
                            exp = []
                            exp.append(BB[i.split(":")[0]])
                            tag = 0
                        else:
                            exp.append(i.split(":")[1])
                        BB[i.split(":")[0]] = exp
                    else:
                        BB[i.split(":")[0]] = i.split(":")[1]
        each["Alt IDs Interactor A"] = A
        each["Alt IDs Interactor B"] = B
        each["Aliases Interactor A"] = AA
        each["Aliases Interactor B"] = BB

        if "column_0" in each and each["column_0"] is not None:  # ID Interactor A
            if ":" in each["column_0"]:
                A1 = {}
                A1[each["column_0"].split(":")[0]] = each["column_0"].split(":")[1]
                each["ID Interactor A"] = A1

        if "column_1" in each and each["column_1"] is not None:  # ID Interactor B
            if ":" in each["column_1"]:
                A2 = {}
                A2[each["column_1"].split(":")[0]] = each["column_1"].split(":")[1]
                each["ID Interactor B"] = A2

        if "column_8" in each and each["column_8"] is not None:  # Publication Identifiers
            if ":" in each["column_8"]:
                A3 = {}
                A3[each["column_8"].split(":")[0]] = each["column_8"].split(":")[1]
                each["Publication Identifiers"] = A3

        if "column_9" in each and each["column_9"] is not None:  # Taxid Interactor A
            if ":" in each["column_9"]:
                A4 = {}
                A4[each["column_9"].split(":")[0]] = each["column_9"].split(":")[1]
                each["Taxid Interactor A"] = A4

        if "column_10" in each and each["column_10"] is not None:  # Taxid Interactor B
            if ":" in each["column_10"]:
                A5 = {}
                A5[each["column_10"].split(":")[0]] = each["column_10"].split(":")[1]
                each["Taxid Interactor B"] = A5

        if "column_12" in each and each["column_12"] is not None:  # Interaction Identifiers
            if ":" in each["column_12"]:
                A6 = {}
                A6[each["column_12"].split(":")[0]] = each["column_12"].split(":")[1]
                each["Interaction Identifiers"] = A6

        if "column_15" in each and each["column_15"] is not None:  # Confidence Values
            if ":" in each["column_15"]:
                A7 = {}
                A7[each["column_15"].split(":")[0]] = each["column_15"].split(":")[1]
                each["Confidence Values"] = A7

        if "column_11" in each and each["column_11"] is not None:  # Interaction Types
            if ":" in each["column_11"] and len(each["column_11"].split(":")) == 3:
                A8 = {}
                A88 = {}
                A888 = each["column_11"].split(":")[2].replace("\"", " ")
                A88[each["column_11"].split(":")[1][1:]] = A888
                A8[each["column_11"].split(":")[0]] = A88
                each["Interaction Identifiers"] = A8

        if "column_17" in each and each["column_17"] is not None:  # Source Database
            if ":" in each["column_17"] and len(each["column_17"].split(":")) == 3:
                A9 = {}
                A99 = {}
                A999 = each["column_17"].split(":")[2].replace("\"", " ")
                A99[each["column_17"].split(":")[1][1:]] = A999
                A9[each["column_17"].split(":")[0]] = A99
                each["Source Database"] = A9

        if "column_13" in each and each["column_13"] is not None:  # Interaction Detection Method
            if ":" in each["column_13"] and len(each["column_13"].split(":")) == 3:
                A110 = {}
                A1100 = {}
                A11000 = each["column_13"].split(":")[2].replace("\"", " ")
                A1100[each["column_13"].split(":")[1][1:]] = A11000
                A110[each["column_13"].split(":")[0]] = A1100
                each["Interaction Detection Method"] = A110

        if "" in each.keys():
            del each[""]

        if "column_14" in each and each["column_14"] is not None:  # Publication 1st Author
            each["Publication 1st Author"] = each["column_14"][1:-1]

        return each

#########################################################################################################################################
if __name__ == '__main__':
    cfgfile = "../conf/drugkb_test.config"
    config = configparser.ConfigParser()
    config.read(cfgfile)
    biogrid_parser = BiogridParser()

    for i in range(0, int(config.get('biogrid', 'data_path_num'))):
        data_path = config.get('biogrid', 'data_path_' + str(i + 1))
        dict_data_generator = biogrid_parser.parse(data_path)

        # 只输出一个字典
        first_entry = next(dict_data_generator)
        print(first_entry)

        # 如果只需要处理一个文件，添加一个break
        break
