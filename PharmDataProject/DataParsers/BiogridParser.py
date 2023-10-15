# -*- coding: utf-8 -*-
"""
# @Time        : 2023/1/11
# @Author      : tanliqiu
# @FileName    : BiogridParser.py
# @Software    : PyCharm
# @ProjectName : Biogrid
"""

import csv
import os
import json
import traceback

class DrugsParser:
    # parse txt
    def parser_data_txt(self,data_path,json_path,col_delimiter: str = " "):
        result = []
        num = 0
        with open(data_path, 'r', encoding='utf-8') as csvfile:
            read_result = csv.DictReader(csvfile, delimiter=col_delimiter)
            for each in read_result:
                num = num + 1
                A = {}
                if each["Alt IDs Interactor A"] is not None:
                    if "|" in each["Alt IDs Interactor A"] and not each["Alt IDs Interactor A"] is None:
                        tag = 1
                        for i in each["Alt IDs Interactor A"].split("|"):
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
                                # Alt IDs Interactor B
                B = {}
                if each["Alt IDs Interactor B"] is not None:
                    if "|" in each["Alt IDs Interactor B"] and not each["Alt IDs Interactor B"] is None:
                        tag = 1
                        for i in each["Alt IDs Interactor B"].split("|"):
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

                                # Aliases Interactor A
                AA = {}
                if each["Aliases Interactor A"] is not None:
                    if "|" in each["Aliases Interactor A"] and not each["Aliases Interactor A"] is None:
                        tag = 1
                        for i in each["Aliases Interactor A"].split("|"):
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

                                # Aliases Interactor B

                BB = {}
                if each["Aliases Interactor B"] is not None:
                    if "|" in each["Aliases Interactor B"] and not each["Aliases Interactor B"] is None:
                        tag = 1
                        for i in each["Aliases Interactor B"].split("|"):
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

                if each["#ID Interactor A"] != None:
                    if ":" in each["#ID Interactor A"]:
                        A1 = {}
                        A1[each["#ID Interactor A"].split(":")[0]] = each["#ID Interactor A"].split(":")[1]
                        each["#ID Interactor A"] = A1
                        each["ID Interactor A"]=each.pop("#ID Interactor A")

                if each["ID Interactor B"] != None:
                    if ":" in each["ID Interactor B"]:
                        A2 = {}
                        A2[each["ID Interactor B"].split(":")[0]] = each["ID Interactor B"].split(":")[1]
                        each["ID Interactor B"] = A2

                if each["Publication Identifiers"] != None:
                    if ":" in each["Publication Identifiers"]:
                        A3 = {}
                        A3[each["Publication Identifiers"].split(":")[0]] = each["Publication Identifiers"].split(":")[
                            1]
                        each["Publication Identifiers"] = A3

                if each["Taxid Interactor A"] != None:
                    if ":" in each["Taxid Interactor A"]:
                        A4 = {}
                        A4[each["Taxid Interactor A"].split(":")[0]] = each["Taxid Interactor A"].split(":")[1]
                        each["Taxid Interactor A"] = A4


                if each["Taxid Interactor B"] != None:
                    if ":" in each["Taxid Interactor B"]:
                        A5 = {}
                        A5[each["Taxid Interactor B"].split(":")[0]] = each["Taxid Interactor B"].split(":")[1]
                        each["Taxid Interactor B"] = A5


                if each["Taxid Interactor B"] != None:
                    if ":" in each["Interaction Identifiers"]:
                        A6 = {}
                        A6[each["Interaction Identifiers"].split(":")[0]] = each["Interaction Identifiers"].split(":")[
                            1]
                        each["Interaction Identifiers"] = A6


                if each["Confidence Values"] != None:
                    if ":" in each["Confidence Values"]:
                        A7 = {}
                        A7[each["Confidence Values"].split(":")[0]] = each["Confidence Values"].split(":")[1]
                        each["Confidence Values"] = A7


                if each["Interaction Types"] != None:
                    if ":" in each["Interaction Types"] and len(each["Interaction Types"].split(":")) == 3:
                        A8 = {}
                        A88 = {}
                        A888 = each["Interaction Types"].split(":")[2].replace("\"", " ")
                        A88[each["Interaction Types"].split(":")[1][1:]] = A888
                        A8[each["Interaction Types"].split(":")[0]] = A88
                        each["Interaction Identifiers"] = A8

                if each["Source Database"] != None:
                    if ":" in each["Source Database"] and len(each["Source Database"].split(":")) == 3:
                        A9 = {}
                        A99 = {}
                        A999 = each["Source Database"].split(":")[2].replace("\"", " ")
                        A99[each["Source Database"].split(":")[1][1:]] = A999
                        A9[each["Source Database"].split(":")[0]] = A99
                        each["Source Database"] = A9

                if each["Interaction Types"] != None:
                    if ":" in each["Interaction Detection Method"] and len(
                            each["Interaction Detection Method"].split(":")) == 3:
                        A110 = {}
                        A1100 = {}
                        A11000 = each["Interaction Detection Method"].split(":")[2].replace("\"", " ")
                        A1100[each["Interaction Detection Method"].split(":")[1][1:]] = A11000
                        A110[each["Interaction Detection Method"].split(":")[0]] = A1100
                        each["Interaction Detection Method"] = A110

                if "" in each.keys():
                    del each[""]

                if each["Publication 1st Author"] != None:
                    each["Publication 1st Author"] = each["Publication 1st Author"][1:-1]

        # extract
        FileSystem.folder_is_exists(os.path.split(json_path)[0])
        with open(json_path, 'w', encoding='utf-8') as fp:
            json.dump(each, fp, indent=4)
        fp.close()
        # write
class FileSystem:
    def folder_is_exists(src: str):
        try:
            if not os.path.exists(src):
                os.makedirs(src)
            return src
        except (FileNotFoundError, PermissionError):
            traceback.print_exc()

if __name__ == "__main__":
    DrugsParser().parser_data_txt("./Biogrid_data/BIOGRID-ALL-4.4.224.mitab.txt","./Biogrid_Parser/data.json",'\t')
