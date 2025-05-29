"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2023/10/22 14:00
  @Email: 2665109868@qq.com
  @function
"""
import configparser
import os

class DoParser:
    def __init__(self, datapath):
        self.datapath = datapath

    def parse(self):
        xref_list = {}
        xref_id_list = []
        xref_id_value_list = []

        parent_id_name_dic = {}
        parent = []

        def_discription = {}

        synonym_list = []
        subset_number_list = []
        alt_id_number_list = []

        dic = {}

        try:
            with open(self.datapath, 'r', encoding="utf-8") as f:
                for line in f.readlines():
                    lable = line.split(":")[0]  # Read the list ‘name’, starting from the position of '0', ending with ":", reading all field names

                    if lable == "id":  # Judge the label for storage
                        id_number = line[3:].strip().split(":")[1]
                        dic["doid"] = id_number
                    elif lable == "name":
                        name_number = line[5:].strip()
                        dic["name"] = name_number
                    elif lable == "is_obsolete":
                        is_obsolete_number = line[12:].strip()
                        dic["is_obsolete"] = is_obsolete_number
                    elif lable == "def":
                        DEF_number = line[5:].strip().split("[")[0].replace(("\""), ("")).replace(('\\'), "\'")
                        def_discription["def_description"] = DEF_number
                        def_url = list(line[5:].strip().split("[")[1].split("]")[0].replace("\\", "").replace("url:", "").split(", "))
                        def_discription["def_url"] = def_url
                        dic["def"] = def_discription
                    elif lable == "property_value":
                        property_value_number = line[15:].strip()
                        dic["property_value"] = property_value_number
                    elif lable == "comment":
                        comment_number = line[8:].strip()
                        dic["comment"] = comment_number
                    elif lable == "xref":
                        xref_id = line[5:].strip().split(":")[0]
                        xref_id_list.append(xref_id)
                        xref_id_value = line[5:].strip().split(":")[1]
                        xref_id_value_list.append(xref_id_value)
                        for i in range(0, len(xref_id_list)):
                            xref_list[xref_id_list[i]] = xref_id_value_list[i]
                            dic["xref"] = xref_list
                    elif lable == "disjoint_from":
                        disjoint_from_number = line[14:].strip()
                        dic["disjoint_from"] = disjoint_from_number
                    elif lable == "alt_id":
                        alt_id_number_list.append(line[7:].strip().split(":")[1])
                        dic["alt_doid"] = alt_id_number_list
                    elif lable == "subset":
                        subset_number_list.append(line[7:].strip())
                        dic["subset"] = subset_number_list
                    elif lable == "created_by":
                        created_by_number = line[11:].strip()
                        dic["created_by"] = created_by_number
                    elif lable == "creation_date":
                        creation_date_number = line[14:].strip()
                        dic["creation_date"] = creation_date_number
                    elif lable == "is_a":
                        parent_id = line[5:].strip().split(":")[1].split("!")[0].strip()
                        parent_id_name_dic["doid"] = parent_id
                        parent_name_value = line[5:].strip().split("!")[1].strip()
                        parent_id_name_dic["name"] = parent_name_value
                        parent.append(parent_id_name_dic)
                        parent_id_name_dic = {}
                        dic["parent"] = parent
                    elif lable == "synonym":
                        synonym_list.append(line[8:].strip().split("\"")[1])
                        dic["synonym"] = synonym_list
                    elif line.strip("\n") == "[Term]":
                        def_discription = {}
                        synonym_list = []
                        subset_number_list = []
                        xref_list = {}
                        xref_id_list = []
                        xref_id_value_list = []
                        parent = []
                        alt_id_number_list = []
                        if dic:  # Check if dic is not empty
                            yield dic
                        dic = {}  # Reset for next term
                if dic:  # Ensure the last dictionary is added
                    yield dic
        except PermissionError as e:
            print(f"Permission denied: {self.datapath}")
            print(e)
        except Exception as e:
            print(f"An error occurred while parsing the file: {self.datapath}")
            print(e)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb_test.config'
    config.read(cfgfile)

    for i in range(0, int(config.get('do', 'data_path_num'))):
        datapath = config.get('do', 'data_path_' + str(i + 1))
        parser = DoParser(datapath)
        for data_dict in parser.parse():
            print(data_dict)  # 打印第一个字典
            break  # 成功打印后退出循环
