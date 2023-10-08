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
        with open(data_path, 'r', encoding='utf-8') as csvfile:
            read_result = csv.DictReader(csvfile, delimiter=col_delimiter)
            for line in read_result:
                result.append(line)
        # extract
        FileSystem.folder_is_exists(os.path.split(json_path)[0])
        with open(json_path, 'w', encoding='utf-8') as fp:
            json.dump(result, fp, indent=4)
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
