"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2023/10/15 18:41
  @Email: 2665109868@qq.com
  @function
"""

# -*- coding: utf-8 -*-
""" Index dgidb dataset with MongoDB"""
import os
import configparser

class DGIDBParser:

    def __init__(self, data_path):
        self.data_path = data_path

    def parse(self):
        file_list = os.listdir(self.data_path)
        if not file_list:
            print("No files found in the directory:", self.data_path)
            return []

        file_path = os.path.join(self.data_path, file_list[0])
        print(f"Reading file: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
        except UnicodeDecodeError:
            print(f"Failed to read file with utf-8 encoding: {file_path}")
            try:
                with open(file_path, 'r', encoding='latin1') as file:
                    lines = file.readlines()
            except UnicodeDecodeError:
                print(f"Failed to read file with latin1 encoding: {file_path}")
                return []

        if not lines:
            print(f"No content found in the file: {file_path}")
            return []

        keys = lines[0].strip().split('\t')
        parsed_data = []
        for line in lines[1:]:
            values = line.strip().split('\t')
            # 确保每个键都有值，使用 None 填充缺失的值
            data_dict = {keys[i]: (values[i] if i < len(values) else None) for i in range(len(keys))}
            parsed_data.append(data_dict)

        return parsed_data  # 返回解析的数据字典列表


if __name__ == '__main__':
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb_test.config'
    config.read(cfgfile)

    for i in range(int(config.get('dgidb', 'data_path_num'))):
        data_path = config.get('dgidb', 'data_path_' + str(i + 1))
        print(f"Processing data path: {data_path}")

        parser = DGIDBParser(data_path)
        data = parser.parse()

        if data:
            print(data[0])  # 打印第一个元素
            break  # 处理完第一组数据后停止循环
        else:
            print("No data parsed.")
