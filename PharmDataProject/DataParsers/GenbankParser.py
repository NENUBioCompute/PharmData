# -*- coding: utf-8 -*-
""" Index genbank dataset with MongoDB"""
import os
import gzip
import configparser
from concurrent.futures import ThreadPoolExecutor

class GenbankParser:
    def __init__(self, data_path):
        self.data_path = data_path

    def mkpath(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def un_gz(self, file_name):
        f_name = file_name.replace(".gz", "")
        g_file = gzip.GzipFile(file_name)
        open(f_name, "wb+").write(g_file.read())
        g_file.close()
        return f_name

    def parse_line(self, line, keys):
        values = line.strip('\n').split('\t')
        for i in range(len(values)):
            if '|' in values[i]:
                values[i] = values[i].split('|')
        return {keys[i].replace('.', ' '): values[i].replace('-', '') if isinstance(values[i], str) else values[i]
                for i in range(len(keys))}

    def parse(self):
        file_list = os.listdir(self.data_path)
        if not file_list:
            print("No files found in the directory:", self.data_path)
            return

        file_path = os.path.join(self.data_path, file_list[0])
        print(f"Reading file: {file_path}")

        if file_path.endswith('.gz'):
            file_path = self.un_gz(file_path)

        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        if not lines:
            print(f"No content found in the file: {file_path}")
            return

        keys = lines[0].lstrip('#').rstrip('\n').split('\t')
        for line in lines[1:]:
            yield self.parse_line(line, keys)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb_test.config'
    config.read(cfgfile)

    for i in range(1, int(config.get('genebank', 'data_path_num')) + 1):
        data_path = config.get('genebank', 'data_path_' + str(i))
        print(f"Processing data path: {data_path}")

        parser = GenbankParser(data_path)
        data_gen = parser.parse()

        for data_dict in data_gen:
            print(data_dict)  # 打印第一个元素
            break  # 处理完第一组数据后停止循环
