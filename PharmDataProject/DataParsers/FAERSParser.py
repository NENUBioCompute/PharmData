"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2024/03/31 15:07
  @Email: 2665109868@qq.com
  @function
"""
import pandas as pd

import zipfile

import configparser


import os



class FAERSParser:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('../conf/drugkb_test.config')
        self.save_path = self.config.get('faers', 'data_path_1')

    def get_zip_file_path(self):
        # 指定目标目录
        target_dir = self.save_path
        zip_file_paths = []
        # 递归遍历目录及其子目录
        for root, dirs, files in os.walk(target_dir):
            for file in files:
                # 检查文件是否为 ZIP 格式
                if file.endswith(".zip"):
                    # 构建 ZIP 文件的完整路径
                    zip_file_path = os.path.join(root, file)
                    zip_file_paths.append(zip_file_path)
        return zip_file_paths
    def faers_parse(self,zip_file_path):
        # 解压缩zip文件
        extract_folder = zip_file_path.split('.')[0]
        print(extract_folder)


        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)

        # 遍历提取的文件，找出ASCII目录下的txt文件
        ascii_txt_files = []

        for root, dirs, files in os.walk(os.path.join(extract_folder, 'ASCII')):
            for file in files:
                if file.endswith('.txt'):
                    ascii_txt_files.append(os.path.join(root, file))
            for file in files:
                if file.endswith('.TXT'):
                    ascii_txt_files.append(os.path.join(root, file))
        for root, dirs, files in os.walk(os.path.join(extract_folder, 'ascii')):
            for file in files:
                if file.endswith('.txt'):
                    ascii_txt_files.append(os.path.join(root, file))
            for file in files:
                if file.endswith('.TXT'):
                    ascii_txt_files.append(os.path.join(root, file))
        ascii_txt_files = list(set(ascii_txt_files))


        # 创建一个空的DataFrame来存储所有数据
        all_data = pd.DataFrame()
        merged_data = pd.DataFrame()
        try:
            # 逐个读取文件并合并数据
            for file_path in ascii_txt_files:
                data = pd.read_csv(file_path, sep='$',low_memory=False)  # 读取文件
                all_data = pd.concat([all_data, data], ignore_index=True)  # 连接数据

        # 根据primaryid合并数据

            merged_data = all_data.groupby('primaryid')
            # for key, group in merged_data:
            #     data = {k: '' if pd.isna(v) else v for k, v in group.to_dict(orient='records')[0].items()}
            yield merged_data
        except Exception as e:
            pass

    def parser_all_data(self):
        zip_file_paths = self.get_zip_file_path()
        for zip_file_path in zip_file_paths:
            try:
                merged_data = self.faers_parse(zip_file_path)
            except Exception as e:
                print(e)
                pass
            yield from merged_data


if __name__ == '__main__':
    faersparser = FAERSParser()
    for merged_data in faersparser.parser_all_data():

        # Process merged_data here
        # print(merged_data)
        for key, group in merged_data:
            data = {k: '' if pd.isna(v) else v for k, v in group.to_dict(orient='records')[0].items()}
            print(data)
            break
        break
