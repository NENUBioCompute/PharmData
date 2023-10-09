# -*- coding: utf-8 -*-
# @Time : 2023/10/9
# @Author : zhudexu
# @FileName: PubChemDataRetriever.py
# @Software: PyCharm

import requests
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient
import time


class PubChemDataRetriever:
    def __init__(self, base_url, mongodb_url, database_name, collection_name):
        self.base_url = base_url

        # 连接MongoDB数据库
        self.client = MongoClient(mongodb_url)
        # 选择或创建数据库
        self.db = self.client[database_name]
        # 选择或创建集合（表）
        self.collection = self.db[collection_name]

    def retrieve_compound_info(self, cid):
        # 用于检索给定化合物ID列表的化合物信息
        info_url = f'{self.base_url}/{cid}/JSON'
        try:

            info_response = requests.get(info_url)  # 发送请求到Pubchem API
            info_json = info_response.json()

            if "Record" in info_json:
                self.save_compound_data(info_json, cid)
            elif info_json['Fault']['Code'] == 'PUGVIEW.ServerBusy':
                print(f"服务器繁忙再访问一次此组数据{cid}")
                self.retrieve_compound_info(cid)
            elif info_json['Fault']['Code'] == 'PUGVIEW.NotFound':
                print(f"数据{cid}不存在，继续存储其他数据")
            else:
                print(info_json)
        except Exception as e:
            print(f'{cid}数据访问出错：{str(e)}')

    def save_compound_data(self, info_json, cid):
        try:
            if "Record" in info_json:
                compound_data = info_json["Record"]  # 从响应中提取化合物数据
                self.collection.insert_one(compound_data)  # 将化合物数据插入到Mongodb集合
                print(f'{cid}数据保存成功！')
            else:
                print('未找到相关化合物信息。')
        except Exception as e:
            print(f'{cid}数据保存出错：{str(e)}')

    def multi_thread_retrieval(self, cid_list):
        with ThreadPoolExecutor(max_workers=10) as executor:
            for cid in cid_list:
                executor.submit(self.retrieve_compound_info, cid)

    def generate_cid_list(self, start_cid, end_cid):
        cid_list = []
        for i in range(start_cid, end_cid + 1):  # 生成化合物ID列表从start_cid到end_cid
            cid_list.append(i)
        return cid_list


if __name__ == "__main__":
    time_start = time.time()
    base_url = 'https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound'
    mongodb_url = 'mongodb://localhost:27017'
    database_name = 'PharmRG'
    collection_name = 'PubChem'

    retriever = PubChemDataRetriever(base_url, mongodb_url, database_name, collection_name)
    cid_list = retriever.generate_cid_list(1, 10000)

    # 使用多线程运行数据检索
    retriever.multi_thread_retrieval(cid_list)
    time_end = time.time()

    time_sum = time_end - time_start  # 计算的时间差为程序的执行时间，单位为秒/s
    print(time_sum)
