# -*- coding: utf-8 -*-
# @Time : 2023/10/15 14:37
# @Author : zhudexu
# @FileName: PubChemDataParsers.py
# @Software: PyCharm
import requests
from concurrent.futures import ThreadPoolExecutor


class PubChemDataParsers:
    def __init__(self, base_url):
        self.base_url = base_url

    def retrieve_compound_info(self, cid):

        info_url = f'{self.base_url}/{cid}/JSON'

        try:

            info_response = requests.get(info_url)
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

    def multi_thread_retrieval(self, cid_list):
        with ThreadPoolExecutor(max_workers=10) as executor:
            for cid in cid_list:
                executor.submit(self.retrieve_compound_info, cid)

    def generate_cid_list(self, start_cid, end_cid):
        cid_list = []
        for i in range(start_cid, end_cid + 1):
            cid_list.append(i)
        return cid_list
