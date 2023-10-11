"""
  -*- encoding: utf-8 -*-
  @Author: luwei
  @Time  : 2023/10/09 21:37
  @Email: 3305551248@qq.com
  @function
"""


import requests
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient
import time

# 设置MongoDB连接信息
mongo_url = "mongodb://readwrite:readwrite@59.73.198.168:27017/?authMechanism=DEFAULT"


# 定义任务函数，根据化合物ID获取数据并保存到MongoDB
def fetch_and_insert(compound_id, collection):
    compound_url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/{compound_id}/JSON/?response_type=display"

    max_retries = 3  # 最大重试次数
    retry_delay = 3  # 重试间隔时间（秒）

    for retry in range(max_retries):
        compound_response = requests.get(compound_url)
        if compound_response.status_code == 200:
            compound_data = compound_response.json()
            if "Record" in compound_data:
                collection.insert_one(compound_data["Record"])
                print(f'Data saved for compound {compound_id}.')
                return  # 成功获取并保存数据，结束重试
            else:
                print(f'Failed to retrieve data for compound {compound_id}. No record found.')
                return  # 数据不存在，无需重试
        else:
            print(f'Failed to retrieve data for compound {compound_id}. Status code: {compound_response.status_code}')
            if retry < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)  # 等待一段时间后进行重试

    print(f"Failed to retrieve data for compound {compound_id} after {max_retries} retries.")


# 封装多线程处理函数
def process_compounds(cid_list, collection):
    with ThreadPoolExecutor(max_workers=10) as executor:
        # 遍历化合物ID，提交任务
        for compound_id in cid_list:
            executor.submit(fetch_and_insert, compound_id, collection)


# 生成化合物ID子列表
def generate_cid_lists():
    cid_lists = []
    cid_list = []

    for i in range(260002, 260003):  # 假设有10000个化合物ID186382  186393
        cid_list.append(i)
        if len(cid_list) >= 500:
            cid_lists.append(cid_list.copy())  # 将当前列表的副本添加到列表的列表中
            cid_list.clear()

    # 处理剩余的化合物ID
    if len(cid_list) > 0:
        cid_lists.append(cid_list)

    return cid_lists
