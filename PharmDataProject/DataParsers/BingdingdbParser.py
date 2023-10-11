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
                # print(f'Failed to retrieve data for compound {compound_id}. No record found.')
                return  # 数据不存在，无需重试
        else:
            # print(f'Failed to retrieve data for compound {compound_id}. Status code: {compound_response.status_code}')
            if retry < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)  # 等待一段时间后进行重试
