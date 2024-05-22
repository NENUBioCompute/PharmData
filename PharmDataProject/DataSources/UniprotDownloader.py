"""
======================
-*- coding: utf-8 -*-
@author: Qiufen.Chen
@time: 2021/6/23:14:51
@email: 1760812842@qq.com
@File: get_data.py
@Project: DrugKB
======================
"""
import os
import configparser
from tqdm import tqdm
import requests

def download_with_progress(url, output_path):
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024  # 1 Kibibyte

    with open(output_path, 'wb') as file, tqdm(
        desc=os.path.basename(output_path),
        total=total_size,
        unit='B',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in response.iter_content(block_size):
            file.write(data)
            bar.update(len(data))

def get_uniprot_data(down_url, save_path):
    # Get file name
    file_name = os.path.basename(down_url)

    if not os.path.exists(save_path):
        os.makedirs(save_path)

    output_path = os.path.join(save_path, file_name)

    if os.path.exists(output_path):
        print(f"{file_name} 已经存在，跳过下载。")
    else:
        print('开始下载')
        download_with_progress(down_url, output_path)
        print(f'\n{file_name} 下载成功!')

# =================================================================================================================
if __name__ == "__main__":
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb_test.config'
    config.read(cfgfile)
    down_url = config.get('uniprot', 'source_url_1')
    save_path = config.get('uniprot', 'data_path_1')
    get_uniprot_data(down_url, save_path)
