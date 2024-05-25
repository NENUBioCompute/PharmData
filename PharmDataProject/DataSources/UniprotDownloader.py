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
import wget
import configparser


def get_uniprot_data(down_url, save_path):
    # Get file name
    file_name = wget.filename_from_url(down_url)
    # print(file_name)
    # /data/DrugKB/data/uniprot/BindingDB

    if not os.path.exists(save_path):
        os.mkdir(save_path)
    print('开始下载')
    wget.download(down_url, out=os.path.join(save_path, file_name))
    print(file_name + 'is downloaded successfully!')


# =================================================================================================================
if __name__ == "__main__":
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)
    down_url = config.get('uniprot', 'source_url_1')
    save_path = config.get('uniprot', 'data_path_1')
    get_uniprot_data(down_url, save_path)
