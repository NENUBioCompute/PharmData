#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2021/6/23
@Author  : Jean Qu
@Email   : quj165@nenu.edu.cn
@File    : get_data.py
@Software: PyCharm

"""

import wget
import os
import configparser


def mkdir(path):
    isExists = os.path.exists(path)
    if not isExists:
        os.makedirs(path)
        return path
    else:
        return path

def download_to_data(url, path):
    print(path.split('/')[-2], "正在下载...")

    obj = os.path.join(mkdir(path), url.split('/')[-1])
    wget.download(url, obj)

    print(" 下载完成！")


if __name__ == '__main__':
    # d = os.path.dirname(os.path.abspath(__file__))

    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)

    download_to_data(config.get('dgidb', 'source_url_1'),
                 config.get('dgidb', 'data_path_1'))

    download_to_data(config.get('dgidb', 'source_url_2'),
                 config.get('dgidb', 'data_path_2'))

    download_to_data(config.get('dgidb', 'source_url_3'),
                 config.get('dgidb', 'data_path_3'))

    download_to_data(config.get('dgidb', 'source_url_4'),
                 config.get('dgidb', 'data_path_4'))

