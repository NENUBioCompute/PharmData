# ! /usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2020/8/15
# @Author  : Yuanzhao Guo
# @Email   : guoyz@nenu.edu.cn
# @File    : Nsides.py
# @Software: PyCharm


from urllib import request
import os
import shutil
from os.path import join, getsize
import configparser


def download(url, dir):
    print("downloading with urllib")
    request.urlretrieve(url, dir)

def get_twosides(url, dir):
    dir1 = dir + "TWOSIDES.csv.gz"
    download(url, dir1)
    print('下载完成')

def get_offsides(url, dir):
    dir2 = dir + "OFFSIDES.csv.gz"
    download(url, dir2)
    print('下载完成')


if __name__ == '__main__':

    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)

    get_twosides(config.get('twosides', 'source_url_1'),
                 config.get('twosides', 'data_path_1'))

    get_offsides(config.get('twosides', 'source_url_2'),
                 config.get('twosides', 'data_path_2'))