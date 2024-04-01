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


class TwosidesDownloader:
    def __init__(self):
        pass

    def download(self, url, dir):
        print("downloading with urllib")
        request.urlretrieve(url, dir)

    def get_twosides(self, url, dir):
        dir1 = dir + "TWOSIDES.csv.gz"
        self.download(url, dir1)
        print('下载完成')

    def get_offsides(self, url, dir):
        dir2 = dir + "OFFSIDES.csv.gz"
        self.download(url, dir2)
        print('下载完成')


if __name__ == '__main__':
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)
    TwosidesDownloader = TwosidesDownloader

    TwosidesDownloader.get_twosides(config.get('twosides', 'source_url_1'),
                 config.get('twosides', 'data_path_1'))

    TwosidesDownloader.get_offsides(config.get('twosides', 'source_url_2'),
                 config.get('twosides', 'data_path_2'))
