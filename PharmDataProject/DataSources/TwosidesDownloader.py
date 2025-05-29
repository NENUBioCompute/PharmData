import configparser
from urllib import request
import os
from tqdm import tqdm
import gzip

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
        self.config = configparser.ConfigParser()
        self.cfgfile = '../conf/drugkb_test.config'
        self.config.read(self.cfgfile)
    def download(self,url, dir):
        os.makedirs(os.path.dirname(dir), exist_ok=True)
        print("downloading with urllib")
        request.urlretrieve(url, dir)

    def get_twosides(self,url, dir):
        dir1 = dir + "TWOSIDES.csv.gz"
        self.download(url, dir1)
        with gzip.open(dir1, 'rb') as f_in:
            with open(dir+'TWOSIDES.csv', 'wb') as f_out:
                f_out.write(f_in.read())
        print('下载完成')

    def get_offsides(self,url, dir):
        dir2 = dir + "OFFSIDES.csv.gz"
        self.download(url, dir2)
        with gzip.open(dir2, 'rb') as f_in:
            with open(dir+'OFFSIDES.csv', 'wb') as f_out:
                f_out.write(f_in.read())
        print('下载完成')
    def download_all(self):
        self.get_twosides(self.config.get('twosides', 'source_url_1'),
                     self.config.get('twosides', 'data_path_1'))

        self.get_offsides(self.config.get('twosides', 'source_url_2'),
                     self.config.get('twosides', 'data_path_2'))


if __name__ == '__main__':
    twosidesdownloader = TwosidesDownloader()
    twosidesdownloader.download_all()




