"""
  -*- encoding: utf-8 -*-
  @Author: wangyang
  @Time  : 2023/10/04 19:38
  @Email: 2168259496@qq.com
  @function
"""
import wget
import os
import configparser
from PharmDataProject.Utilities.NetDownloads.HttpDownloader import HTTP

class GenbankDownloader:
    def __init__(self):
        pass
    def mkdir(self, path):
        isExists = os.path.exists(path)
        if not isExists:
            os.makedirs(path)
            return path
        else:
            return path

    def download_to_data(self, url, path):
        """
        下载文件，存入相应的文件目录下
        :param url: 下载地址
        :param path: 存储地址
        :return:
        """
        print(path.split('/')[-2], "正在下载...")
        obj = os.path.join(mkdir(path), url. split('/')[-1])
        wget.download(url, obj)
        print(" 下载完成！\n")



if __name__ == '__main__':

    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)
    GenbankDownloader = GenbankDownloader
    # download_to_data(config.get('genebank', 'source_url_0'),
    #                  config.get('genebank', 'data_path_0'))
    GenbankDownloader.download_to_data(config.get('genebank', 'source_url_2'),
                     config.get('genebank', 'data_path_2'))




