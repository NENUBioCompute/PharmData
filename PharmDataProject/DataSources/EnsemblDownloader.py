"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2024/05/26 17:00
  @Email: 2665109868@qq.com
  @function
"""
import wget
import os
import gzip
import configparser
import shutil
class EnsemblDownloader:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.cfgfile = '../conf/drugkb.config'
        self.config.read(self.cfgfile)
        self.url = self.config.get('ensembl', 'source_url_1')
        self.path = self.config.get('ensembl', 'data_path_1')

    def mkdir(self,path):
        isExists = os.path.exists(path)
        if not isExists:
            os.makedirs(path)
            return path
        else:
            return path

    def download_to_data(self):
        print(self.path.split('/')[-2], "正在下载...")

        obj = os.path.join(self.mkdir(self.path), self.url.split('/')[-1])
        wget.download(self.url, obj)

        print(" 下载完成！")
        # 使用gzip和shutil模块解压文件
        with gzip.open(obj, 'rb') as f_in:
            with open(self.path+'Homo_sapiens.GRCh38.cds.all.fa', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)


if __name__ == '__main__':
    ensembldownloader = EnsemblDownloader()
    ensembldownloader.download_to_data()
