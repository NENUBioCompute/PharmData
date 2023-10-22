"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2023/10/22 13:35
  @Email: 2665109868@qq.com
  @function
"""
import configparser
import os

def get_do_data(url,path):
      #url = "http://purl.obolibrary.org/obo/doid.obo"
      os.system("wget " + str(url)+" -P "+path)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)
    get_do_data(config.get('do', 'source_url_1'),
                config.get('do', 'data_path_1'))