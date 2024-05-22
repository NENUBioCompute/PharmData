"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2023/10/22 13:35
  @Email: 2665109868@qq.com
  @function
"""
import configparser
import os
import requests

def get_do_data(url, path):
    response = requests.get(url, stream=True)
    file_name = os.path.join(path, url.split('/')[-1])
    os.makedirs(path, exist_ok=True)

    with open(file_name, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    print(f"Downloaded {file_name}")

if __name__ == "__main__":
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb_test.config'
    config.read(cfgfile)
    get_do_data(config.get('do', 'source_url_1'),
                config.get('do', 'data_path_1'))
