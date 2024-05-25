# -*- encoding: utf-8 -*-
# @Author: zhaojingtong
# @Time  : 2023/10/22 13:35
# @Email: 2665109868@qq.com
# @function: DoDownloader class to download files

import configparser
import os
import requests

class DoDownloader:
    def __init__(self, config_path):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

    def get_do_data(self, url, path):
        response = requests.get(url, stream=True)
        file_name = os.path.join(path, url.split('/')[-1])
        os.makedirs(path, exist_ok=True)

        with open(file_name, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded {file_name}")

    def download_from_config(self):
        self.get_do_data(self.config.get('do', 'source_url_1'),
                         self.config.get('do', 'data_path_1'))

if __name__ == "__main__":
    cfgfile = '../conf/drugkb_test.config'
    downloader = DoDownloader(cfgfile)
    downloader.download_from_config()
