"""
  -*- encoding: utf-8 -*-
  @Author: yuziyang
  @Time  : 2023/10/09
  @Email: 2601183959@qq.com
  @function
"""
import logging
import zipfile
import requests


class PharmGkbDownloader:
    def __init__(self, url):
        self.url = url

    def download_file(self, zip_name):
        url = self.url + zip_name
        response = requests.get(url)
        if response.status_code == 200:
            with open(zip_name, "wb") as f:
                f.write(response.content)
            logging.log("文件下载成功！")
        else:
            logging.log("文件下载失败。")

    def decompress_file(self, zip_name, dir_name="data"):
        self.download_file(zip_name)
        try:
            with zipfile.ZipFile(zip_name, 'r') as zip_file:
                if zip_file.testzip() is not None:
                    logging.log('zip 文件已损坏！')
                else:
                    with zipfile.ZipFile(zip_name, "r") as zip_ref:
                        zip_ref.extractall(dir_name)
                    logging.log("解压缩完成！")
        except zipfile.BadZipFile:
            logging.log('不是 zip 文件！')