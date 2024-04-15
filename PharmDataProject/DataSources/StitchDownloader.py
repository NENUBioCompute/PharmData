"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2024/04/05 21:31
  @Email: 2665109868@qq.com
  @function
"""
import requests

class FileDownloader:
    def __init__(self, url, filename):
        self.url = url
        self.filename = filename

    def download_file(self):
        response = requests.get(self.url)
        with open(self.filename, 'wb') as file:
            file.write(response.content)
        print(f"文件 {self.filename} 下载完成")

# 创建类实例并调用方法
url = 'http://stitch.embl.de/download/protein_chemical.links.detailed.v5.0/9606.protein_chemical.links.detailed.v5.0.tsv.gz'
filename = '9606.protein_chemical.links.detailed.v5.0.tsv.gz'

downloader = FileDownloader(url, filename)
downloader.download_file()