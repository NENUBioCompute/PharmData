from bs4 import BeautifulSoup
from urllib.request import urlopen
import configparser
import threading
import requests
from tqdm import tqdm

import os

class FAERSDownloader:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('../conf/drugkb.config')
        self.save_path = self.config.get('faers','data_path_1')

    def getFilesUrl(self):
        """
        find all web urls in target page.
        :return: dict files = {"name":"url"}
        """
        target_page = self.config.get('faers','source_url_1')
        files = {}
        try:
            request = urlopen(target_page)
            page_bs = BeautifulSoup(request, "lxml")
            request.close()
        except:
            request = urlopen(target_page)
            page_bs = BeautifulSoup(request)
        for url in page_bs.find_all("a"):
            a_string = str(url)
            if "ASCII" in a_string.upper():
                t_url = url.get('href')
                files[str(url.get('href'))[-16:-4]] = t_url
        return files


    def download_file(self,url,save_path,name):
        try:
            # 发送HTTP GET请求
            response = requests.get(url, stream=True)
            # 确认请求成功
            if response.status_code == 200:
                # 获取文件大小
                total_size = int(response.headers.get('content-length', 0))
                # 使用tqdm显示下载进度条
                with open(save_path, 'wb') as f, tqdm(
                        desc=save_path,
                        total=total_size,
                        unit='B',
                        unit_scale=True,
                        unit_divisor=1024,
                ) as pbar:
                    # 逐块写入数据，并更新进度条
                    for data in response.iter_content(chunk_size=1024):
                        f.write(data)
                        pbar.update(len(data))
                print("文件下载完成")
            else:
                print("下载失败，状态码：", response.status_code)
        except Exception as e:
            print(e)
            self.download_file(url, self.save_path + name + '.zip',name)

    def down_all_files(self):
        urls = self.getFilesUrl()
        for name, url in urls.items():
            try:
                self.download_file(url,self.save_path+name+'.zip',name)
            except Exception as e:
                print(e)
                self.download_file(url,self.save_path+name+'.zip',name)
            finally:
                self.download_file(url, self.save_path + name + '.zip',name)


if __name__ == '__main__':
    faers_downloader = FAERSDownloader()
    faers_urls = faers_downloader.down_all_files()
