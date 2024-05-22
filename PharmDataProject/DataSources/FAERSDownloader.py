from bs4 import BeautifulSoup
from urllib.request import urlopen
import ssl
import configparser
import requests
from tqdm import tqdm
import os


class FAERSDownloader:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('../conf/drugkb_test.config')
        self.save_path = self.config.get('faers', 'data_path_1')
        self.ssl_context = ssl._create_unverified_context()

    def getFilesUrl(self):
        """
        find all web urls in target page.
        :return: dict files = {"name":"url"}
        """
        target_page = self.config.get('faers', 'source_url_1')
        files = {}
        try:
            request = urlopen(target_page, context=self.ssl_context)
            page_bs = BeautifulSoup(request, "lxml")
            request.close()
        except:
            request = urlopen(target_page, context=self.ssl_context)
            page_bs = BeautifulSoup(request, "html.parser")
        for url in page_bs.find_all("a"):
            a_string = str(url)
            if "ASCII" in a_string.upper():
                t_url = url.get('href')
                files[str(url.get('href'))[-16:-4]] = t_url
        return files

    def download_file(self, url, save_path, name):
        # 确保目录存在
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        print(f"Downloading from {url} to {save_path}")
        try:
            # 发送HTTP GET请求
            response = requests.get(url, stream=True, verify=False)
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
            self.download_file(url, save_path, name)

    def down_all_files(self):
        urls = self.getFilesUrl()
        for name, url in urls.items():
            try:
                full_save_path = os.path.join(self.save_path, name + '.zip')
                print(f"Checking if {name} needs to be downloaded to {full_save_path}")
                if not os.path.exists(full_save_path):
                    self.download_file(url, full_save_path, name)
                else:
                    print(f"{full_save_path} 已存在，跳过下载")
            except Exception as e:
                print(e)
                self.download_file(url, full_save_path, name)
            finally:
                self.download_file(url, full_save_path, name)


if __name__ == '__main__':
    faers_downloader = FAERSDownloader()
    faers_downloader.down_all_files()
