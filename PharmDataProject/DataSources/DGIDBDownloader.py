import wget
import os
import configparser
import requests
from tqdm import tqdm

class DGDBDownloader:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.cfgfile = '../conf/drugkb_test.config'
        self.config.read(self.cfgfile)

    def mkdir(self, path):
        # 检查路径是否存在，如果不存在则创建
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def download_to_data(self, url, path):
        # 获取文件大小
        response = requests.head(url)
        file_size = int(response.headers.get('Content-Length', 0))
        # 下载文件到指定路径并显示进度条
        print(path.split('/')[-2], "正在下载...")
        obj = os.path.join(self.mkdir(path), url.split('/')[-1])

        with requests.get(url, stream=True) as r:
            with open(obj, 'wb') as f:
                with tqdm(total=file_size, unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc=url.split('/')[-1]) as bar:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)
                            bar.update(len(chunk))
        print("\n下载完成！")
    def download(self):
        self.download_to_data(self.config.get('dgidb', 'source_url_1'), self.config.get('dgidb', 'data_path_1'))
        self.download_to_data(self.config.get('dgidb', 'source_url_2'), self.config.get('dgidb', 'data_path_2'))
        self.download_to_data(self.config.get('dgidb', 'source_url_3'), self.config.get('dgidb', 'data_path_3'))
        self.download_to_data(self.config.get('dgidb', 'source_url_4'), self.config.get('dgidb', 'data_path_4'))

if __name__ == '__main__':


    downloader = DGDBDownloader()  # 实例化DGDBDownloader类

    downloader.download()
