import configparser
import os
import requests
from tqdm import tqdm

class PharmRGKBDownloader:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.cfgfile = '../conf/drugkb_test.config'
        self.config.read(self.cfgfile)

    def download(self, url, dir):
        # 创建目录（如果不存在）
        os.makedirs(os.path.dirname(dir), exist_ok=True)

        # 使用requests下载文件并显示进度条
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024
        t = tqdm(total=total_size, unit='iB', unit_scale=True, desc=os.path.basename(dir))

        with open(dir, 'wb') as file:
            for data in response.iter_content(block_size):
                t.update(len(data))
                file.write(data)
        t.close()
        if total_size != 0 and t.n != total_size:
            print("ERROR, something went wrong")

    def un_gz(self, file_name):
        # 解压zip文件
        f = file_name.replace(".zip", "")
        os.system('unzip %s -d %s' % (file_name, f))

    def main(self,url, dir):
        print('start download')
        downloader = PharmRGKBDownloader()  # 创建类的实例
        downloader.download(url, dir)  # 通过实例调用download方法
        downloader.un_gz(dir)  # 通过实例调用un_gz方法
    def download_all(self):
        for idx in range(1, int(self.config.get('pharmgkb', 'col_num')) + 1):
            source_url = self.config.get('pharmgkb', 'source_url_%d' % idx)
            data_path = self.config.get('pharmgkb', 'data_path_%d' % idx)
            self.main(source_url, data_path)

if __name__ == '__main__':

    downloader = PharmRGKBDownloader()  # 创建类的实例
    downloader.download_all()  # 通过实例调用download方法