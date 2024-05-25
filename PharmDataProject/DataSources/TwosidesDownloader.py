import configparser
from urllib import request
import os
from tqdm import tqdm

class TwosidesDownloader:
    def __init__(self):
        pass

    def download(self, url, dir):
        print(f"Downloading from {url} to {dir}")
        with tqdm(unit='B', unit_scale=True, miniters=1, desc=os.path.basename(dir)) as t:
            request.urlretrieve(url, dir, reporthook=self.report_hook(t))
        print(f"Downloaded to {dir}")

    def report_hook(self, t):
        last_b = [0]

        def update_to(b=1, bsize=1, tsize=None):
            if tsize is not None:
                t.total = tsize
            t.update((b - last_b[0]) * bsize)
            last_b[0] = b
        return update_to

    def get_twosides(self, url, dir):
        dir1 = os.path.join(dir, "TWOSIDES.csv.gz")
        print(f"Checking if TWOSIDES needs to be downloaded to {dir1}")
        if not os.path.exists(dir1):
            self.download(url, dir1)
            print('TWOSIDES 下载完成')
        else:
            print(f"{dir1} already exists, skipping download.")

    def get_offsides(self, url, dir):
        dir2 = os.path.join(dir, "OFFSIDES.csv.gz")
        print(f"Checking if OFFSIDES needs to be downloaded to {dir2}")
        if not os.path.exists(dir2):
            self.download(url, dir2)
            print('OFFSIDES 下载完成')
        else:
            print(f"{dir2} already exists, skipping download.")

if __name__ == '__main__':
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb_test.config'
    config.read(cfgfile)
    downloader = TwosidesDownloader()  # 创建 TwosidesDownloader 类的实例

    # 使用实例调用方法
    twosides_url = config.get('twosides', 'source_url_1')
    twosides_path = config.get('twosides', 'data_path_2')
    offsides_url = config.get('twosides', 'source_url_2')
    offsides_path = config.get('twosides', 'data_path_1')

    print(f"TWOSIDES download URL: {twosides_url}")
    print(f"TWOSIDES save path: {twosides_path}")
    downloader.get_twosides(twosides_url, twosides_path)

    print(f"OFFSIDES download URL: {offsides_url}")
    print(f"OFFSIDES save path: {offsides_path}")
    downloader.get_offsides(offsides_url, offsides_path)
