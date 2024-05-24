import configparser
from urllib import request
import os


class TwosidesDownloader:
    def __init__(self):
        pass

    def download(self, url, dir):
        print("downloading with urllib")
        request.urlretrieve(url, dir)

    def get_twosides(self, url, dir):
        dir1 = dir + "TWOSIDES.csv.gz"
        self.download(url, dir1)
        print('下载完成')

    def get_offsides(self, url, dir):
        dir2 = dir + "OFFSIDES.csv.gz"
        self.download(url, dir2)
        print('下载完成')


if __name__ == '__main__':
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)
    downloader = TwosidesDownloader()  # 创建 TwosidesDownloader 类的实例

    # 使用实例调用方法
    downloader.get_twosides(config.get('twosides', 'source_url_1'),
                            config.get('twosides', 'data_path_1'))

    downloader.get_offsides(config.get('twosides', 'source_url_2'),
                            config.get('twosides', 'data_path_2'))
