import configparser
import os


class PharmRGKBDownloader:
    def __init__(self):
        pass

    def download(self, url, dir):
        # 使用wget下载文件
        os.system('wget %s -O %s' % (url, dir))

    def un_gz(self, file_name):
        # 解压zip文件
        f = file_name.replace(".zip", "")
        os.system('unzip %s -d %s' % (file_name, f))

def main(url, dir):
    print('start download')
    downloader = PharmRGKBDownloader()  # 创建类的实例
    downloader.download(url, dir)  # 通过实例调用download方法
    downloader.un_gz(dir)  # 通过实例调用un_gz方法

if __name__ == '__main__':
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)
    # 下载和解压源数据
    for idx in range(1, int(config.get('pharmgkb', 'col_num')) + 1):
        source_url = config.get('pharmgkb', 'source_url_%d' % idx)
        data_path = config.get('pharmgkb', 'data_path_%d' % idx)
        main(source_url, data_path)
