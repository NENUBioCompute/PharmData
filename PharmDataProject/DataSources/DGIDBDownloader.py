import wget
import os
import configparser

class DGDBDownloader:
    def __init__(self):
        pass

    def mkdir(self, path):
        # 检查路径是否存在，如果不存在则创建
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def download_to_data(self, url, path):
        # 下载文件到指定路径
        print(path.split('/')[-2], "正在下载...")
        obj = os.path.join(self.mkdir(path), url.split('/')[-1])
        wget.download(url, obj)
        print("下载完成！")

if __name__ == '__main__':
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)

    downloader = DGDBDownloader()  # 实例化DGDBDownloader类

    # 通过实例调用方法
    downloader.download_to_data(config.get('dgidb', 'source_url_1'), config.get('dgidb', 'data_path_1'))
    downloader.download_to_data(config.get('dgidb', 'source_url_2'), config.get('dgidb', 'data_path_2'))
    downloader.download_to_data(config.get('dgidb', 'source_url_3'), config.get('dgidb', 'data_path_3'))
    downloader.download_to_data(config.get('dgidb', 'source_url_4'), config.get('dgidb', 'data_path_4'))
