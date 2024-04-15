import wget
import os
import configparser

class GenbankDownloader:
    def __init__(self):
        pass

    def mkdir(self, path):
        # 确保目录存在
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def download_to_data(self, url, path):
        """
        下载文件，存入相应的文件目录下
        :param url: 下载地址
        :param path: 存储地址
        :return:
        """
        print(path.split('/')[-2], "正在下载...")
        # 确保目录存在
        complete_path = self.mkdir(path)
        obj = os.path.join(complete_path, url.split('/')[-1])
        wget.download(url, obj)
        print("下载完成！\n")

if __name__ == '__main__':
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)

    downloader = GenbankDownloader()  # 创建 GenbankDownloader 类的实例

    # 通过实例调用方法
    downloader.download_to_data(config.get('genebank', 'source_url_2'),
                                config.get('genebank', 'data_path_2'))
