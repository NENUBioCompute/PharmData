import wget
import os
import configparser
from tqdm import tqdm


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

        with tqdm(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc=url.split('/')[-1]) as bar:
            def bar_progress(current, total, width=80):
                bar.total = total
                bar.n = current
                bar.refresh()

            wget.download(url, obj, bar=bar_progress)
        print("\n下载完成！\n")


if __name__ == '__main__':
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb_test.config'
    config.read(cfgfile)

    downloader = GenbankDownloader()  # 创建 GenbankDownloader 类的实例

    # 下载配置文件中的所有文件
    for i in range(1, int(config.get('genebank', 'source_url_num')) + 1):
        url = config.get('genebank', f'source_url_{i}')
        path = config.get('genebank', f'data_path_{i}')
        downloader.download_to_data(url, path)
