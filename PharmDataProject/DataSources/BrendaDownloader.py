import configparser
import time
import os

class BrendaDownloader:
    def __init__(self, url, file_name):
        self.url = url
        self.file_name = file_name

    def download(self):
        # 检查文件是否存在
        if os.path.exists(self.file_name):
            print('File found:', self.file_name)
        else:
            print('File not found. Please register to download the BRENDA Textfile!', self.url)

    def un_gz(self):
        # 解压缩文件
        f_name = self.file_name[:self.file_name.rindex('/')]
        print('Extracting:', 'tar -zxvf %s -C %s' % (self.file_name, f_name))
        os.system('tar -zxvf %s -C %s' % (self.file_name, f_name))

def main(url, file_name):
    print('Start downloading...')
    downloader = BrendaDownloader(url, file_name)
    downloader.download()
    downloader.un_gz()

if __name__ == '__main__':
    print('Start:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    start = time.time()
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)
    section = 'brenda'
    col_num = int(config.get(section, 'col_num'))

    for idx in range(col_num):
        source_url = config.get(section, 'source_url_%d' % (idx + 1))
        file_name = config.get(section, 'data_path_%d' % (idx + 1))
        main(source_url, file_name)

    print('End:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('Duration:', time.time() - start)
