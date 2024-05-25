import configparser
import time
import os
import tarfile
import requests
from tqdm import tqdm


class BrendaDownloader:
    def __init__(self, url, file_name, sourcebase, sourcebase_des):
        self.url = url
        self.file_name = file_name
        self.sourcebase = sourcebase
        self.sourcebase_des = sourcebase_des

    def download(self):
        # 检查文件目录是否存在
        os.makedirs(os.path.dirname(self.file_name), exist_ok=True)

        # 检查文件是否存在
        if os.path.exists(self.file_name):
            print('File found:', self.file_name)
            return True
        else:
            # 下载文件
            try:
                print('Downloading file from:', self.url)
                response = requests.get(self.url, stream=True)
                content_type = response.headers.get('Content-Type')
                if 'text/html' in content_type:
                    print(
                        f"Error: The URL returned an HTML page instead of a tar.gz file. Please download the file manually from: {self.sourcebase}")
                    print(f"Description: {self.sourcebase_des}")
                    return False

                total_size = int(response.headers.get('content-length', 0))
                with open(self.file_name, 'wb') as f:
                    for data in tqdm(response.iter_content(chunk_size=1024), total=total_size // 1024, unit='KB',
                                     desc=self.file_name):
                        f.write(data)
                print('Download completed:', self.file_name)
                return True
            except requests.exceptions.RequestException as e:
                print(f"Error downloading file: {e}")
                return False

    def un_gz(self):
        # 解压缩文件
        if tarfile.is_tarfile(self.file_name):
            f_name = os.path.dirname(self.file_name)
            print('Extracting:', self.file_name)
            with tarfile.open(self.file_name, 'r:gz') as tar:
                tar.extractall(path=f_name)
            print('Extraction completed.')
        else:
            print('The file is not a tar.gz archive:', self.file_name)
            # 输出文件类型以便进一步诊断
            with open(self.file_name, 'rb') as f:
                header = f.read(4)
                print('File header:', header)


def main(url, file_name, sourcebase, sourcebase_des):
    print('Start downloading...')
    downloader = BrendaDownloader(url, file_name, sourcebase, sourcebase_des)
    if downloader.download():
        downloader.un_gz()


if __name__ == '__main__':
    print('Start:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    start = time.time()
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb_test.config'
    config.read(cfgfile)
    section = 'brenda'
    col_num = int(config.get(section, 'col_num'))
    sourcebase = config.get(section, 'sourcebase')
    sourcebase_des = config.get(section, 'sourcebase_des')

    for idx in range(col_num):
        source_url = config.get(section, 'source_url_%d' % (idx + 1))
        file_name = config.get(section, 'data_path_%d' % (idx + 1))
        main(source_url, file_name, sourcebase, sourcebase_des)

    print('End:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('Duration:', time.time() - start)
