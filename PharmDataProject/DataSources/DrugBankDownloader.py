import configparser
import os
from tqdm import tqdm
import requests


class DrugbankDownloader:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('../conf/drugkb_test.config')
        self.url = self.config.get('drugbank', 'source_url_1')
        self.save_path = self.config.get('drugbank', 'data_path_1')

    def download_file(self, url, save_path):
        if not os.path.exists(os.path.dirname(save_path)):
            os.makedirs(os.path.dirname(save_path))
        response = requests.get(url, stream=True, verify=False)
        total_size = int(response.headers.get('content-length', 0))
        with open(save_path, 'wb') as f, tqdm(
                desc=save_path,
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
        ) as bar:
            for data in response.iter_content(chunk_size=1024):
                f.write(data)
                bar.update(len(data))
        print("Download completed.")

    def start_download(self):
        print(f"Downloading from {self.url} to {self.save_path}")
        self.download_file(self.url, self.save_path)


if __name__ == '__main__':
    downloader = DrugbankDownloader()
    downloader.start_download()
