# -*- encoding: utf-8 -*-
# @Author: Deepwind
# @Time  : 4/15/2024 11:04 PM
# @Email: deepwind32@163.com

import os
import subprocess
import threading
import requests
from tqdm import tqdm
from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


class DownloadKit:
    def __init__(self, data_path):
        self.data_path = data_path

    def add(self, url, rename=None):
        self.url = url
        self.filename = rename if rename else url.split('/')[-1]
        return self

    def wait(self, show=True):
        retries = 5
        for _ in range(retries):
            try:
                response = requests.get(self.url, stream=True)
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                filepath = os.path.join(self.data_path, self.filename)

                if os.path.exists(filepath):
                    print(f"{filepath} already exists. Skipping download.")
                    return self

                with open(filepath, 'wb') as file, tqdm(
                        desc=filepath,
                        total=total_size,
                        unit='B',
                        unit_scale=True,
                        unit_divisor=1024,
                ) as bar:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
                        bar.update(len(chunk))

                print(f"{self.filename} downloaded successfully.")
                return self
            except requests.exceptions.RequestException as e:
                print(f"Download failed: {e}. Retrying...")
        raise Exception(f"Failed to download {self.filename} after {retries} retries.")


class StitchDownloader:
    def __init__(self, config):
        self.config = config
        self.data_path = config.get("data_path")
        self.DownloadKit = DownloadKit(self.data_path)

    def __download_and_gunzip(self, filename, url):
        self.DownloadKit.add(url, rename=filename).wait(show=False)
        filepath = os.path.join(self.data_path, filename)
        if os.path.exists(filepath):
            subprocess.run(["gunzip", "-f", filepath])
            if not os.path.exists(filepath):
                print(f"{filename} downloaded and unzipped successfully.")
            else:
                print(f"Failed to unzip {filename}.")
        else:
            print(f"{filename} does not exist, cannot unzip.")

    def start(self):
        urls = [
            (self.config.get("cc_links_filename"), self.config.get("cc_links_url")),
            (self.config.get("pc_links_detailed_filename"), self.config.get("pc_links_detailed_url")),
            (self.config.get("pc_links_transfer_filename"), self.config.get("pc_links_transfer_url")),
            (self.config.get("actions_filename"), self.config.get("actions_url"))
        ]

        threads = [threading.Thread(target=self.__download_and_gunzip, args=(filename, url)) for filename, url in urls]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()


if __name__ == "__main__":
    cfg = "../conf/drugkb_test.config"
    config = ConfigParser(cfg)
    config.set_section("stitch")
    StitchDownloader(config).start()
