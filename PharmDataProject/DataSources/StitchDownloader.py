"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 4/15/2024 11:04 PM
  @Email: deepwind32@163.com
"""
import os
import subprocess
import threading

from DownloadKit import DownloadKit

from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


class StitchDownloader:
    def __init__(self, config):
        self.config = config
        self.data_path = config.get("data_path")
        self.DownloadKit = DownloadKit(self.data_path)

    def __download_and_gunzip(self, filename, url):
        self.DownloadKit.add(url, rename=filename).wait(show=True)
        subprocess.run(["gunzip", os.path.join(self.data_path, filename)])
        # print(f"{filename} download successfully.")

    def start(self):
        urls = [(self.config.get("cc_links_filename"), self.config.get("cc_links_url")),
                (self.config.get("pc_links_detailed_filename"), self.config.get("pc_links_detailed_url")),
                (self.config.get("pc_links_transfer_filename"), self.config.get("pc_links_transfer_url")),
                (self.config.get("actions_filename"), self.config.get("actions_url"))]

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