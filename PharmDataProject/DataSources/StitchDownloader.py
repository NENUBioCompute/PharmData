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
        self.data_path = config.get("stitch", "data_path")
        self.DownloadKit = DownloadKit(self.data_path)

    def __download_and_gunzip(self, url):
        self.DownloadKit.add(url, rename=url.split("/")[-1]).wait(show=False)

    def start(self):
        urls = [self.config.get("stitch", "cc_links_url"),
                self.config.get("stitch", "pc_links_detailed_url"),
                self.config.get("stitch", "pc_links_transfer_url"),
                self.config.get("stitch", "actions_url")]

        threads = [threading.Thread(target=self.__download_and_gunzip, args=(url,)) for url in urls]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()


if __name__ == "__main__":
    cfg = "/home/zhaojingtong/tmpcode/PharmData/PharmDataProject/conf/drugkb.config"
    config = ConfigParser.GetConfig(cfg)
    StitchDownloader(config).start()