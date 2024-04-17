"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 4/13/2024 4:44 PM
  @Email: deepwind32@163.com
"""
import os
import subprocess
import threading

from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser
from PharmDataProject.Utilities.NetDownloads.HttpDownloader import HTTP

"""
Download Page: https://smpdb.ca/downloads
"""


class SMPDBDownloader:
    def __init__(self, config):
        self.config = config
        self.data_path = config.get("smpdb", "data_path")

    def __download_and_unzip(self, url):
        filename = url.split("/")[-1]
        HTTP.get_data(url, self.data_path, filename)
        subprocess.run(
            args=["unzip", "-q", "-n", self.data_path + filename, "-d", self.data_path + filename.split(".")[0]],
            check=True, bufsize=4096)
        os.remove(self.data_path + filename)

    def start(self):
        urls = [self.config.get("smpdb", "pathways_csv_data_source"),
                self.config.get("smpdb", "metabolite_names_data_source"),
                self.config.get("smpdb", "protein_names_data_source"), self.config.get("smpdb", "sbml_data_source")]

        threads = [threading.Thread(target=self.__download_and_unzip, args=(url,)) for url in urls]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()


if __name__ == "__main__":
    cfg = "/home/zhaojingtong/tmpcode/PharmData/PharmDataProject/conf/drugkb.config"
    config = ConfigParser.get_config(cfg)
    SMPDBDownloader(config).start()
