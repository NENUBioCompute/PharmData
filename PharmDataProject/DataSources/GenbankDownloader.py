"""
  -*- encoding: utf-8 -*-
  @Author: wangyang
  @Time  : 2023/10/04 19:38
  @Email: 2168259496@qq.com
  @function
"""
import requests
import patoolib
import os
class GenebankDownloader:

    """
    The GenbankDownloader class

    """

    def __init__(self, url_info: str = "https://ftp.ncbi.nih.gov/gene/DATA/gene_info.gz", url_pubmed: str = "https://ftp.ncbi.nih.gov/gene/DATA/gene2pubmed.gz",
                 dest_path: str = "."):

        self.url_info= url_info
        self.url_pubmed = url_pubmed
        self.info_path = dest_path+"/gene_info.gz"
        self.pubmed_path = dest_path+"/gene2pubmed.gz"
        self.dest_path = dest_path

    def extract_rar(self, file_path, dest_path):
        patoolib.extract_archive(file_path, outdir=dest_path)


    def download(self, url, file_path, dest_path) -> None:

        response = requests.get(url, stream=True)
        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
        self.extract_rar(file_path, dest_path)
        os.remove(file_path)


    def genebank_download(self) -> None:
        self.download(self.url_pubmed, self.pubmed_path, self.dest_path)
        self.download(self.url_info, self.info_path, self.dest_path)



