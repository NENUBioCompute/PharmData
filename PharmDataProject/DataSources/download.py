'''
@Author: wangbo
@Email: 1808274540@qq.com
@Description: crawler
@Function:get data
@Date: 2023-09-30 10:10:10
@LastEditTime: 2023-09-30 10:10:10
@LastEditors: wangbo
'''
import requests
import zipfile
import tarfile

class sourse:
    def __int__(self,url,filename,filename_zip):
        self.url=url
        self.filename=filename
        self.filename_zip=filename_zip

        """url = https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.xml.gz # URL of compressed file
            filename = archive.zip  # Saved File Name
            filename_zip #unzip file name
        """
    def download(self):
        # Initiate a request and download a compressed file
        response = requests.get(self.url, stream=True)
        response.raise_for_status()
        # Check whether the request was successful
        # Save the compressed file
        with open(self.filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)


        # Unzip the file
    def decompression(self):
        if self.filename.endswith(".zip"):
            with zipfile.ZipFile(self.filename, "r") as zip_ref:
                zip_ref.extractall(self.filename_zip)
        elif self.filename.endswith((".tar.gz", ".tgz")):
            with tarfile.open(self.filename, "r:gz") as tar_ref:
                tar_ref.extractall(self.filename_zip)
        elif self.filename.endswith(".tar"):
            with tarfile.open(self.ilename, "r:") as tar_ref:
                tar_ref.extractall(self.filename_zip)
