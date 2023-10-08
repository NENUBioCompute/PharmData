# -*- coding: utf-8 -*-
"""
# @Time        : 2023/10/8
# @Author      : yvshilin
# @Email       : 3542804395@qq.com
# @FileName    : ChEMBLDownloader.py
# @Software    : PyCharm
# @ProjectName : ChEMBL
"""
import urllib.request
import urllib.error
import time
import socket
import zipfile
import ssl

socket.setdefaulttimeout(20)

class DownloadAndUnzip:
    def RecuDown(self, url, savePath):  # recurrent download with ContentTooShortError
        try:
            ssl._create_default_https_context = ssl._create_unverified_context
            urllib.request.urlretrieve(url, savePath)
        except urllib.error.ContentTooShortError:
            self.RecuDown(url, savePath)
        time.sleep(1)

    def UnzipFile(self, zipFile, extractFolder):
        with zipfile.ZipFile(zipFile, 'r') as zipRef:
            zipRef.extractall(extractFolder)
