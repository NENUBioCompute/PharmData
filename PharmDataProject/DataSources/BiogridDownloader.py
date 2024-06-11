# -*- coding: utf-8 -*-
"""
# @Time        : 2023/1/9
# @Author      : tanliqiu
# @FileName    : BiogridDownloader.py
# @Software    : PyCharm
# @ProjectName : Biogrid
"""

import os
import psutil
import logging
import requests
import urllib.error
from zipfile import ZipFile
from FileSystem import folder_is_exists
from HttpDownloader import HTTP
import wget
# from DrugMapProject.Utilities.NetDownloads.HttpDownloader import HTTP
# from DrugMapProject.Utilities.FileDealers.ConfigParser import ConfigParser


class DrugsDownload:

    def __init__(self):
        self.curState = 0
        self.pid = None

    def checkStatus(self):
        '''
        Check the current download status
        @return:
        '''
        state = ['Pause', 'Stop', 'Downloading', 'Downloaded', 'Failed']
        return state[self.curState]

    def pause(self):
        '''
        Pause the download
        @return:
        '''
        self.curState = 0
        p = psutil.Process(self.pid)
        p.suspend()

    def stop(self):
        '''
        Stop download
        @return:
        '''
        self.curState = 1
        p = psutil.Process(self.pid)
        p.kill()

    def wake(self):
        '''
        Resume download
        @return:
        '''
        self.curState = 2
        p = psutil.Process(self.pid)
        p.resume()

    # def start(self, configFile, logPath):
    def start(self, logPath,sourceUrl,folderPath,fileName):
        """
        Data source "do" download
        @param configFile: config file
        @param logPath: log file path
        @return: "do" file folder
        """
        # Log information
        logger = logging.getLogger('DoDownloader')
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler(logPath)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        try:
            self.curState = 2
            folder_is_exists.__func__(folderPath)
            self.pid = HTTP.DownLoad(sourceUrl, folderPath, fileName)  # Download
            logger.info('Downloading')
            localPath = os.path.join(folderPath, fileName)
            wget.download(sourceUrl, localPath)
            myzip = ZipFile(localPath)
            myzip.extractall(path=folderPath)
            # unzip
            # The successful operation here depends on deleting the ". zip" part of the Download function of the HttpDownloader class


        except urllib.error.URLError:
            self.curState = 4
            logger.error('URLError')
            raise urllib.error.URLError

        except requests.exceptions.ConnectionError:
            self.curState = 4
            logger.error('ConnectionError')
            raise requests.exceptions.ConnectionError

        except requests.exceptions.ChunkedEncodingError:
            self.curState = 4
            logger.error('ChunkedEncodingError')
            raise requests.exceptions.ChunkedEncodingError

        except Exception as e:
            self.curState = 4
            logger.error('Unknown Error')
            raise e

        else:
            self.curState = 3
            logger.info(f'{fileName} Success')

        finally:
            pass
        return folderPath


if __name__ == "__main__":
    DrugsDownload().start('./static.log','https://downloads.thebiogrid.org/Download/BioGRID/Release-Archive/BIOGRID-4.4.224/BIOGRID-ALL-4.4.224.mitab.zip','./Biogrid_data/','drugs_data.zip')
    # pass
