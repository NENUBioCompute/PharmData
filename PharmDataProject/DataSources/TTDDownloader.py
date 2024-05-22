# -*-coding:utf-8-*-
"""
@Time    : 2022/7/20
@Auth    : song
@File    : TTDDownloader.py
@IDE     : PyCharm
@Edition : 004
@Describe: Data source "ttd" download
"""
import psutil
import logging
import requests
import urllib.error
import configparser
from tqdm import tqdm
import os
import time
from PharmDataProject.Utilities.FileDealers.FileSystem import *
from PharmDataProject.Utilities.NetDownloads.HttpDownloader import HTTP
from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser

class TTdDownloader:

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

    def start(self):
        """
        Data source "ttd" download
        @param configFile: configuration file
        @param xlsxPath:
        @param tsvPath:
        @return: "ttd" file folder
        """
        cfg = configparser.ConfigParser()
        cfg.read('../conf/drugkb_test.config')
        # Log information
        logger = logging.getLogger('TtdDownloader')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler = logging.FileHandler('ttd_downloader.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        if cfg is None:
            logger.critical("File Error")
            raise FileNotFoundError

        sourceNum = int(cfg.get('ttd', 'source_url_num'))
        for i in range(1, sourceNum + 1):
            sourceDir = cfg.get('ttd', 'source_url_' + str(i))
            dataDir = cfg.get('ttd', 'data_path_' + str(i))  # Local storage directory
            folderPath, fileName = os.path.split(dataDir)  # file name

            # Check if the file already exists
            if os.path.exists(dataDir):
                logger.info(f"File already exists: {dataDir}")
                continue

            try:
                self.curState = 2
                logger.info(f'Starting download: {fileName} from {sourceDir}')
                self.pid = HTTP.download(sourceDir, folderPath + '/', fileName)  # Download
                logger.info('Downloading')

                with tqdm(total=100, desc=fileName, unit='B', unit_scale=True, unit_divisor=1024) as pbar:
                    for _ in range(100):
                        pbar.update(1)
                        time.sleep(0.1)  # Simulate the download progress

            except urllib.error.URLError as e:
                self.curState = 4
                logger.error(f'URLError: {e}')
                raise urllib.error.URLError

            except requests.exceptions.ConnectionError as e:
                self.curState = 4
                logger.error(f'ConnectionError: {e}')
                raise requests.exceptions.ConnectionError

            except requests.exceptions.ChunkedEncodingError as e:
                self.curState = 4
                logger.error(f'ChunkedEncodingError: {e}')
                raise requests.exceptions.ChunkedEncodingError

            except Exception as e:
                self.curState = 4
                logger.error(f'Unknown Error: {e}')
                raise e

            else:
                self.curState = 3
                logger.info(f'{fileName} Success')

            finally:
                pass


if __name__ == '__main__':
    ttddownloader = TTdDownloader()
    ttddownloader.start()
