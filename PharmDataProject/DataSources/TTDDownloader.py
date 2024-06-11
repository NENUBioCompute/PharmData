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
        ata source "ttd" download
        @param configFile: configuration file
        @param xlsxPath:
        @param tsvPath:
        @return: "ttd" file folder
        """
        cfg = configparser.ConfigParser()
        cfg.read('../conf/drugkb.config')
        # Log information
        logger = logging.getLogger('TtdDownloader')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')


        if cfg == None:
            logger.critical("File Error")
            raise FileNotFoundError

        sourceNum = int(cfg.get('ttd', 'source_url_num'))
        for i in range(1, sourceNum + 1):
            sourceDir = cfg.get('ttd', 'source_url_' + str(i))
            dataDir = cfg.get('ttd', 'data_path_' + str(i))  # Local storage directory
            folderPath, fileName = os.path.split(dataDir)  # file name

            try:
                self.curState = 2
                # print(sourceDir)
                # print(folderPath)
                # print(fileName)
                self.pid = HTTP.DownLoad(sourceDir, folderPath + '/', fileName)  # Download
                logger.info('Downloading')

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

        # # Special disposal under special circumstances
        # xlsxFolder, xlsxFile = os.path.split(xlsxPath)
        # xlsx_to_tsv(xlsxPath, tsvPath, 'Sheet1')
        # del_file.__func__(xlsxFolder, xlsxFile)


if __name__ == '__main__':
    # TtdDownloader().start(r'../../../conf/drugkb.config',  './static.log', '../../../data/ttd/target2drug.xlsx', '../../../data/ttd/target2drug.tsv')
    pass
    ttddownloader = TTdDownloader()
    ttddownloader.start()