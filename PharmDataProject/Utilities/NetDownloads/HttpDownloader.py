'''
@author: SHENGKUN
@Time  : 2022/10/02 12:28
'''

import time
import urllib
import logging
import requests
import subprocess
from multiprocessing import Process
from threading import Thread
from urllib import request, error
from PharmDataProject.Utilities.FileDealers.FileSystem import *
import wget


class HTTP:
    """
    Downloading specific data source files with responding url in config file.
    """

    @staticmethod
    def download(url: str, local_path: str, file_name: str, ):
        p1 = Process(target=HTTP.get_data, args=(url, local_path, file_name), name=file_name)
        p1.start()
        return os.getpid()

    @staticmethod
    def download_with_thread(url: str, local_path: str, file_name: str, block: bool = True):
        thread = Thread(target=HTTP.get_data, args=(url, local_path, file_name), name=file_name)
        thread.start()
        if block:
            thread.join()

    def get_data(url: str, local_path: str, file_name: str):
        log_path = './test.log'
        logger = logging.getLogger('HttpDownloader')
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler(log_path)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.info('supprocess : %s，parent process : %s' % (os.getpid(), os.getppid()))

        try:
            start = time.time()
            folder_is_exists.__func__(local_path)
            file_is_exists.__func__(file_name)
            # print(f"wget {url} -O {local_path + file_name}")
            # subprocess.call(f"wget {url} -O {local_path + file_name}", timeout=600)
            wget.download(url, local_path + file_name)

        except subprocess.TimeoutExpired as e:
            raise subprocess.SubprocessError("%s was terminated as of timeout!!" % e.cmd)

        except error.URLError as e:
            raise urllib.error.URLError(" :TIME OUT!!")

        except requests.exceptions.ConnectionError:
            logger.error('ConnectionError')
            raise requests.exceptions.ConnectionError

        except requests.exceptions.ChunkedEncodingError:
            logger.error('ChunkedEncodingError')
            raise requests.exceptions.ChunkedEncodingError

        except Exception as e:
            logger.info("UnKnown error ：{}".format(e))

        else:
            logger.info(f'{file_name} Success')
        finally:
            end = time.time()
            timeusage = end - start
            logger.info("Time usage: %.2f" % (end - start) + " second")
        return timeusage


if __name__ == '__main__':
    url = input("url：")
    local_path = input("local_path：")
    file_name = input("file_name：")

    task = HTTP.download(url, local_path, file_name)

    # task = HTTP.DownLoad('https://smpdb.ca/downloads/smpdb_pathways.csv.zip',
    #                   '../../data_kk/smpdb/pathway/', '6')
