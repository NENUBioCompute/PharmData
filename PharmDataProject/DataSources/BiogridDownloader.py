import os
import psutil
import logging
import requests
import urllib.error
from zipfile import ZipFile
from PharmDataProject.Utilities.FileDealers.FileSystem import folder_is_exists
from PharmDataProject.Utilities.NetDownloads.HttpDownloader import HTTP
from tqdm import tqdm
import configparser
class BiogridDownload:

    def __init__(self):
        self.curState = 0
        self.pid = None
        self.config = configparser.ConfigParser()
        self.config.read('../conf/drugkb_test.config')
        self.source_url = self.config.get('biogrid','source_url_1')
        self.save_path = self.config.get('biogrid', 'data_path_1')

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

    def download_with_progress(self, url, folderPath, fileName):
        localPath = os.path.join(folderPath, fileName)
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024  # 1 Kilobyte
        t = tqdm(total=total_size, unit='iB', unit_scale=True, desc='Downloading')

        with open(localPath, 'wb') as file:
            for data in response.iter_content(block_size):
                t.update(len(data))
                file.write(data)
        t.close()
        return localPath

    def extract_with_progress(self, zipPath, extractPath):
        with ZipFile(zipPath, 'r') as zip_ref:
            total_files = len(zip_ref.infolist())
            with tqdm(total=total_files, unit='file', desc='Extracting') as t:
                for file in zip_ref.infolist():
                    zip_ref.extract(file, extractPath)
                    t.update(1)

    def start(self):
        """
        Data source "do" download
        @param logPath: log file path
        @param sourceUrl: source URL
        @param folderPath: folder path to save downloaded file
        @param fileName: name of the downloaded file
        @return: folder path
        """
        # Log information
        logger = logging.getLogger('BiogridDownloader')
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler('./text.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        try:
            self.curState = 2
            folder_is_exists.__func__(self.save_path)
            self.pid = os.getpid()  # Set the current process ID
            logger.info('Downloading')
            localPath = self.download_with_progress(self.source_url, self.save_path, 'biogrid_data.zip')
            logger.info('Download completed. Extracting...')
            self.extract_with_progress(localPath, self.save_path)

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


        finally:
            pass
    pass


if __name__ == "__main__":
    BiogridDownload().start()
