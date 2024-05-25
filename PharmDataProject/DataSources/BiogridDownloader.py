import os
import psutil
import logging
import requests
import urllib.error
from zipfile import ZipFile
from PharmDataProject.Utilities.FileDealers.FileSystem import folder_is_exists
from PharmDataProject.Utilities.NetDownloads.HttpDownloader import HTTP
from tqdm import tqdm

class BiogridDownload:

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

    def start(self, logPath, sourceUrl, folderPath, fileName):
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
        fh = logging.FileHandler(logPath)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        try:
            self.curState = 2
            folder_is_exists.__func__(folderPath)
            self.pid = os.getpid()  # Set the current process ID
            logger.info('Downloading')
            localPath = self.download_with_progress(sourceUrl, folderPath, fileName)
            logger.info('Download completed. Extracting...')
            self.extract_with_progress(localPath, folderPath)

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
    BiogridDownload().start('./test.log',
                            'https://downloads.thebiogrid.org/Download/BioGRID/Release-Archive/BIOGRID-4.4.224/BIOGRID-ALL-4.4.224.mitab.zip', '../../data/biogrid/', 'biogrid_data.zip')
