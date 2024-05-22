'''
@Author : gaoqingshan
@Time   : 2023/10/8 17:33
@Email  : 519546702@qq.com
@function This class downloads TSV files from the bindingdb database
'''
import configparser
import argparse
import io
import zipfile
import requests
import logging
from tqdm import tqdm

config = configparser.ConfigParser()
# cfgfile = "./conf/drugkb_test.config"
cfgfile = r"D:\PharmData\PharmDataProject\conf\drugkb_test.config"
config.read(cfgfile)


def download_with_progress(url, dest_path):
    try:
        # Send the request and get the data
        response = requests.get(url, stream=True)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error("Request failed: %s", e)
        return

    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024  # 1 Kibibyte
    t = tqdm(total=total_size, unit='iB', unit_scale=True)

    with io.BytesIO() as file_stream:
        for data in response.iter_content(block_size):
            t.update(len(data))
            file_stream.write(data)
        t.close()

        file_stream.seek(0)
        with zipfile.ZipFile(file_stream) as zip_ref:
            zip_ref.extractall(dest_path)
        logging.info("The TSV file is saved")


def bindingdbDownloader():
    url = args.source_url_1
    download_with_progress(url, args.extract_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='downloadFromDB.py, This class downloads TSV files from the bindingdb database')
    parser.add_argument('source_url_1', help='Request address')
    parser.add_argument('extract_dir', help='Specify the decompression directory')
    args = parser.parse_args([config.get('bindingdb', 'source_url_1'), '../../data/bindingdb'])
    bindingdbDownloader()
