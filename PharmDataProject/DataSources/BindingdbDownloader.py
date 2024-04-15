
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

config = configparser.ConfigParser()
cfgfile = "../conf/drugkb.config"
config.read(cfgfile)


def bindingdbDownloader():
    url = args.source_url_1

    try:
        # Send the request and get the data
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error("Request failed: %s", e)
        return

    if response.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            zip_ref.extractall(args.extract_dir)
        logging.info("The TSV file is saved")
        return
    else:
        logging.error("File download failure")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='downloadFromDB.py, This class downloads TSV files from the bindingdb database')
    parser.add_argument('source_url_1', help='Request address')
    parser.add_argument('extract_dir', help='Specify the decompression directory')
    args = parser.parse_args([config.get('bindingdb', 'source_url_1'), '../data/bindingdb'])
    bindingdbDownloader()