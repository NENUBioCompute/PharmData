# Title     : myGetData.py.py
# Created by: julse@qq.com
# Created on: 2021/5/28 17:06
# des : download zip data and unzip data
import configparser
import os


class PharmRGKBDownloader:
    def __init__(self):
        pass

    def download(self, url,dir):
        os.system('wget %s -O %s'%(url,dir))

    def un_gz(self, file_name):
        f = file_name.replace(".zip", "")
        os.system('unzip %s -d %s'%(file_name,f))

def main(url, dir):
    print('start download')
    PharmRGKBDownloader.download(url,dir)
    PharmRGKBDownloader.un_gz(dir)

#########################################################################################################################################
if __name__ == '__main__':

    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)
    '''
    download and extract source data
    '''
    for idx in range(1,int(config.get('pharmgkb', 'col_num'))+1):
        source_dir = config.get('pharmgkb', 'source_url_%d'%idx)
        data_dir = config.get('pharmgkb', 'data_path_%d'%idx)
        main(source_dir,data_dir)


