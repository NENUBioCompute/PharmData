# Title     : getdata.py
# Created by: julse@qq.com
# Created on: 2021/6/23 14:00
import configparser
import time
import os


def download(url,fout):
    if os.access(fout,os.F_OK):
        print('find ',fout)
    else:
        assert 'Please register to download the BRENDA Textfile! %s'%url
def un_gz(file_name):
    f_name = file_name[:file_name.rindex('/')]
    print('tar -zxvf %s -C %s'%(file_name,f_name))
    os.system('tar -zxvf %s -C %s'%(file_name,f_name))

def main(url, dir):
    print('start download')
    download(url,dir)
    un_gz(dir)
if __name__ == '__main__':
    print('start', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    start = time.time()
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)
    section = 'brenda'
    tables = config.get(section, 'tables')[1:-1].split(',')
    '''
    download and extract source data
    '''
    for idx in range(int(config.get(section, 'col_num'))):
        source_dir = config.get(section, 'source_url_%d'%(+1))
        data_dir = config.get(section, 'data_path_%d'%(idx+1))
        main(source_dir,data_dir)
    print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('time', time.time() - start)


