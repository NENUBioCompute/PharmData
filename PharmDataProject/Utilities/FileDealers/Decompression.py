# encoding: utf-8
'''
@Author  : Wenjie.Lee
@Time    : 2022/10/26 19:56
@desc    : 
'''
import gzip
import os


class Decompression:

    def __unZIP(self, fileName:str):
        '''
        @param fileName: file name
        @return: None
        '''
        f = fileName.replace(".zip", "")
        os.system('unzip %s -d %s' % (fileName, f))

    def __unGZ(self, fileName:str):
        '''
        @param fileName
        @return: None
        '''
        fName = fileName.replace(".gz", "")
        gFile = gzip.GzipFile(fileName)
        open(fName, "wb+").write(gFile.read())
        gFile.close()

    def start(self, fileName):
        '''
        @param fileName
        @return
        '''
        if('zip' in fileName):
            self.__unZIP(fileName)
        elif('gz' in fileName):
            self.__unGZ(fileName)



if __name__ == '__main__':
    decompression = Decompression()
    decompression.start("../../data/twosides/offsides/OFFSIDES.csv.gz")
    decompression.start("../../data/pharmgkb/genes.zip")