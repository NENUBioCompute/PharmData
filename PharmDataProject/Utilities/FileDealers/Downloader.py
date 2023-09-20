#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：DrugMap 
@File    ：Downloader.py
@Author  ：Cecilia Cai
@Email   ：1594589425@qq.com
@Date    ：2022/10/21 14:51
'''

import psutil
class Downloader:

    def __init__(self):
        self.curState = 0
        self.pid = None

    def CheckStatus(self):
        '''
        Check the current download status
        @return:
        '''
        state = ['Pause', 'Stop', 'Downloading', 'Downloaded' 'Failed']
        return state[self.curState]

    def Pause(self):
        '''
        Pause the download
        @return:
        '''
        self.curState = 0
        p = psutil.Process(self.pid)
        p.suspend()

    def Stop(self):
        '''
        Stop download
        @return:
        '''
        self.curState = 1
        p = psutil.Process(self.pid)
        p.kill()

    def Wake(self):
        '''
        Resume download
        @return:
        '''
        self.curState = 2
        p = psutil.Process(self.pid)
        p.resume()

    def Start(self, config_path, log_path):
        '''
        Download
        :param config_path:configure file path
        :param log_path:log file path
        :return
        '''
        pass


if __name__ == '__main__':
    pass