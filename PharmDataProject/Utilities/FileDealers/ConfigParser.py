#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2022/10/14 10:00
# @Author  : ouyangxike
# @FileName: ConfigParser.py
# @Software: PyCharm
import configparser
import os.path


class ConfigParser:

    @staticmethod
    def GetConfig(path):
        '''
        Read the configuration file
        @return:config object
        '''
        config = configparser.ConfigParser()
        if not os.path.exists(path):
            return None
        else:
            cfgfile = path
            config.read(cfgfile)
            if len(config) == 1:
                return None
            else:
                return config


if __name__ == '__main__':
    ConfigParser.GetConfig(r'C:\Users\15717\Desktop\DrugMapProject\Data\conf\drugkb.config')
