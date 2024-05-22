#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：DrugMap
@File    ：TxtFileDealer.py
@Author  ：Cecilia Cai
@Email   ：1594589425@qq.com
@Date    ：2022/10/16 18:34
'''

import os
import shutil
import re

class TxtFileDealer:
    """
       Process TXT files
    """
    def __init__(self):
        pass
    @staticmethod
    def filewriter(file_dir:str, save_dir:str, suffix:str):
        """
         description:Write different type file to destination folder
        :param file_dir: source folder
        :param save_dir: destination folder
        :param suffix: file type ".txt/.csv/.xml/.tsv/..."
        :return:
        """

        if not os.path.exists(save_dir):
            os.makedirs(save_dir)  # Create the directory if it doesn't exist
        filelist = []  # Store the full name of the file to be copied
        for dirpath, dirnames, filenames in os.walk(file_dir):
            for file in filenames:
                file_type = file.split('.')[-1]
                if (file_type in suffix):
                    file_fullname = os.path.join(dirpath, file)
                    filelist.append(file_fullname)
        for file in filelist:
            shutil.copy(file, save_dir)

    @staticmethod
    def IterRead(file: str, num_read: int = 1, drop: str = r"", favor: str = r"") -> iter:
        """
        :param file: str File full path
        :param num_read:int The number of lines read each time
        :param drop: Drop the line/s including the given value
        :param favor: Keep the line/s including the given value
        :return:generator
        """
        count = 0
        lines = []
        with open(file, 'r') as f:
            for aline in f:
                count += 1
                lines.append(aline.strip())
                if count >= num_read:
                    yield lines
                    # Free buffer zone
                    count = 0
                    lines.clear()
            # yield the reset lines counted less than num_read
            if count > 0:
                yield lines

    @staticmethod
    def IterReadMatrix(file: str, rows: range, cols: range, col_delimiter: str = r" ") -> iter:
        """
        :param file:str File full path
        :param rows:The range of rows the file needs to be split on
        :param cols:The range of columns the file needs to be split on
        :param col_delimiter: the delimiter/s may be used in the text file, examples: r"[ \n\t;,!]"
        """
        with open(file, 'r') as f:
            for i in range(rows.start):
                next(f)
            for i in range(rows.start, rows.stop):
                aline = f.readline()
                if not aline:
                    break
                elements = re.split(col_delimiter, aline)
                col_len = len(elements)
                if cols.stop < col_len:
                    yield elements[cols.start:cols.stop]
                else:
                    yield elements[cols.start:col_len-1]

    @staticmethod
    def FileLen(file:str):
        File = TxtFileDealer.IterRead(file, 1)
        File_len = 0
        for aline in File:
            File_len +=1
        return File_len


if __name__ == "__main__":
    # Example for IterRead
    # test = TXTFileDealer()
    # result = test.IterRead("TXT_test.txt", 3)
    # for line in result:
    #     print(line)
    #
    # Example for IterReadMatrix
    # test = TXTFileDealer()
    # result = test.IterReadMatrix("TXT_test.txt", range(0, 3), range(0, 2), r"[ \n]")
    # for line in result:
    #     print(line)
    # Example for IterReadMatrix
    # file_len = TXTFileDealer().FileLen("E:/net download/GSE62867_series_matrix.txt")
    # print(file_len)
    test_file = TxtFileDealer.filewriter("E:/net download/","E:/test/",".txt")
    file = TxtFileDealer.IterReadMatrix("E:/net download/GSE62867_series_matrix.txt", range(27650, 30000), range(0, 24), r"[ \n\t]")
    for line in file:
        print(line)
