# -*- coding: utf-8 -*-
# @Author   : H.Wang
# @Time     : 2022/6/10 11:40
# @File     : CSVDealer.py
# @Project  : DrugMapProject
import os
import csv
class CSVWriter:
    """
        CSV file writer
    """

    def __init__(self, filename, headers=[]):
        self.filename = filename
        self.headers = headers
        if os.path.exists(filename):
            raise Exception("File already exists.")
        self.f = open(filename, 'a', newline='', encoding='utf-8')
        writer = csv.DictWriter(self.f, headers)
        writer.writeheader()
        self.writer = csv.DictWriter(self.f, self.headers)

    def save_single_data(self, data: dict):
        """
        Save a single data to csv file.
        :param data: data to be saved
        """
        self.writer.writerow(data)

    def save_many_data(self,data: list):
        """
        Save many data to csv file.
        :param data: data to be saved
        """
        self.writer.writerows(data)

    def save_to_specific_location(self, new_path):
        """
        Move file to somewhere else.
        :param new_path: new full file path
        """
        self.f.close()
        os.rename(self.filename, new_path)
        self.f = open(self.filename, 'a', newline='', encoding='utf-8')
    def close(self):
        """
        Close the csv file.
        """
        self.f.close()
if __name__ == '__main__':
    csv_handler = CSVWriter('test.csv', ['a', 'b', 'c'])
    csv_handler.save_single_data({'a': 1, 'b': 2, 'c': 3})
    csv_handler.save_single_data({'a': 4, 'b': 5, 'c': 6})
    csv_handler.save_single_data({'a': 7, 'b': 8, 'c': 9})
    csv_handler.save_to_specific_location('test2.csv')
    csv_handler.close()