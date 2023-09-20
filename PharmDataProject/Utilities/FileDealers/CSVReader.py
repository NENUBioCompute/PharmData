# -*- coding: utf-8 -*-
# @Time    : 2022/7/29 20:41
# @Author  : ouyangxike
# @Email   : 1586658982@qq.com
# @File    : CSVReader.py
# @Software: PyCharm
import csv

class CSVReader:
    """
    Process files in CSV format.
    """
    @staticmethod
    def IterRead(file:str, num_read: int = 1,col_delimiter: str = " " ,drop:str = r"", favor:str = r"")->iter:

        count = 0
        lines = []
        csv.register_dialect('mydialect', delimiter=col_delimiter, quoting=csv.QUOTE_ALL)
        with open(file, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile, dialect='mydialect')
            for aline in csv_reader:
                count += 1
                lines.append(aline)
                if count >= num_read:
                    yield lines
                    count = 0
                    lines.clear()
            if count > 0:
                yield lines


    @staticmethod
    def IterReadMatrix(file:str, rows:range, cols:range, col_delimiter: str = '') -> iter:

        csv.register_dialect('mydialect', delimiter=col_delimiter, quoting=csv.QUOTE_ALL)
        dicts = {}

        with open(file, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile, dialect='mydialect')
            result = list(csv_reader)
            for aline in result[rows.start:rows.stop]:
                for j in range(cols.start, cols.stop):
                    new_aline = list(aline.items())
                    dicts[new_aline[j][0]] = new_aline[j][1]
                yield dicts

if __name__ == '__main__':

    iter_reader = CSVReader.IterRead('../../data/dgidb/categories/categories.tsv', 5, '\t')
    for lines in iter_reader:
        for aline in lines:
            print(aline)
