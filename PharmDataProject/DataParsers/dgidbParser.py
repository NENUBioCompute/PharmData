# -*- coding: utf-8 -*-
# @Time    : 2023/10/12 10:49
# @Author  : jiangwenjun
# @File    : info
# @Software: PyCharm
# @ProjectName: python
import csv

class CSVParser:
    def __init__(self, file_path):
        self.file_path = file_path

    def parse_csv(self):
        try:
            with open(self.file_path, newline='', encoding='utf-8') as csvfile:
                csv_reader = csv.reader(csvfile)
                data = []
                for row in csv_reader:
                    data.append(row)
                return data
        except FileNotFoundError:
            pass
        except Exception as e:
            pass

# 使用示例
# if __name__ == "__main__":
#     csv_file_path = r"dgidb文件下载路径"
#     parser = CSVParser(csv_file_path)
#     parsed_data = parser.parse_csv()
#     if parsed_data:
#         for row in parsed_data:
#             print(row)

