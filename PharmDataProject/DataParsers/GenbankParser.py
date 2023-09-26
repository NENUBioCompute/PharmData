"""
  -*- encoding: utf-8 -*-
  @Author: wangyang
  @Time  : 2023/09/26 17:50
  @Email: 2168259496@qq.com
  @function
"""

import CsvParser as csv

path = {'target': "D:\summer\chembl\\target.tsv"}

loca = csv.CsvParser(csv_path=path, db_name='chembl')
#
# loca.insert_data(col_name='target', buffer_size=100000)

loca.set_fields(fields_removed=['ChEMBL ID', 'Name', 'Organism'])
