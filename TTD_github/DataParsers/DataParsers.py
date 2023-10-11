
import xlrd2
import json


# 数据库解析
def DataParsers(name1):


    # 读取Excel文件
    data = xlrd2.open_workbook(name1)
    table = data.sheets()[0]

    # 读取excel第一行数据作为存入mongodb的字段名
    row_stag = table.row_values(0)
    n_rows = table.nrows
    return_data = {}
    # 原始数据

    k = 0

    # 上传数据
    for i in range(1, n_rows):
        # 将字段名和excel数据存储为字典形式，并转换为json格式
        return_data[i] = json.dumps(dict(zip(row_stag, table.row_values(i))))
        # 通过编解码还原数据
        return_data[i] = json.loads(return_data[i])

    return return_data
