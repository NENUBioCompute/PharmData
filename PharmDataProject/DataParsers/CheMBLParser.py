"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2023/12/14 17:29
  @Email: 2665109868@qq.com
  @function
"""
import pymysql
import pandas as pd

def get_mysql_conn():
    # 设置数据库连接参数
    db_config = {
        'host': '59.73.198.168',
        'user': 'root',
        'password': '001231',
        'database': 'chembl_33',
    }
    # 创建连接
    conn = pymysql.connect(**db_config)
    return conn
