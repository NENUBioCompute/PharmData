# -*- coding: utf-8 -*-
# @Time : 2023/10/24 20:30
# @Author : zhudexu
# @FileName: pubchem(2).py
# @Software: PyCharm
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient
import requests
import gzip
import xmltodict
import time


def connect_to_mongodb(mongodb_url, mongo_db, collection_name):
    client = MongoClient(mongodb_url)
    db = client[mongo_db]
    collection = db[collection_name]
    return collection


def download_and_convert_xml(download_url, collection):
    # 下载压缩文件
    response = requests.get(download_url)
    compressed_data = response.content
    print(f"{download_url}下载完成")

    # 解压缩gzip文件
    uncompressed_data = gzip.decompress(compressed_data)

    # 将XML转换为字典对象
    data_dict = xmltodict.parse(uncompressed_data)
    print(f"{download_url}转换完成")
    # 将字典对象插入MongoDB集合
    collection.insert_many(data_dict["PC-Compounds"]["PC-Compound"])

    print(f"{download_url}转换完成，数据已保存到MongoDB数据库。")


if __name__ == "__main__":
    time_start = time.time()
    # 定义url名前缀和后缀
    prefix = "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/XML/Compound_"
    suffix = ".xml.gz"
    url_list = []
    # 定义开始和结束编号以及步长
    start_num = 1
    end_num = 169000000
    step = 500000
    # 循环生成url名
    for i in range(start_num, end_num + 1, step):
        url_name = f"{prefix}{i:09d}_{i + step - 1:09d}{suffix}"
        # print(url_name)
        url_list.append(url_name)
    print(url_list)
    print(len(url_list))
    # 定义下载链接、MongoDB配置等参数
    # download_url = "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/XML/Compound_000000001_000500000.xml.gz"
    mongodb_url = 'mongodb://localhost:27017'
    mongo_db = "PharmRG"
    collection_name = "PubChem_new"

    # 连接MongoDB数据库
    collection = connect_to_mongodb(mongodb_url, mongo_db, collection_name)


    # 使用线程池执行处理函数
    with ThreadPoolExecutor(max_workers=1) as executor:
        # executor.submit(download_and_convert_xml, download_url, collection)
        for url in url_list:
            executor.submit(download_and_convert_xml, url, collection)

    time_end = time.time()

    time_sum = time_end - time_start  # 计算的时间差为程序的执行时间，单位为秒/s
    print(time_sum)
