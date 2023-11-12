import requests
import random
import re
import time
import redis
import pymongo
import csv
import os

from lxml import etree
from concurrent.futures import ThreadPoolExecutor

class CSVHandler:
  '''
  你只需定义一个包含列标题元组，例如: headers = ['name', 'age', 'city']
  当你实例化CSVHandler类时，只需传入你希望使用的列标题，这样就可以在创建新CSV文件时自动添加列标题了
  '''
  def __init__(self, filename, headers = []):

      self.filename = filename
      self.headers = headers

      # 判断文件是否存在，如果不存在，则创建文件并初始化列标题
      if not os.path.exists(filename):
          with open(filename, 'w', newline='', encoding='utf-8') as f:
              writer = csv.DictWriter(f, headers)
              writer.writeheader()  # 写入列标题


  def save_single_data(self, data):
      """
      保存单条数据到CSV文件中
      :param data: 一个字典，其中的键应该和列标题对应
      """
      with open(self.filename, 'a', newline='', encoding='utf-8') as f:
          writer = csv.DictWriter(f, self.headers)
          writer.writerow(data)


  def save_to_specific_location(self, new_path):
      """
      将CSV文件保存到指定的位置
      :param new_path: 新文件的完整路径
      """
      os.rename(self.filename, new_path)


# 使用示例
# headers = ['name', 'age', 'city']
# handler = CSVHandler('data.csv', headers)
# data = {'name': 'John', 'age': 28, 'city': 'New York'}
# handler.save_single_data(data)
# handler.save_to_specific_location('new_data.csv')


class SpiderHandler:

    # 初始化随机user_agent
    def ua_init(self):
        # 生成一个随机的、伪造的User-Agent字符串
        # 每次发送请求时都会使用一个不同的User-Agent，帮助绕过某些基于User-Agent的简单反爬虫机制
        base_user_agent = 'Mozilla/5.0 (Windows NT {0}; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{1}.0.{2}.{3} Safari/537.36'
        user_agent = base_user_agent.format(random.choice(['6.1', '6.2'] + ['10.0' for i in range(7)]),
                                            random.choice([i for i in range(70, 99)]),
                                            random.choice([i + 1 for i in range(2200)]),
                                            random.choice([i + 1 for i in range(99)]),
                                            )
        return user_agent

    # 初始化请求头信息
    def __init__(self):

        # 将与HTTP请求一起发送的头信息
        self.headers = {
            "authority": "icd.who.int",
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9",
            "referer": "https://icd.who.int/dev11/l-m/en",
            "sec-ch-ua": "\"Chromium\";v=\"118\", \"Google Chrome\";v=\"118\", \"Not=A?Brand\";v=\"99\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": self.ua_init(),
            "x-requested-with": "XMLHttpRequest"
        }

        # Cookies通常用于存储会话数据
        self.cookies = {
            ".AspNetCore.Antiforgery.RtGCWVXC8-4": "CfDJ8Jq_h8XBjjNAtvvkag-LchhXr6BaeCxMfvI5beO_kxHscvDypFCrdCnHVx6oYi5WrIuAoPQw7UOQ52MU5LHq4mWHJRsepH0UTMnyWtrjRc7TOmLxalslFD0nNWGx6V5nK56lzDfKKPNz7tEkfG2YMXk",
            "ai_user": "qqDllWTs2S1EW2T5BgYf3A|2023-10-30T02:32:10.846Z"
        }

        # 初始化一个Redis客户端
        # self.redis_client = redis.Redis(host='127.0.0.1',port='6379')
        self.redis_client = redis.Redis()
        # try:
        #   self.redis_client = redis.Redis(host='172.29.129.100',port='6379')
        #   # 其他与Redis相关的代码
        # except redis.ConnectionError:
        #   print("无法连接到Redis服务")

        self.redis_name = 'ICD:deep_url'

    # 格式化字符串
    def deal_str(self, str1):

        # 对字符串进行清洗和格式化
        new_str = re.sub('\\s{2,}', ' ', re.sub('(\t|\r|\n|\xa0)+', ' ', str1).replace(',', '，').strip()).replace(
            '\u200b', '').replace("\u3000", '')

        return new_str

    # 获取父列表
    def get_Root(self):

        JsonGetRootConcepts_url = "https://icd.who.int/dev11/l-m/en/JsonGetRootConcepts"
        JsonGetRootConcepts_params = {
            "useHtml": "true"
        }
        while True:
            try:
                response = requests.get(JsonGetRootConcepts_url, headers=self.headers, cookies=self.cookies,
                                        params=JsonGetRootConcepts_params, timeout=5).json()
                break
            except Exception as e:
                self.headers['user-agent'] = self.ua_init()
                print(e)
        print('共有一级标签', len(response), '个')
        for respon in response:
            print('=====================正在解析第', response.index(respon) + 1, '个=========================')
            ID = respon.get('ID')
            isLeaf = respon.get('isLeaf')
            print(ID, isLeaf)
            self.get_Children(ID, isLeaf)

    # 获取子列表
    def get_Children(self, ID, isLeaf):

        if isLeaf:
            print('最底层子标签', ID, isLeaf)
            # 将最底层标签id存入redis
            self.redis_client.sadd(self.redis_name, ID)
            # self.redis_client.sadd(self.redis_name + '_bak', ID)
        else:
            JsonGetChildrenConcepts_url = "https://icd.who.int/dev11/l-m/en/JsonGetChildrenConcepts"
            params = {
                "ConceptId": ID,
                "useHtml": "true",
                "showAdoptedChildren": "true",
                "isAdoptedChild": "false"
            }
            while True:
                try:
                    response = requests.get(JsonGetChildrenConcepts_url, headers=self.headers, cookies=self.cookies,
                                            params=params, timeout=10).json()
                    break
                except Exception as e:
                    self.headers['user-agent'] = self.ua_init()
                    print(e)

            print('共有二级标签', len(response), '个')

            for respon in response:
                print('-------正在解析第', response.index(respon) + 1, '个-------')
                ID = respon.get('ID')
                isLeaf = respon.get('isLeaf')
                print('-------', ID, isLeaf)
                self.get_Children(ID, isLeaf)

    # 采集详情
    def get_info(self, ID):

        data = {}
        url = "https://icd.who.int/dev11/l-m/en/GetConcept"
        params = {
            "ConceptId": ID
        }
        while True:
            try:
                response = requests.get(url, headers=self.headers, cookies=self.cookies, params=params, timeout=8)
                break
            except Exception as e:
                self.headers['user-agent'] = self.ua_init()
                print(e)
        sel = etree.HTML(response.text)

        # id
        data['_id'] = ID

        # 采集日期
        grab_date = time.strftime("%Y-%m-%d", time.gmtime())
        data['grab_date'] = grab_date

        # 访问链接
        data['url'] = 'https://icd.who.int/dev11/l-m/en#/' + ID

        # all_name
        all_name = ''.join(sel.xpath("string(//div[@class='detailsTitle'])"))
        data['all_name'] = self.deal_str(all_name) if all_name else ''

        # 防止有请求状态码是200，但数据请求失败的情况
        # 如果从HTML中没有获取到 all_name，函数会跳过当前的处理，这是为了确保数据的完整性
        if not data['all_name']:
            pass
        else:
            # first_name
            first_name = sel.xpath("//div[@class='detailsTitle']/span/text()")
            data['first_name'] = first_name[0] if first_name else ''

            # last_name
            last_name = ''.join(sel.xpath("//div[@class='detailsTitle']/text()")).strip()
            data['last_name'] = last_name if last_name else ''

            # 描述信息
            description = ''.join(
                sel.xpath("string(//div[contains(text(),'Description')]/following-sibling::div[1])")).strip()
            data['description'] = description if description else ''

            print(data)

            # 实例化CSVHandler类将数据保存到CSV文件

            handler.save_single_data(data)

            # self.save_data(data)

            # 存入数据后将该id从redis中移除
            self.redis_client.srem(self.redis_name, ID)


if __name__ == '__main__':

    # 实例化CSVHandler对象
    headers = ['_id', 'grab_date', 'url', 'all_name', 'first_name', 'last_name', 'description']
    handler = CSVHandler('icd11_data.csv', headers)

    # 实例化SpiderHandler对象
    spiders = SpiderHandler()

    # 执行获取列表函数
    spiders.get_Root()

    # 检查Redis数据库中的spiders.redis_name集合的大小，并将其赋值给flag
    flag = spiders.redis_client.scard(spiders.redis_name)

    while flag:

        # 从redis中读取id并解码存入list
        ids = []
        for id in spiders.redis_client.smembers(spiders.redis_name):
            ids.append(id.decode())
        print(len(ids))

        # 多线程执行详情解析,定义线程数，没有代理ip的情况下，线程数应小于等于5
        executor = ThreadPoolExecutor(5)
        for data in executor.map(spiders.get_info, ids):
            pass

        flag = spiders.redis_client.scard(spiders.redis_name)

    handler.save_to_specific_location('E:\\Tool\\Pycharm\\PycharmProject\\code\\ICD11\\icd11_data.csv')