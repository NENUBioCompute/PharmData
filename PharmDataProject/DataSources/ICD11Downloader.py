"""
  -*- encoding: utf-8 -*-
  @Author: Deepwind
  @Time  : 3/31/2024 11:00 AM
  @Email: deepwind32@163.com
"""
import requests
import random
import re
import time
import redis

from lxml import etree
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

from PharmDataProject.Utilities.FileDealers.CSVDealer import CSVWriter


class SpiderHandler:
    def ua_init(self):
        base_user_agent = 'Mozilla/5.0 (Windows NT {0}; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{1}.0.{2}.{3} Safari/537.36'
        user_agent = base_user_agent.format(random.choice(['6.1', '6.2'] + ['10.0' for i in range(7)]),
                                            random.choice([i for i in range(70, 99)]),
                                            random.choice([i + 1 for i in range(2200)]),
                                            random.choice([i + 1 for i in range(99)]),
                                            )
        return user_agent
    def __init__(self, config):
        self.config = config
        self.headers = {
            "authority": "icd.who.int",
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9",
            "referer": config.get("icd11", "referer"),
            "sec-ch-ua": '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": self.ua_init(),
            "x-requested-with": "XMLHttpRequest"
        }
        self.cookies = {
            ".AspNetCore.Antiforgery.RtGCWVXC8-4": "CfDJ8Jq_h8XBjjNAtvvkag-LchhXr6BaeCxMfvI5beO_kxHscvDypFCrdCnHVx6oYi5WrIuAoPQw7UOQ52MU5LHq4mWHJRsepH0UTMnyWtrjRc7TOmLxalslFD0nNWGx6V5nK56lzDfKKPNz7tEkfG2YMXk",
            "ai_user": "qqDllWTs2S1EW2T5BgYf3A|2023-10-30T02:32:10.846Z"
        }
        self.redis_client = redis.Redis()
        self.redis_name = 'ICD:deep_url'
    def get_config_value(self, key):
        return self.config.get("icd11", key)

    def get_Root(self):
        """
        get root list
        """
        JsonGetRootConcepts_url = self.get_config_value("json_get_root_concepts_url")
        JsonGetRootConcepts_params = {"useHtml": "true"}
        while True:
            try:
                response = requests.get(JsonGetRootConcepts_url, headers=self.headers, cookies=self.cookies,
                                        params=JsonGetRootConcepts_params, timeout=5).json()
                break
            except Exception:
                self.headers['user-agent'] = self.ua_init()
        for item in tqdm(response):
            ID = item.get('ID')
            isLeaf = item.get('isLeaf')
            self.get_Children(ID, isLeaf)

    def get_Children(self, ID, isLeaf):
        """
        Get children list.
        """
        if isLeaf:
            self.redis_client.sadd(self.redis_name, ID)
        else:
            JsonGetChildrenConcepts_url = self.get_config_value("json_get_children_concepts_url")
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
                except Exception:
                    self.headers['user-agent'] = self.ua_init()

            for item in response:
                ID = item.get('ID')
                isLeaf = item.get('isLeaf')
                self.get_Children(ID, isLeaf)

    # 采集详情
    def get_info(self, ID):
        def deal_str(string):
            """对字符串进行清洗和格式化"""
            new_str = re.sub('\\s{2,}', ' ', re.sub('(\t|\r|\n|\xa0)+', ' ', string).replace(',', '，').strip()).replace(
                '\u200b', '').replace("\u3000", '')
            return new_str

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

        data['_id'] = ID
        data['grab_date'] = time.strftime("%Y-%m-%d", time.gmtime())
        data['url'] = 'https://icd.who.int/dev11/l-m/en#/' + ID

        # all_name
        all_name = ''.join(sel.xpath("string(//div[@class='detailsTitle'])"))
        data['all_name'] = deal_str(all_name) if all_name else ''

        # 防止有请求状态码是200，但数据请求失败的情况
        # 如果从HTML中没有获取到 all_name，函数会跳过当前的处理，这是为了确保数据的完整性
        if not data['all_name']:
            pass
        else:
            first_name = sel.xpath("//div[@class='detailsTitle']/span/text()")
            last_name = ''.join(sel.xpath("//div[@class='detailsTitle']/text()")).strip()
            description = ''.join(
                sel.xpath("string(//div[contains(text(),'Description')]/following-sibling::div[1])")).strip()
            data['first_name'] = first_name[0] if first_name else ''
            data['last_name'] = last_name if last_name else ''
            data['description'] = description if description else ''
            print(data)

            # 实例化CSVHandler类将数据保存到CSV文件

            handler.save_single_data(data)

            # self.save_data(data)

            # 存入数据后将该id从redis中移除
            self.redis_client.srem(self.redis_name, ID)
    def start(self):
        self.

    def __parse(self):
        handler = CSVWriter(self.get_config_value("csv_file_location"),
                            ['_id', 'grab_date', 'url', 'all_name', 'first_name', 'last_name', 'description'])
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

            executor = ThreadPoolExecutor(5)
            for data in executor.map(spiders.get_info, ids):
                # print(data)
                pass

            flag = spiders.redis_client.scard(spiders.redis_name)

        handler.save_to_specific_location('./icd11_data.csv')

if __name__ == '__main__':
    icd11_parser = ICD