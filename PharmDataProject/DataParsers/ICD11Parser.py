import configparser
from datetime import datetime, timedelta
import logging
import requests
import random
import re
import time
from lxml import etree
from PharmDataProject.Utilities.Database.dbutils_v2 import DBConnection


def ua_init():
    base_user_agent = 'Mozilla/5.0 (Windows NT {0}; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{1}.0.{2}.{3} Safari/537.36'
    user_agent = base_user_agent.format(random.choice(['6.1', '6.2'] + ['10.0' for i in range(7)]),
                                        random.choice([i for i in range(70, 99)]),
                                        random.choice([i + 1 for i in range(2200)]),
                                        random.choice([i + 1 for i in range(99)]),
                                        )
    return user_agent


class ICD11Parser:
    def __init__(self):
        self.config = None
        self.headers = {
            "authority": "icd.who.int",
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9",
            "referer": "https://icd.who.int/dev11/l-m/en",
            "sec-ch-ua": '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": ua_init(),
            "x-requested-with": "XMLHttpRequest"
        }
        self.cookies = {
            ".AspNetCore.Antiforgery.RtGCWVXC8-4": "CfDJ8Jq_h8XBjjNAtvvkag-LchhXr6BaeCxMfvI5beO_kxHscvDypFCrdCnHVx6oYi5WrIuAoPQw7UOQ52MU5LHq4mWHJRsepH0UTMnyWtrjRc7TOmLxalslFD0nNWGx6V5nK56lzDfKKPNz7tEkfG2YMXk",
            "ai_user": "qqDllWTs2S1EW2T5BgYf3A|2023-10-30T02:32:10.846Z"
        }
        self.max_retry = 10
        self.ids = set()
        self.buffer_data = []
        self.buffer_data_size = None
        self.db = None
        self.counter = 0
        self.saved_data_counter = 0
        self.start_time = datetime.now()

    def __get_config_value(self, key):
        return self.config.get("icd11", key)

    def __get_resp(self, url, params):
        retry_count = 0
        response = None
        while retry_count < self.max_retry:
            try:
                response = requests.get(url, headers=self.headers, cookies=self.cookies, params=params, timeout=10)
                break
            except Exception as e:
                self.headers['user-agent'] = ua_init()
                logging.error(e)
                time.sleep(20)
            retry_count += 1
        if response is None:
            raise requests.RequestException("Network error, Maximum number of attempts exceeded")
        return response

    def __get_root_ids(self):
        """
        get root id list
        """
        root_concepts_url = self.__get_config_value("json_get_root_concepts_url")
        params = {"useHtml": "true"}
        for item in self.__get_resp(root_concepts_url, params).json():
            self.__get_children_id(item.get('ID'), item.get('isLeaf'))

    def __get_children_id(self, id, is_leaf):
        """
        Get children list.
        """
        if is_leaf:
            # avoid duplicate id
            old_len = len(self.ids)
            self.ids.add(id)
            if old_len == len(self.ids):
                return
            self.__save_info(id)
        else:
            children_concepts_url = self.__get_config_value("json_get_children_concepts_url")
            params = {
                "ConceptId": id,
                "useHtml": "true",
                "showAdoptedChildren": "true",
                "isAdoptedChild": "false"
            }
            for item in self.__get_resp(children_concepts_url, params).json():
                self.__get_children_id(item.get('ID'), item.get('isLeaf'))

    def __save_info(self, id):
        """
        Store corresponding page data according to id
        """

        def deal_str(string):
            """对字符串进行清洗和格式化"""
            new_str = re.sub('\\s{2,}', ' ', re.sub('(\t|\r|\n|\xa0)+', ' ', string).replace(',', '，').strip()).replace(
                '\u200b', '').replace("\u3000", '')
            return new_str

        url = self.__get_config_value("get_content_url")
        params = {"ConceptId": id}
        sel = etree.HTML(self.__get_resp(url, params).text)
        data = {'url': self.__get_config_value("data_url_prefix") + id}
        all_name = ''.join(sel.xpath("string(//div[@class='detailsTitle'])"))
        all_name = deal_str(all_name) if all_name else ''

        # To prevent cases where the request status code is 200 but the data retrieval fails
        if not all_name:
            pass
        else:
            first_name = sel.xpath("//div[@class='detailsTitle']/span/text()")
            last_name = ''.join(sel.xpath("//div[@class='detailsTitle']/text()")).strip()
            description = ''.join(
                sel.xpath("string(//div[contains(text(),'Description')]/following-sibling::div[1])")).strip()
            data['disease_id'] = first_name[0] if first_name else ''
            data['disease_name'] = last_name if last_name else ''
            data['description'] = description if description else ''
            if len(self.buffer_data) >= self.buffer_data_size:
                self.saved_data_counter += len(self.buffer_data)
                logging.info(f"Saved Record: {self.saved_data_counter}")
                self.db.insert(self.buffer_data)
                self.buffer_data = []
            self.counter += 1
            elapsed_time = datetime.now() - self.start_time
            # for debug
            print(
                f"\rCount: {self.counter}, Elapse time: {elapsed_time}, average_elapsed_time: {timedelta(seconds=elapsed_time.seconds) / self.counter}",
                end='')
            self.buffer_data.append(data)

    def __parse(self):
        self.__get_root_ids()
        # insert the left data
        if len(self.buffer_data) > 0:
            self.saved_data_counter += len(self.buffer_data)
            logging.info(f"Saved Record: {self.saved_data_counter}")
            self.db.insert(self.buffer_data)

    def start(self, config):
        self.config = config
        self.buffer_data_size = int(self.__get_config_value("data_buffer_size"))
        self.db = DBConnection(self.__get_config_value("db_name"), self.__get_config_value("collection_name"),
                               cfg_file=self.__get_config_value("cfg_file_location"))
        self.__parse()
        self.db.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%m/%d %I:%M:%S')
    icd11_parser = ICD11Parser()
    cfg = "../conf/drugkb.config"
    config = configparser.ConfigParser()
    config.read(cfg)
    icd11_parser.start(config)
