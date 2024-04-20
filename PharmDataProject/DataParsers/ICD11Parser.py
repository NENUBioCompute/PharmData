import logging
import queue
import random
import re
import threading
import time
from datetime import datetime, timedelta

import requests
from lxml import etree

from PharmDataProject.Utilities.Database.dbutils_v2 import DBConnection
from PharmDataProject.Utilities.FileDealers.ConfigParser import ConfigParser


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
        self.max_retry = 20
        self.ids = set()
        self.buffer_data = []
        self.buffer_data_size = None
        self.db = None
        self.counter = 0
        self.saved_data_counter = 0
        self.start_time = datetime.now()
        self.retry_queue = queue.Queue()
        self.id_queue = queue.Queue()

    def __get_config_value(self, key):
        return self.config.get("icd11", key)

    def __get_resp(self, url, params):
        retry_count = 0
        response = None
        while retry_count < self.max_retry:
            try:
                response = requests.get(url, headers=self.headers, cookies=self.cookies, params=params, timeout=5)
                if response.status_code == 401:
                    self.headers['user-agent'] = ua_init()
                    response = requests.get(url, headers=self.headers, cookies=self.cookies, params=params, timeout=5)
                    if response.status_code == 401:
                        raise Exception(401)
                    time.sleep(10)
                break
            except Exception as e:
                self.headers['user-agent'] = ua_init()
                logging.error(e)
                time.sleep(20)
            retry_count += 1
        if response is None:
            logging.error(f"Network error, Maximum number of attempts exceeded. \n params: {params}")
        return response

    def __get_root_ids(self):
        """
        get root id list
        """
        root_concepts_url = config.get("json_get_root_concepts_url")
        params = {"useHtml": "true"}
        resp = self.__get_resp(root_concepts_url, params)
        if resp is None:
            raise requests.RequestException("Root concept id acquiring failed. Network issue, out of max try.")
        root_data = [(item.get('ID'), item.get('isLeaf')) for item in resp.json()]

        threads = [threading.Thread(target=self.__get_children_id, args=(arg[0], arg[1])) for arg in root_data]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

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
            self.id_queue.put(id)
        else:
            children_concepts_url = config.get("json_get_children_concepts_url")
            params = {
                "ConceptId": id,
                "useHtml": "true",
                "showAdoptedChildren": "true",
                "isAdoptedChild": "false"
            }
            resp = self.__get_resp(children_concepts_url, params)
            if resp is None:
                self.retry_queue.put(("children_id", id, is_leaf))
                logging.error("children_id: {id} is added into retry queue.")
                return
            for item in resp.json():
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

        url = config.get("get_content_url")
        params = {"ConceptId": id}
        data = {'url': config.get("data_url_prefix") + id}

        if self.db.search_record(data) is not None:
            logging.info(f"A saved data record found with ID: {id}")
            self.counter += 1
            return

        sel = None
        sel_counter = self.max_retry
        while sel is None:
            resp = self.__get_resp(url, params)
            if resp is None:
                self.retry_queue.put(("concept", id))
                return

            sel = etree.HTML(resp.text)
            sel_counter -= 1
            if sel_counter == 0:
                logging.error("concept_id: {id} is added into retry queue.")
                self.retry_queue.put(("concept", id))
                return

        all_name = ''.join(sel.xpath("string(//div[@class='detailsTitle'])"))
        all_name = deal_str(all_name) if all_name else ''

        # To prevent cases where the request status code is 200 but the data retrieval fails
        if all_name:
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

    def __retry(self):
        """
        Retry failed tasks.
        """
        logging.info("Retrying starts.")
        while not self.retry_queue.empty():
            task_info = self.retry_queue.get()
            if task_info[0] == "children_id":
                self.__get_children_id(task_info[1], task_info[2])
            elif task_info[0] == "concept":
                self.__save_info(task_info[1])
            else:
                raise ValueError("Unknown task type")

    def __parse(self):
        self.__get_root_ids()
        self.__retry()

        def get_data():
            try:
                while True:
                    self.__save_info(self.id_queue.get(block=False))
            except queue.Empty:
                return

        # save_info
        threads = [threading.Thread(target=get_data) for _ in range(int(config.get("thread_num")))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        # insert the left data
        if len(self.buffer_data) > 0:
            self.saved_data_counter += len(self.buffer_data)
            logging.info(f"Saved Record: {self.saved_data_counter}")
            self.db.insert(self.buffer_data)
        logging.info("Work finished.")

    def start(self, config):
        self.config = config
        self.buffer_data_size = int(config.get("data_buffer_size"))
        self.db = DBConnection(config.get("db_name"), config.get("collection_name"),
                               config=config)
        self.__parse()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%m/%d %I:%M:%S')
    icd11_parser = ICD11Parser()
    cfg = "/home/zhaojingtong/tmpcode/PharmData/PharmDataProject/conf/drugkb.config"
    config = ConfigParser(cfg)
    config.set_section("icd11")
    icd11_parser.start(config)
