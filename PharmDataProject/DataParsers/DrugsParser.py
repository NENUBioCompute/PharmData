import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor

import time

# client = MongoClient("localhost", 27017, username="readwrite", password="readwrite")
client = MongoClient("59.73.198.168", 27017, username="readwrite", password="readwrite")
db = client["PharmRG"]

collection = db["source_drugs"]
url_list = []


# 发送GET请求获取网页内容
class DrugLinkCrawler:

    def get_url(self, i, j):
        print(i, j)
        visited_links = set()  # 存储已访问过的链接
        url = f'https://www.drugs.com/alpha/{chr(i)}{chr(j)}.html'
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
        }
        # headers = copy_headers_dict(headers)
        response = requests.get(url=url, headers=headers)

        # 检查响应状态码
        if response.status_code == 200:
            # 创建BeautifulSoup对象
            soup = BeautifulSoup(response.content, 'html.parser')
            box_element = soup.find('ul', class_='ddc-list-column-2')
            if box_element is not None:
                li_elements = box_element.find_all('li')
                if li_elements:
                    for li in li_elements:
                        link = li.find('a')['href']
                        if link not in visited_links:  # 检查是否已经访问过
                            visited_links.add(link)  # 添加到已访问集合中
                            full_link = "https://www.drugs.com" + link

                            # 发送HTTP请求获取网页内容
                            url_list.append(full_link)
                            # print(full_link)
                            print(len(url_list))
        else:
            print(response)

    def muilt_url(self):
        with ThreadPoolExecutor(max_workers=5) as executor:
            for i in range(97, 123):
                for j in range(97, 123):
                    executor.submit(self.get_url, i, j)
            executor.shutdown(wait=True)


class Parsedata:

    def GetData(self, url):

        response = requests.get(url)
        html = response.text

        # 创建Beautiful Soup对象
        soup = BeautifulSoup(html, 'html.parser')

        # 查找class为contentBox的盒子
        content_box = soup.find('div', class_="contentBox")
        # 获取h1标签内容
        drug_name = content_box.find('h1').text

        # 获取class为drug-subtitle的段落内容
        # 找到 drug_subtitle 元素
        drug_subtitle = soup.find('p', class_='drug-subtitle')

        # 创建一个空字典来存储字段名和字段值
        drug_info = {}

        # 检查是否找到 drug_subtitle 元素
        if drug_subtitle is not None:
            # 遍历所有的<b>标签
            for element in drug_subtitle.find_all('b'):
                field_name = element.string.strip()[:-1]

                # 获取当前标签之后的所有兄弟元素
                siblings = element.next_siblings

                field_value = ''
                # 遍历所有兄弟元素
                for sibling in siblings:
                    # 如果当前兄弟元素是字符串，则将其文本内容添加到字段值（跳过空白字符）
                    if isinstance(sibling, str):
                        sibling_value = sibling.strip()
                        if sibling_value:
                            field_value += sibling_value
                    # 如果当前兄弟元素是<br>标签，结束提取内容
                    elif sibling.name == 'br':
                        break
                    # 如果当前兄弟元素是<a>标签，则将其文本内容添加到字段值
                    elif sibling.name == 'a' or 'p':
                        field_value += sibling.text.strip()
                    elif sibling.name == 'i':
                        field_value += sibling.string.strip()
                drug_info[field_name] = field_value
        else:
            generic_name = brand_name = dosage_form = drug_class = None

        # 输出指定的四个字段的值，如果为空则输出 None
        generic_name = drug_info.get("Generic name", None)
        brand_name = drug_info.get("Brand name", None)
        dosage_form = drug_info.get("Dosage form", None)
        drug_class = drug_info.get("Drug class", None)

        # 获取h2标签及其后续标签的内容
        def get_section_content(h2_id):
            section_tag = soup.find(id=h2_id)
            if section_tag:
                section_content = section_tag.text
                next_element = section_tag.next_sibling
                while next_element and next_element.name != "h2":
                    if next_element.name == "p":
                        section_content += "\n" + next_element.text
                    next_element = next_element.next_sibling
                return section_content
            else:
                return None

        uses = get_section_content("uses")
        warnings = get_section_content("warnings")
        interactions = get_section_content("interactions")
        side_effects = get_section_content("side-effects")
        dosage = get_section_content("dosage")

        # 返回结果
        result = {
            "drug_name": drug_name,
            "generic name:": generic_name,
            "brand name:": brand_name,
            "dosage form:": dosage_form,
            "drug class:": drug_class,
            "uses": uses,
            "warnings": warnings,
            "interactions": interactions,
            "side_effects": side_effects,
            "dosage": dosage
        }
        print('baocun')
        collection.insert_one(result)
        print(result)

    def muilt_url_2(self, useless):
        with ThreadPoolExecutor(max_workers=5) as executor:
            for url in useless:
                executor.submit(self.GetData, url)
            executor.shutdown(wait=True)


if __name__ == '__main__':
    start_time = time.time()
    DrugLinkCrawler = DrugLinkCrawler()
    DrugLinkCrawler.muilt_url()

    end_time = time.time()
    print(f"shijianwei{end_time - start_time}")
    sets = set(url_list)
    useless = list(sets)
    print(len(useless))
    start_time2 = time.time()
    Parsedata = Parsedata()
    Parsedata.muilt_url_2(useless)
    end_time2 = time.time()
    print(f"shijianwei22222{end_time2 - start_time2}")
