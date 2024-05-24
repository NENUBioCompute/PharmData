import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

import time

# client = MongoClient("localhost", 27017, username="readwrite", password="readwrite")
client = MongoClient("59.73.198.168", 27017, username="readwrite", password="readwrite")
db = client["PharmRG"]

collection = db["source_drugs"]
url_list = []


# 发送GET请求获取网页内容
class DrugLinkCrawler:
    def get_url(self, i, j):
        print(i, j)  # 可以选择保留或移除这行，根据你的需求来监控当前处理的字符
        visited_links = set()  # 存储已访问过的链接
        url = f'https://www.drugs.com/alpha/{chr(i)}{chr(j)}.html'
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
        }
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
                            url_list.append(full_link)
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
        soup = BeautifulSoup(html, 'html.parser')

        # 获取药物名称
        drug_name = soup.find('h1').text if soup.find('h1') else None

        drug_info = {
            'drug_name': drug_name,
            'fields': {}
        }

        # 获取副标题信息
        drug_subtitle = soup.find('p', class_='drug-subtitle')
        if drug_subtitle:
            for element in drug_subtitle.find_all('b'):
                field_name = element.text.strip()[:-1]
                field_value = ''.join(
                    [sib.text.strip() for sib in element.next_siblings if getattr(sib, 'name', None) != 'br'])
                drug_info['fields'][field_name] = field_value

        # 从h2标签中获取更多信息
        def get_section_content(h2_id):
            content = ''
            section = soup.find(id=h2_id)
            while section:
                section = section.find_next_sibling()
                if section and section.name == 'p':
                    content += section.text.strip() + '\n'
                else:
                    break
            return content.strip()

        for section_id in ['uses', 'warnings', 'interactions', 'side-effects', 'dosage']:
            content = get_section_content(section_id)
            if content:
                drug_info[section_id] = content

        return drug_info

    def save_to_mongo(self, data, collection):
        if data:
            collection.insert_one(data)
            print('Data saved to MongoDB:', data)

    def muilt_url_2(self, urls, collection):
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_url = {executor.submit(self.GetData, url): url for url in urls}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                    self.save_to_mongo(data, collection)
                except Exception as exc:
                    print(f'{url} generated an exception: {exc}')


if __name__ == '__main__':
    start_time = time.time()
    crawler = DrugLinkCrawler()
    urls = crawler.muilt_url()  # 假设这个方法返回所有有效的URL列表

    # 现在我们只关心第一个URL，因此仅获取并处理第一个URL
    if urls:
        parser = Parsedata()
        first_drug_data = parser.GetData(urls[0])
        print(first_drug_data)  # 打印出第一个字典
        # 如果需要停止后续操作，直接使用break或不继续其它操作

    end_time = time.time()
    print(f"Total time taken: {end_time - start_time} seconds")
