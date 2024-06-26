# @Author：huqianxiang
# @Time：2023/10/9
# @FileName：Transporter.py
# @Software:PyCharm

import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import json
from pymongo import MongoClient


class TransporterParser:
    def __init__(self, url):
        self.url = url

    def get_transporter_links(self):
        response = requests.get(self.url)
        content = response.content
        soup = BeautifulSoup(content, 'html.parser')
        links = set()
        for link in soup.find_all('a'):
            href = link.get('href')
            parsed_href = urlparse(href)
            if parsed_href.scheme == '' and parsed_href.netloc == '':
                href = urljoin(self.url, href)
            if href and "transporters" in href:
                links.add(href)
        sorted_links = sorted(links)
        return sorted_links

    def scrape_and_save_data(self, name):
        response = requests.get(self.url)
        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table")
        data = []

        for table in tables:
            table_data = []
            headers = table.find_all("th")
            header_row = [header.text.strip() for header in headers]

            title = table.find_previous_sibling("h2")
            if title:
                title = title.text.strip()

            rows = table.find_all("tr")

            for row in rows:
                row_data = {}
                cells = row.find_all("td")
                if len(cells) == len(header_row):
                    for i in range(len(cells)):
                        row_data[header_row[i]] = cells[i].text.strip()
                    table_data.append(row_data)

            if len(table_data) > 0:
                data.append({'title': title, 'table_data': table_data})

        if len(data) > 0:
            filename = self.url.split("/")[-2] + ".json"
            with open(filename, "w") as json_file:
                json.dump(data, json_file, indent=4)

            client = MongoClient('mongodb://readwrite:readwrite@59.73.198.168/?authMechanism=DEFAULT')
            db = client['PharmRG']
            collection = db['source_transporter']
            with open(filename, "r") as json_file:
                json_data = json.load(json_file)
            collection.insert_one({'url': self.url, 'name': name, 'data': json_data})
            client.close()

            print(f"Data scraped and saved for {self.url}")
        else:
            print(f"No data found for {self.url}")


if __name__ == '__main__':
    url = "https://transportal.compbio.ucsf.edu/index/"
    # 实例化 TransporterParser 类，传入 URL
    transporter_parser = TransporterParser(url)  # 使用实例化的对象
    transporter_links = transporter_parser.get_transporter_links()

    for link in transporter_links:
        print(link)
        try:
            name = link.split("/")[-2]
            # 对每个链接创建新的实例，因为 self.url 需要更新
            transporter_instance = TransporterParser(link)
            transporter_instance.scrape_and_save_data(name)
        except Exception as e:
            print(f"Error processing {link}: {str(e)}")