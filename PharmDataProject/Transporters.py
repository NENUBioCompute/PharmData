# @Author：huqianxiang
# @Time：2023/10/9
# @FileName：Transporter.py
# @Software:PyCharm


import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import json
from pymongo import MongoClient

def get_transporter_links(url):
    response = requests.get(url)
    content = response.content
    soup = BeautifulSoup(content, 'html.parser')
    links = set()
    for link in soup.find_all('a'):
        href = link.get('href')
        parsed_href = urlparse(href)
        if parsed_href.scheme == '' and parsed_href.netloc == '':
            href = urljoin(url, href)
        if href and "transporters" in href:
            links.add(href)
    sorted_links = sorted(links)
    return sorted_links

def scrape_and_save_data(url, name):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    tables = soup.find_all("table")
    data = []

    for table in tables:
        table_data = []
        headers = table.find_all("th")
        header_row = [header.text.strip() for header in headers]

        # 获取表格标题
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
        filename = url.split("/")[-2] + ".json"
        with open(filename, "w") as json_file:
            json.dump(data, json_file, indent=4)

        client = MongoClient('mongodb://readwrite:readwrite@59.73.198.168/?authMechanism=DEFAULT')
        db = client['PharmRG']
        collection = db['transporter']
        with open(filename, "r") as json_file:
            json_data = json.load(json_file)
        collection.insert_one({'url': url, 'name': name, 'data': json_data})
        client.close()

        print(f"Data scraped and saved for {url}")
    else:
        print(f"No data found for {url}")

url = "https://transportal.compbio.ucsf.edu/index/"
transporter_links = get_transporter_links(url)

for link in transporter_links:
    try:
        name = link.split("/")[-2]
        scrape_and_save_data(link, name)
    except:
        print(f"No data found for {link}")
