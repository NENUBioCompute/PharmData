# @Author：huqianxiang
# @Time：2023/10/9
# @FileName：Transporter.py
# @Software:PyCharm

import requests
from bs4 import BeautifulSoup
#from urllib.parse import urlparse, urljoin
#import json
#from pymongo import MongoClient

def scrape_data(url):
    response = requests.get(url)
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

    return data