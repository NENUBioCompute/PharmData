# @Author：huqianxiang
# @Time：2023/10/9
# @FileName：Transporter.py
# @Software:PyCharm


import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
#import json
#from pymongo import MongoClient


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
