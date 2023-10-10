import requests
from bs4 import BeautifulSoup
import urllib.parse
import os

def download_files_from_url():
    # 目标网页的URL
    url = "https://wikipathways-data.wmcloud.org/current/gpml/"

    # 发送HTTP请求获取网页内容
    response = requests.get(url)

    if response.status_code == 200:
        # 解析HTML内容
        soup = BeautifulSoup(response.text, 'html.parser')

        # 提取资源链接
        resource_links = []

        for link in soup.find_all('a'):
            href = link.get('href')
            if href and not href.endswith('/'):  # 确保链接不是目录链接
                resource_links.append(href)

        # 创建一个目录来存储下载的资源
        download_directory = os.path.join(os.getcwd(), "downloaded_resources")
        os.makedirs(download_directory, exist_ok=True)

        # 下载资源
        for resource_link in resource_links:
            full_resource_url = urllib.parse.urljoin(url, resource_link)
            file_name = os.path.join(download_directory, resource_link.split("/")[-1])
            urllib.request.urlretrieve(full_resource_url, file_name)
            print(f"Downloaded: {file_name}")
    else:
        print("Failed to retrieve the webpage.")
