import requests
from bs4 import BeautifulSoup
import os

url = "https://www.smpdb.ca/downloads"
response = requests.get(url)

# 检查是否成功连接
if response.status_code == 200:
    # 使用BeautifulSoup解析HTML
    soup = BeautifulSoup(response.text, 'html.parser')

    # 查找包含下载链接的元素
    download_links = soup.find_all('a', href=True)

    # 指定下载目录
    download_directory = "your_download_directory"
    os.makedirs(download_directory, exist_ok=True)

    # 遍历下载链接并下载文件
    for link in download_links:
        file_url = link['href']
        file_name = os.path.join(download_directory, os.path.basename(file_url))

        # 下载文件
        with requests.get(file_url, stream=True) as r:
            with open(file_name, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
else:
    print(f"Failed to connect to {url}. Status code: {response.status_code}")
