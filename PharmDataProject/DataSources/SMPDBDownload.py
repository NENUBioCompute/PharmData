import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin  # 用于处理相对链接

# 目标网站的URL
url = "https://www.smpdb.ca/downloads"

# 发送HTTP请求获取页面内容
response = requests.get(url)

# 检查请求是否成功
if response.status_code == 200:
    # 使用BeautifulSoup解析页面内容
    soup = BeautifulSoup(response.text, 'html.parser')

    # 找到所有的下载链接
    download_links = soup.find_all('a', href=True)

    # 创建一个文件夹来保存下载的文件
    download_folder = 'SMPDBdownload'
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    # 遍历下载链接并下载压缩包
    for link in download_links:
        file_url = link['href']

        # 处理相对链接
        absolute_url = urljoin(url, file_url)

        # 只下载以.zip结尾的链接，你可以根据实际需要修改条件
        if absolute_url.endswith('.zip'):
            file_name = os.path.join(download_folder, os.path.basename(absolute_url))

            # 下载压缩包
            with requests.get(absolute_url, stream=True) as download_response:
                with open(file_name, 'wb') as file:
                    for chunk in download_response.iter_content(chunk_size=128):
                        file.write(chunk)

            print(f"文件 '{file_name}' 下载完成。")

else:
    print(f"无法访问网站。状态码：{response.status_code}")
