import requests
from bs4 import BeautifulSoup

url = "https://www.smpdb.ca/downloads"

# 发送请求获取页面内容
response = requests.get(url)

# 使用BeautifulSoup解析HTML
soup = BeautifulSoup(response.text, 'html.parser')

# 找到所有的链接
links = soup.find_all('a', href=True)

# 遍历链接并下载数据
for link in links:
    file_url = link['href']
    file_name = link.get_text()  # 可以根据实际情况选择使用链接文本作为文件名

    # 发送请求下载文件
    file_response = requests.get(file_url)

    # 保存文件到本地
    with open(file_name, 'wb') as file:
        file.write(file_response.content)

print("Download complete.")
