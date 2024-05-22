import requests
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

class Downloader:
    def __init__(self):
        self.base_url = "https://www.drugs.com"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
        }

    def get_url(self, i, j):
        url = f'https://www.drugs.com/alpha/{chr(i)}{chr(j)}.html'
        response = requests.get(url=url, headers=self.headers)
        links = []
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            box_element = soup.find('ul', class_='ddc-list-column-2')
            if box_element:
                links = ["https://www.drugs.com" + li.find('a')['href'] for li in box_element.find_all('li') if li.find('a')]
        return links

    def fetch_urls(self):
        urls = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self.get_url, i, j) for i in range(97, 123) for j in range(97, 123)]
            for future in as_completed(futures):
                urls.extend(future.result())
        return urls

if __name__ == '__main__':
    downloader = Downloader()
    url_list = downloader.fetch_urls()
    print("Fetched URLs:", url_list)
