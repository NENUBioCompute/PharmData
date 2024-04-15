import urllib
import random
import requests
from Bio.KEGG import REST

class KEGGDownloader:
    def __init__(self):
        pass

    def get_id(self):
        drug_list = REST.kegg_list("drug").read()  # Assuming you want to fetch drug list
        items = []
        for line in drug_list.strip().split("\n"):
            ids, description = line.split("\t")
            items.append(ids)
        return items

    def download(self, drug):
        items = self.get_id()
        idx = items.index(drug)
        print(f"Starting download from index {idx} of {len(items)} items.")
        for item in items[idx:]:
            print(f"Downloading: {item}")
            ua_list = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36 Edg/103.0.1264.62',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0',
                'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.81 Safari/537.36 SE 2.X MetaSr 1.0'
            ]
            headers = {'User-Agent': random.choice(ua_list)}
            url = f'https://rest.kegg.jp/get/{item}'
            response = requests.get(url, headers=headers)
            data = response.text
            yield data

if __name__ == "__main__":
    downloader = KEGGDownloader()
    drug_to_start = "D00001"  # Example drug ID to start from
    for data in downloader.download(drug_to_start):
        print(data)  # Output or process each downloaded data
