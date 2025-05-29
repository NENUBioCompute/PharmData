import random
import requests
from Bio.KEGG import REST
from tqdm import tqdm
import os
import configparser


class KEGGDownloader:
    def __init__(self, config_path='../conf/drugkb_test.config'):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

    def get_id(self, source):
        if source == 'ddi':
            return self.get_ddi_id()
        else:
            item_list = REST.kegg_list(source).read()  # Fetch the list based on the source
            items = []
            for line in item_list.strip().split("\n"):
                ids, description = line.split("\t")
                items.append(ids)
            return items

    def get_ddi_id(self):
        drug_list = REST.kegg_list("drug").read()  # Assuming you want to fetch drug list for ddi
        items = []
        for line in drug_list.strip().split("\n"):
            ids, description = line.split("\t")
            items.append(ids)
        return items

    def download(self, start_id, source, save_path, test_mode=False):
        items = self.get_id(source)
        if start_id in items:
            idx = items.index(start_id)
        else:
            idx = 0
        print(f"Starting download from index {idx} of {len(items)} items.")

        # Ensure the save directory exists
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        for item in tqdm(items[idx:], desc="Downloading KEGG data", unit="file"):
            print(f"\nDownloading: {item}")
            ua_list = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36 Edg/103.0.1264.62',
                'Mozilla/5.0 (Windows NT 10.0; rv:98.0) Gecko/20100101 Firefox/98.0',
                'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.81 Safari/537.36 SE 2.X MetaSr 1.0'
            ]
            headers = {'User-Agent': random.choice(ua_list)}

            if source == 'ddi':
                # Fetch interactions for the given drug
                url = f'https://rest.kegg.jp/link/ddi/{item}'
                response = requests.get(url, headers=headers)
                data = response.text

                if response.status_code != 200 or not data.strip():
                    print(f"Failed to fetch DDI links for {item}, status code: {response.status_code}")
                    continue

                # Extract DDI IDs from the response
                ddi_ids = [line.split("\t")[1] for line in data.strip().split("\n") if line]
                for ddi_id in ddi_ids:
                    ddi_url = f'https://rest.kegg.jp/get/{ddi_id}'
                    ddi_response = requests.get(ddi_url, headers=headers)
                    ddi_data = ddi_response.text

                    if ddi_response.status_code != 200 or not ddi_data.strip():
                        print(f"Failed to fetch DDI data for {ddi_id}, status code: {ddi_response.status_code}")
                        continue

                    # Save DDI data to file
                    file_path = os.path.join(save_path, f"{ddi_id}.txt")
                    with open(file_path, 'w') as file:
                        file.write(ddi_data)

                    # Output additional information
                    print(f"Downloaded {ddi_id} from {ddi_url}")
                    print(f"Status Code: {ddi_response.status_code}")
                    print(f"Headers: {ddi_response.headers}")
                    print(f"Content Length: {len(ddi_data)} bytes")
                    print(f"Saved to: {file_path}")

            else:
                url = f'https://rest.kegg.jp/get/{item}'
                response = requests.get(url, headers=headers)
                data = response.text

                if response.status_code != 200 or not data.strip():
                    print(f"Failed to fetch data for {item}, status code: {response.status_code}")
                    continue

                # Save data to file
                file_path = os.path.join(save_path, f"{item}.txt")
                with open(file_path, 'w') as file:
                    file.write(data)

                # Output additional information
                print(f"Downloaded {item} from {url}")
                print(f"Status Code: {response.status_code}")
                print(f"Headers: {response.headers}")
                print(f"Content Length: {len(data)} bytes")
                print(f"Saved to: {file_path}")

            if test_mode:
                break  # 仅下载第一个文件


if __name__ == "__main__":
    config_path = '../conf/drugkb_test.config'
    downloader = KEGGDownloader(config_path)

    config = configparser.ConfigParser()
    config.read(config_path)

    # 从配置文件中读取保存路径并测试下载每个路径的一个文件
    for i in range(int(config.get('kegg', 'data_path_num'))):
        start_id = "D00001"  # Example ID to start from
        source = config.get('kegg', f'source_url_{i + 1}')
        save_path = config.get('kegg', f'data_path_{i + 1}')

        print(f"Testing download for source: {source}")
        downloader.download(start_id, source, save_path, test_mode=True)
        print(f"Finished testing download for source: {source}")
