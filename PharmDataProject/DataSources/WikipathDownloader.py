import traceback
import configparser
import os
from tqdm import tqdm
from urllib import request


class HTTP:
    @staticmethod
    def download(url, dir, filename):
        os.makedirs(dir, exist_ok=True)
        save_path = os.path.join(dir, filename)
        print(f"Downloading from {url} to {save_path}")
        try:
            with tqdm(unit='B', unit_scale=True, miniters=1, desc=filename) as t:
                request.urlretrieve(url, save_path, reporthook=HTTP.report_hook(t))
            print(f"Downloaded to {save_path}")
        except Exception as e:
            print(f"Failed to download {url}. Error: {e}")

    @staticmethod
    def report_hook(t):
        last_b = [0]

        def update_to(b=1, bsize=1, tsize=None):
            if tsize is not None:
                t.total = tsize
            t.update((b - last_b[0]) * bsize)
            last_b[0] = b

        return update_to


class Wikipathway_Downloader:

    def start(self, cfgfile):
        config = configparser.ConfigParser()
        config.read(cfgfile, encoding="utf-8")
        url = config.get('wikipathway', 'source_url_1')
        data_path = config.get('wikipathway', 'data_path_1')
        filename = os.path.basename(url)

        print(f"Wikipathway download URL: {url}")
        print(f"Wikipathway save path: {data_path}")

        print(f"Checking if file needs to be downloaded to {os.path.join(data_path, filename)}")

        if not os.path.exists(os.path.join(data_path, filename)):
            try:
                HTTP.download(url, data_path, filename)
            except Exception:
                traceback.print_exc()

    def pause(self):
        pass

    def stop(self):
        pass

    def get_status(self):
        pass


if __name__ == '__main__':
    Wikipathway_Downloader().start('../conf/drugkb_test.config')
