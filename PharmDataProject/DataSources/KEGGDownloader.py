"""
  -*- encoding: utf-8 -*-
  @Author: evie
  @Time  : ${DATA}
  @Email: 2762376919@qq.com
  @function
"""
import urllib
import random
from wsgiref import headers
import requests
from Bio.KEGG import REST
class KEGGDownloader:
    def get_id(self):
        drug_list = REST.kegg_list(self).read()
        # get all ids
        items = []
        for line in drug_list.rstrip().split("\n"):
            ids, description = line.split("\t")
            items.append(ids)
        return items
    def download(example):
        items = KEGGDownloader.get_id(example)
        for item in items:
            print(item)
            # proxy_handler = urllib.request.ProxyHandler({
            #     'https': '59.73.198.168:27017'
            # })
            opener = urllib.request.build_opener()
            ua_list = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0',
                       'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36 Edg/103.0.1264.62',
                       'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0',
                       'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.81 Safari/537.36 SE 2.X MetaSr 1.0'
                       ]
            opener.addheaders = [('User-Agent', random.choice(ua_list))]
            urllib.request.install_opener(opener)

            url = 'https://rest.kegg.jp/get/' + item
            response = requests.get(url)
            data=response.text
            # data = REST.kegg_get(item).read()
            yield data
if __name__ == "__main__":
    # drugs=KEGGDownloader.get_id('pathway');
    # print(len(drugs))
    KEGGDownloader.download("compound")