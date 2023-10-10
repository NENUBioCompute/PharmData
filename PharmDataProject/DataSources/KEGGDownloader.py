"""
  -*- encoding: utf-8 -*-
  @Author: panyifan
  @Time  : ${DATA}
  @Email: 2762376919@qq.com
  @function
"""
from wsgiref import headers
import requests
from Bio.KEGG import REST
from PharmDataProject.DataParsers.KEGGParsers import Parse
from PharmDataProject.DatatoMongo.KEGGtoMongo import dataSave


class KEGGDownloader:
    def get_id(self):
        drug_list = REST.kegg_list(self).read()

        # get all ids
        items = []
        for line in drug_list.rstrip().split("\n"):
            ids, description = line.split("\t")
            items.append(ids)
        return items

    def download(self):
        items = KEGGDownloader.get_id("compound")
        # i = 415
        for item in items[415:]:
            url = 'https://rest.kegg.jp/get/' + item
            s = requests.session()
            s.headers = headers
            record = s.get(url)
            # record = REST.kegg_get(item)
            record.raise_for_status()
            data = record.text
            entries = data.strip().split("\n///\n")
            #     # 装到一个JSON格式
            json_data = Parse.convert_to_json(entries)

            dataSave.save(json_data, "PharmRG", "59.73.198.168", 27017, "KEGG_Compound", "readwrite", "readwrite")
            # i += 1
            return 0


if __name__ == "__main__":
    # drugs=KEGGDownloader.get_id('drug');
    # print(drugs)
    KEGGDownloader.download()