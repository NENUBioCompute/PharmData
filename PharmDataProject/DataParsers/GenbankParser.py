"""
  -*- encoding: utf-8 -*-
  @Author: wangyang
  @Time  : 2023/10/06 12:20
  @Email: 2168259496@qq.com
  @function
"""
import csv
import json
import time
import requests

class GenbankParsers:

    def __init__(self, file_path, dest_path):
        self.info_path = file_path + "/gene_info"
        self.pubmed_path = file_path + "/gene2pubmed"
        self.dest_path = dest_path + "/genebank"

    def GenebankParsers(self) -> None:

        with open(self.info_path, 'r') as tsvfile:
            gen_info = csv.DictReader(tsvfile, dialect='excel-tab')

            with open(self.pubmed_path, 'r') as tsvfile:
                gen_pubmed = csv.DictReader(tsvfile, dialect='excel-tab')
                data = []
                info = next(gen_info)
                pubmed = next(gen_pubmed)
                info['PubMed_ID'] = []
                headers = info.keys()
                with open(self.dest_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=headers, delimiter='\t')

                    writer.writeheader()

                    while 1:

                        if pubmed == 0:
                            data.append(info)
                            info = next(gen_info, 0)
                            if info == 0:
                                break
                            info['PubMed_ID'] = []
                            continue

                        if int(info['#tax_id']) > int(pubmed['#tax_id']):
                            pubmed = next(gen_pubmed, 0)


                        elif int(info['#tax_id']) == int(pubmed['#tax_id']):

                            if int(info['GeneID']) > int(pubmed['GeneID']):
                                pubmed = next(gen_pubmed, 0)

                            elif int(info['GeneID']) == int(pubmed['GeneID']):
                                info["PubMed_ID"].append(pubmed["PubMed_ID"])
                                pubmed = next(gen_pubmed, 0)

                            else:
                                data.append(info)
                                info = next(gen_info, 0)
                                if info == 0:
                                    break
                                info['PubMed_ID'] = []


                        else:
                            data.append(info)
                            info = next(gen_info, 0)
                            if info == 0:
                                break
                            info['PubMed_ID'] = []

                        if len(data) >= 100000:
                            writer.writerows(data)
                            data = []
                     
                    if data:
                        writer.writerows(data)

