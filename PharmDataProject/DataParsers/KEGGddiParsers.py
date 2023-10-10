"""
  -*- encoding: utf-8 -*-
  @Author: panyifan
  @Time  : ${DATA}
  @Email: 2762376919@qq.com
  @function
"""
import requests
from PharmDataProject.DataSources.getId import GetId
from PharmDataProject.DatatoMongo.KEGGtoMongo import dataSave


class DdiData:
    def ddi_parse(drugs):
        for drug in drugs:
            url = 'https://rest.kegg.jp/ddi/' + drug
            try:
                response = requests.get(url)
                response.raise_for_status()
                html = response.text   #获取ddi数据
                lines = html.split('\n')
                drug_id = lines[0].split()[0].split(":")[1]
                print(drug_id)
                result = []
                for line in lines:
                    parts = line.split()
                    if len(parts) >= 4:
                        dr_value = parts[1].split(":")[1]
                        parameter = parts[2]
                        if "Enzyme:" in line:
                            enzyme_part = " ".join(parts[3:])
                            enzyme_values = enzyme_part.split("Enzyme:")[1].strip().split(" / ")
                        else:
                            enzyme_values = "unclassified"

                        interaction_entry = {
                            "dr_ids": dr_value,
                            "parameter": parameter,
                            "enzyme": enzyme_values
                        }

                        result.append(interaction_entry)

                data_dict = {
                    "drug id": drug_id,
                    "interaction": result
                }

                dataSave.save(data_dict, "PharmRG", "59.73.198.168", 27017, "KEGG_DDI", "readwrite", "readwrite")


            except requests.exceptions.RequestException as e:
                pass


if __name__ == "__main__":

     drugs=GetId.get_id("drug")
     DdiData.ddi_parse(drugs);   #6700




