"""
  -*- encoding: utf-8 -*-
  @Author: panyifan
  @Time  : ${DATA}
  @Email: 2762376919@qq.com
  @function
"""
from Bio.KEGG import REST
class GetId:
    def get_id(self):
        drug_list = REST.kegg_list(self).read()

        # get all ids
        items = []
        for line in drug_list.rstrip().split("\n"):
            ids, description = line.split("\t")
            items.append(ids)
        return items


if __name__ == "__main__":
    drugs=GetId.get_id('drug');
    print(drugs)