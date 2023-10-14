"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2023/09/18 18:06
  @Email: 2665109868@qq.com
  @function
"""
import csv
import collections
import configparser

config = configparser.ConfigParser()
cfgfile = "../conf/drugkb.config"
config.read(cfgfile)

target_fields = [
    'bindingdb Target Chain  Sequence',
    'PDB ID(s) of Target Chain',
    'UniProt (SwissProt) Recommended Name of Target Chain',
    'UniProt (SwissProt) Entry Name of Target Chain',
    'UniProt (SwissProt) Primary ID of Target Chain',
    'UniProt (SwissProt) Secondary ID(s) of Target Chain',
    'UniProt (SwissProt) Alternative ID(s) of Target Chain',
    'UniProt (TrEMBL) Submitted Name of Target Chain',
    'UniProt (TrEMBL) Entry Name of Target Chain',
    'UniProt (TrEMBL) Primary ID of Target Chain',
    'UniProt (TrEMBL) Secondary ID(s) of Target Chain',
    'UniProt (TrEMBL) Alternative ID(s) of Target Chain',
]
chains_key = 'Number of Protein Chains in Target (>1 implies a multichain complex)'


def parse_bindingdb(path):
    """
    :param path:
    """
    csv.register_dialect('mydialect', delimiter='\t', quoting=csv.QUOTE_ALL)
    with open(path, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, dialect='mydialect')
        header = next(reader)
        chains_index = header.index(chains_key)
        target0_index = chains_index + 1
        ligand_fields = header[:chains_index + 1]
        for k, row in enumerate(reader):
            ligand_values = row[:chains_index + 1]
            rowdict = collections.OrderedDict(zip(ligand_fields, ligand_values))
            chains = []
            for i in range(int(rowdict[chains_key])):
                i_0 = target0_index + i * len(target_fields)
                i_1 = target0_index + (i + 1) * len(target_fields)
                target_values = row[i_0:i_1]
                chain = collections.OrderedDict(zip(target_fields, target_values))
                chains.append(chain)
            rowdict['chains'] = chains
            yield rowdict
