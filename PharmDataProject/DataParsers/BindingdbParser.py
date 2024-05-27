import csv
import collections
import configparser
import os

class BindingDbParser:
    def __init__(self, config_path=r"D:\PharmData\PharmDataProject\conf\drugkb_test.config"):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.target_fields = [
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
        self.chains_key = 'Number of Protein Chains in Target (>1 implies a multichain complex)'

    def parse_bindingdb(self, path):
        """
        Parse the BindingDB file.

        :param path: Path to the BindingDB TSV file.
        """
        csv.register_dialect('mydialect', delimiter='\t', quoting=csv.QUOTE_ALL)
        with open(path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, dialect='mydialect')
            header = next(reader)
            chains_index = header.index(self.chains_key)
            target0_index = chains_index + 1
            ligand_fields = header[:chains_index + 1]
            for row in reader:
                ligand_values = row[:chains_index + 1]
                rowdict = collections.OrderedDict(zip(ligand_fields, ligand_values))
                chains = []
                for i in range(int(rowdict[self.chains_key])):
                    i_0 = target0_index + i * len(self.target_fields)
                    i_1 = target0_index + (i + 1) * len(self.target_fields)
                    target_values = row[i_0:i_1]
                    chain = collections.OrderedDict(zip(self.target_fields, target_values))
                    chains.append(chain)
                rowdict['chains'] = chains
                yield rowdict

if __name__ == "__main__":
    # 配置文件路径
    config_path = r"D:\PharmData\PharmDataProject\conf\drugkb_test.config"
    
    # 创建解析器对象
    parser = BindingDbParser(config_path)
    
    # 获取解析文件的路径
    tsv_path = parser.config.get('bindingdb', 'data_path_1')
    print(tsv_path)
    
    # 确保路径是绝对路径
    tsv_path = os.path.abspath(os.path.join(os.path.dirname(config_path), tsv_path))
    
    print(f"Parsing TSV file from: {tsv_path}")
    
    # 解析并输出第一个数据
    for row in parser.parse_bindingdb(tsv_path):
        print(row)
        break