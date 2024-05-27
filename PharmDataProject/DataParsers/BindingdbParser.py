import csv
import collections
import configparser
import os

class BindingDbParser:
    def __init__(self):
        self.cfgfile = "../conf/drugkb_test.config"
        self.config = configparser.ConfigParser()
        self.config.read(self.cfgfile)
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
        self.path = self.config.get('bindingdb', 'data_path_1')
        files = os.listdir(self.path)

        # Filter out TSV files
        tsv_files = [file for file in files if file.endswith('.tsv')][0]
        self.tsv_path = self.path+tsv_files


    def parse_bindingdb(self):
        """
        Parse the BindingDB file.

        :param path: Path to the BindingDB TSV file.
        """
        csv.register_dialect('mydialect', delimiter='\t', quoting=csv.QUOTE_ALL)

        with open(self.tsv_path, 'r', encoding='utf-8') as csvfile:
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
