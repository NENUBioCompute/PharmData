"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2024/05/26 17:41
  @Email: 2665109868@qq.com
  @function
"""
import re
from Bio import SeqIO
import configparser
class EnsemblParser:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('../conf/drugkb.config')
        self.save_path = self.config.get('ensembl', 'data_path_1')
# 解析函数
    def parse_ensembl(self):
        for record in SeqIO.parse(self.save_path+'Homo_sapiens.GRCh38.cds.all.fa', "fasta"):
            header = record.description
            # 提取信息
            ensembl_id = record.id

            chromosome_info = re.search(r'chromosome:([^ ]+)', header)
            chromosome_info = chromosome_info.group(1) if chromosome_info else None

            gene_info = re.search(r'gene:(ENSG\d+\.\d+)', header)
            gene_info = gene_info.group(1) if gene_info else None

            gene_biotype = re.search(r'gene_biotype:(\w+)', header)
            gene_biotype = gene_biotype.group(1) if gene_biotype else None

            transcript_biotype = re.search(r'transcript_biotype:(\w+)', header)
            transcript_biotype = transcript_biotype.group(1) if transcript_biotype else None

            gene_symbol = re.search(r'gene_symbol:(\w+)', header)
            gene_symbol = gene_symbol.group(1) if gene_symbol else None

            description = re.search(r'description:([^[]+)', header)
            description = description.group(1).strip() if description else None

            hgnc_info = re.search(r'\[Source:([^]]+)]', header)
            hgnc_info = hgnc_info.group(1) if hgnc_info else None

            parsed_data={
                'ensembl_id': ensembl_id,
                'chromosome_info': chromosome_info,
                'gene_info': gene_info,
                'gene_biotype': gene_biotype,
                'transcript_biotype': transcript_biotype,
                'gene_symbol': gene_symbol,
                'description': description,
                'hgnc_info': hgnc_info,
                'sequence': str(record.seq)
            }
            yield parsed_data
if __name__ == '__main__':
    ensembl_parser = EnsemblParser()
    ensembl_data = ensembl_parser.parse_ensembl()
    print(next(ensembl_data))
