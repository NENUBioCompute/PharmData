"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2024/04/05 21:31
  @Email: 2665109868@qq.com
  @function
"""
import gzip
import json

class TransformJsonConverter:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def extract_gz_to_json(self):
        with gzip.open(self.input_file, 'rb') as f_in:
            content = f_in.read().decode('utf-8')

        data = []
        lines = content.split('\n')
        header = lines[0].split('\t')
        for line in lines[1:]:
            if line:
                values = line.split('\t')
                entry = dict(zip(header, values))
                data.append(entry)

        with open(self.output_file, 'w') as f_out:
            json.dump(data, f_out, indent=4)

        print(f"文件 {self.output_file} 转换完成")


if __name__ == '__main__':
    # 创建类实例并调用方法
    input_file = '9606.protein_chemical.links.detailed.v5.0.tsv.gz'
    output_file = '9606.protein_chemical.links.detailed.v5.0.json'

    converter = TransformJsonConverter(input_file, output_file)
    converter.extract_gz_to_json()
