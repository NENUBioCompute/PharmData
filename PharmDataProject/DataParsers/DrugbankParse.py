# parser.py

import xmltodict




# 读取DrugBank XML文件并解析的类
class DrugBankXMLParser:

    def __init__(self, infile='../../data/drugbank/full database.xml'):
        self.infile = str(infile)

    def __iter__(self):
        # 打开并解析普通XML文件
        with open(self.infile, 'rb') as inf:
            for entry in self._parse_file(inf):
                yield entry

    def _parse_file(self, file_obj):
        # 使用xmltodict解析XML内容，逐个生成元素
        def item_callback(_, entry):
            self.entries.append(entry)
            return True  # 表示继续解析

        # 创建生成器来逐个生成解析的XML条目
        self.entries = []
        xmltodict.parse(file_obj, item_depth=2, attr_prefix='', item_callback=item_callback)
        for entry in self.entries:
            yield entry

if __name__ == '__main__':
    parser = DrugBankXMLParser()
    for entry in parser:
        print(entry)
        break
