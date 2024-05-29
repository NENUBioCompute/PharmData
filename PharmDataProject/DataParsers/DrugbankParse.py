# parser.py

import xmltodict
from PharmDataProject.Utilities.Parser.objutils import *

# 定义MongoDB默认的集合名称
DOCTYPE = 'source_drugbank'

# 需要处理为列表的属性
LIST_ATTRS = ["transporters", "drug-interactions", "food-interactions",
              "atc-codes", "affected-organisms", "targets", "enzymes",
              "carriers", "groups", "salts", "products",
              'pathways', 'go-classifiers', 'external-links',
              'external-identifiers']

# 更新DrugBank条目以便更好地进行数据库表示
def update_entry_forindexing(e, slim=True):
    # 将某些属性统一处理为列表
    unifylistattributes(e, LIST_ATTRS)
    unifylistattribute(e, 'categories', 'category')
    if 'pathways' in e:
        unifylistattributes(e['pathways'], ['drugs'])
    if "products" in e:
        for product in e["products"]:
            # 检查布尔类型的属性并进行处理
            checkbooleanattributes(product,
                                   ["generic", "approved", "over-the-counter"])
    # 确保数值属性的类型为数值型
    atts = ["carriers", "enzymes", "targets", "transporters"]
    for att in atts:
        if att in e:
            for i in e[att]:
                if 'position' in i:
                    i['position'] = int(i['position'])
                if 'polypeptide' in i:
                    if isinstance(i['polypeptide'], list):
                        for pp in i['polypeptide']:
                            if slim is True:
                                del pp['amino-acid-sequence']
                                del pp['gene-sequence']
                            unifylistattributes(pp, LIST_ATTRS)
                    else:
                        if slim is True:
                            del i['polypeptide']['amino-acid-sequence']
                            del i['polypeptide']['gene-sequence']
                        unifylistattributes(i['polypeptide'], LIST_ATTRS)
    if slim is True:
        if 'sequences' in e:
            del e['sequences']
        if 'patents' in e:
            del e['patents']
    atts = ["average-mass", "monoisotopic-mass"]
    for att in atts:
        if att in e:
            e[att] = float(e[att])
        if 'salts' in e and att in e['salts']:
            e['salts'][att] = float(e['salts'][att])

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
