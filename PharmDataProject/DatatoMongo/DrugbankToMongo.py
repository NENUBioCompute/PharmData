from pprint import pprint
from PharmDataProject.Utilities.Parser.objutils import *
from PharmDataProject.DataParsers.DrugbankParse import  DrugBankXMLParser
from PharmDataProject.Utilities.Database.dbutils import DBconnection
import configparser


class Indexer:
    def __init__(self, config_path="../conf/drugkb_test.config", slim=True):
        self.slim = slim
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.db = DBconnection(
            config_path,
            self.config.get('drugbank', 'db_name'),
            self.config.get('drugbank', 'col_name_1')
        )
        self.collection = self.db.collection

    def update_entry_forindexing(self, e, slim=True):

        # 需要处理为列表的属性
        LIST_ATTRS = ["transporters", "drug-interactions", "food-interactions",
                      "atc-codes", "affected-organisms", "targets", "enzymes",
                      "carriers", "groups", "salts", "products",
                      'pathways', 'go-classifiers', 'external-links',
                      'external-identifiers']

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

    # Index DrugBank entry with MongoDB
    def mongodb_index_entry(self, entry):
        try:
            self.update_entry_forindexing(entry, slim=self.slim)
            docid = self.getdrugid(entry)
            entry["_id"] = docid
            self.collection.insert_one(entry)
            r = True
        except Exception as e:
            pprint(e)
            r = False
        return r

    def getdrugid(self, e):
        if isinstance(e['drugbank-id'], list):
            eid = e['drugbank-id'][0]['#text']
        else:
            eid = e['drugbank-id']['#text']
        return eid


if __name__ == '__main__':

    # 初始化解析器和索引器
    parser = DrugBankXMLParser()
    indexer = Indexer()

    for entry in parser:
        # 处理每个解析的entry并入库
        indexer.mongodb_index_entry(entry)
        print(f"Inserted entry: {entry['_id']}")
        break
