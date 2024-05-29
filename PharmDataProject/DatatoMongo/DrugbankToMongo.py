# indexer.py

from pymongo import MongoClient
from pprint import pprint
from PharmDataProject.DataParsers.DrugbankParse import update_entry_forindexing, DrugBankXMLParser

class Indexer:
    def __init__(self, db_name='test', host='localhost', port=27017, doctype='source_drugbank', slim=True):
        self.doctype = doctype
        self.db_name = db_name
        self.slim = slim
        self.client = MongoClient(host, port)
        self.db = self.client[db_name]
        self.collection = self.db[doctype]

    # Index DrugBank entry with MongoDB
    def mongodb_index_entry(self, entry):
        try:
            update_entry_forindexing(entry, slim=self.slim)
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
    parser = DrugBankXMLParser()
    indexer = Indexer(db_name='test')

    for entry in parser:
        # 处理每个解析的entry并入库
        indexer.mongodb_index_entry(entry)
        print(f"Inserted entry: {entry['_id']}")

