# -*- coding: utf-8 -*-
# @Time    : 2022/9/3 12:20
# @Author  : SunZehua
# @Email   : sunzh857@nenu.edu.cn
# @File    : DrugBank_Parser.py
# @Software: PyCharm
from __future__ import print_function
from zipfile import ZipFile
import pymongo
import xmltodict
from pymongo import IndexModel
from dbutils import DBconnection
from objutils import *


# List attributes, processed by function unifylistattributes()
class DrugBank():
    def __init__(self, cfgfile, mydb, myset):
        self.LIST_ATTRS = ["transporters", "drug-interactions", "food-interactions",
                           "atc-codes", "affected-organisms", "targets", "enzymes",
                           "carriers", "groups", "salts", "products",
                           'pathways', 'go-classifiers', 'external-links',
                           'external-identifiers']

        self.TEXT_FIELDS = ["description", "atc-codes.level.#text",
                            "go-classifiers.description",
                            "mechanism-of-action",
                            "general-references.references.articles.article.citation",
                            "targets.references.articles.article.citation",
                            "targets.polypeptide.gene-name",
                            "drug interactions.description",
                            "targets.polypeptide.general-function",
                            "targets.polypeptide.specific-function"]
        self.db = DBconnection(cfgfile, mydb, myset,collection="Drugdata2")

    # Update DrugBank entry for better database representation

    def update_entry_forindexing(self, e, slim=True):
        unifylistattributes(e, self.LIST_ATTRS)
        unifylistattribute(e, 'categories', 'category')
        if 'pathways' in e:
            unifylistattributes(e['pathways'], ['drugs'])
        if "products" in e:
            for product in e["products"]:
                checkbooleanattributes(product,
                                       ["generic", "approved", "over-the-counter"])
                # Make sure type of numeric attributes are numeric
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
                                unifylistattributes(pp, self.LIST_ATTRS)
                        else:
                            if slim is True:
                                del i['polypeptide']['amino-acid-sequence']
                                del i['polypeptide']['gene-sequence']
                            unifylistattributes(i['polypeptide'], self.LIST_ATTRS)
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

    # parse_drugbank_xmlfile(infile, indxr.mongodb_index_entry)
    def parse_drugbank_xmlfile(self, infile, indexf):
        infile = str(infile)
        print("Reading/indexing %s " % infile)
        if infile.endswith(".zip"):
            with ZipFile(infile) as zipf:
                for fname in zipf.namelist():
                    with zipf.open(fname) as inf:
                        xmltodict.parse(inf, item_depth=2, attr_prefix='',
                                        item_callback=indexf)
        else:
            with open(infile, 'rb', buffering=1000) as inf:
                xmltodict.parse(inf, item_depth=2, attr_prefix='',
                                item_callback=indexf)
        # print("\nCompleted")

    def mongodb_indices(self, mdb):
        indx = [(field, pymongo.TEXT) for field in self.TEXT_FIELDS]
        mdb.create_indexes([IndexModel(indx,
                                       name="text-index-for-selected-fields")])
        indx_fields = [
            "name", "products.name",
            "classification.class", "drug-interactions.name",
            "targets.polypeptide.gene-name",
            "protein_associations.protein.protein_accession",
            "metabolite_associations.metabolite.accession",
            "metabolite_associations.metabolite.name",
            [('name', pymongo.ASCENDING),
             ('drug-interactions.name', pymongo.ASCENDING)]
        ]
        for field in indx_fields:
            mdb.create_index(field)

    def mongodb_index_entry(self, _, entry):
        try:
            self.update_entry_forindexing(entry, slim=True)
            docid = self.db.getdrugid(entry)
            entry["_id"] = docid
            self.db.collection.insert_one(entry)
            self.db.reportprogress(True)
            r = True
        except Exception as e:
            print(e)
            r = False
        return r

    def main(self, infile):

        self.parse_drugbank_xmlfile(infile, self.mongodb_index_entry)
        self.mongodb_indices(self.db.collection)
        print('finished!')


if __name__ == '__main__':
    infile = 'full database.xml'
    drugbank = DrugBank('./drugkb.config', 'DrugKB', 'source_drugbank')
    drugbank.main(infile)
