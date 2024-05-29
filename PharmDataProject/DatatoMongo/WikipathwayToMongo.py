# -*- coding: utf-8 -*-


import configparser
import os
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.WikipathwayParser import WikipathwayParser


class WikipathwaytoMongo:
    def __init__(self, cfgfile):
        self.config = configparser.ConfigParser()
        self.cfgfile = cfgfile
        self.config.read(cfgfile)
        self.name = 'wikipathway'
        self.db = DBconnection(self.cfgfile, self.config.get(self.name, 'db_name'),
                                self.config.get(self.name, 'col_name_1'))

    def insert_pathway(self, pathway):
        self.db.collection.insert_one(pathway)
    def insert_all(self, pathways):
        for pathway in pathways:
            self.insert_pathway(pathway)
    def save_to_mongo(self):
        parser = WikipathwayParser(self.config, self.name)
        infile = os.path.join(self.config.get(self.name, 'data_path_1'),
                              self.config.get(self.name, 'source_url_1').split('/')[-1])
        pathways = parser.read_and_index_pathways(infile)

        try:
            print('begin')
            self.insert_all(pathways)

        except StopIteration:
            print("No pathways found in the file.")


if __name__ == '__main__':
    cfgfile = '../conf/drugkb_test.config'
    wikipathwaytomongo = WikipathwaytoMongo(cfgfile)
    wikipathwaytomongo.save_to_mongo()