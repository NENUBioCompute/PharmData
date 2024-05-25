# -*- coding: utf-8 -*-


import configparser
import os
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.WikipathwayParser import WikipathwayParser


class WikipathwayDBInserter:
    def __init__(self, config, section_name):
        self.dbc = DBconnection('../conf/drugkb_test.config', config.get(section_name, 'db_name'),
                                config.get(section_name, 'col_name_1'))

    def insert_pathway(self, pathway):
        self.dbc.collection.insert_one(pathway)
        return 1

    def insert_all(self, pathways):
        for pathway in pathways:
            self.insert_pathway(pathway)


if __name__ == '__main__':

    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb_test.config'
    config.read(cfgfile)

    parser = WikipathwayParser(config, 'wikipathway')
    inserter = WikipathwayDBInserter(config, 'wikipathway')

    infile = os.path.join(config.get('wikipathway', 'data_path_1'), 'wikipathways-20231010-gpml-Homo_sapiens.zip')
    pathways = parser.read_and_index_pathways(infile)

    # Insert the first dictionary for testing
    try:
        first_pathway = next(pathways)
        print("First pathway parsed:", first_pathway)
        inserter.insert_pathway(first_pathway)
        print("First pathway inserted into the database")
    except StopIteration:
        print("No pathways found in the file.")