# -*- coding: utf-8 -*-
# @Time    : 2022/10/30 21:06
# @Author  : SunZehua
# @Email   : sunzh857@nenu.edu.cn
# @File    : Wikipathway_Parser.py
# @Software: PyCharm
import argparse
import gzip
import json
import os
import re
import time
import pymongo
from xml.etree.ElementTree import fromstring
from zipfile import ZipFile
from xmljson import yahoo
from PharmDataProject.Utilities.Database.dbutils import DBconnection
import configparser
class WikipathwayParser:
    def __init__(self,config,database_name):

        self.dbc =  DBconnection(cfgfile, config.get(database_name, 'db_name'),
                      config.get(database_name, 'col_name_1'))
    # Read WikiPathways xml file, index using the function indexf
    # If the input file is a folder iterate over files in the folder
    def read_and_index_pathways(self,infile, index):
        # (infile,  WikipathwayParser.mongodb_index_pathway, index)
        i = 0
        t1 = time.time()
        if os.path.isdir(infile):
            for child in os.listdir(infile):
                c = os.path.join(infile, child)
                if child.endswith(".zip"):
                    i += self.read_and_index_wikipathways_zipfile(c,index)
                else:
                    self.read_and_index_wikipathways_file(c, index)
                    i += 1
        elif infile.endswith(".zip"):
            i += self.read_and_index_wikipathways_zipfile(infile,index)
        else:
            self.read_and_index_wikipathways_file(infile, index)
            i = 1
        t2 = time.time()
        #print("-- %d files have been processed, in %dms" % (i, (t2 - t1) * 1000))
        return None

    # TODO: remove 'Graphics' and 'GraphId' elements
    # Read WikiPathways zipfile, index using the function indexf
    def read_and_index_wikipathways_zipfile(self, zipfile, index):
        i = 0
        with ZipFile(zipfile) as myzip:
            for fname in myzip.namelist():
                #print("Reading %s " % fname)
                with myzip.open(fname) as jfile:
                    xml = jfile.read()
                    if not isinstance(xml, str):
                        xml = xml.decode('utf-8')
                    r = self.read_and_index_wikipathways_xml(xml, index)
                    i += r
        return i

    # Read WikiPathways file, index using the function indexf
    def read_and_index_wikipathways_file(self,infile, index):
        infile = str(infile)
        #print("Reading %s " % infile)
        if infile.endswith(".gz"):
            f = gzip.open(infile, 'rt')
        else:
            f = open(infile, 'r')
        xmls = f.read()
        r = self.read_and_index_wikipathways_xml(xmls, index)
        return r

    # Index WikiPathways xml using the function indexf
    def read_and_index_wikipathways_xml(self,xml,index):
        xml = re.sub(' xmlns="[^"]+"', '', xml, count=1)
        pathway = yahoo.data(fromstring(xml))["Pathway"]
        # Delete fields that would normally be used for rendering images
        for a in ["Biopax", "BiopaxRef", "Graphics", "Shape", "Group", "InfoBox"]:
            if a in pathway:
                del pathway[a]
        for a in ["Interaction", "DataNode", "Label"]:
            if a in pathway:
                for i in pathway[a]:
                    if isinstance(i, str):
                        continue
                    del i["Graphics"]
                    if "GraphId" in i:
                        del i["GraphId"]
        r = self.mongodb_index_pathway(pathway)
        return r


    def mongodb_index_pathway(self,ba):


        self.dbc.collection.insert_one(ba)
        r = 1
        return r


if __name__ == '__main__':
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)


    k = WikipathwayParser(config, 'wikipathway')
    k.read_and_index_pathways('/home/zhaojingtong/pharmrg/pharmrg_data/wikipathway/wikipathways-20231010-gpml-Homo_sapiens.zip', None)
