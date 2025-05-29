# -*- coding: utf-8 -*-
# @Time    : 2022/10/30 21:06
# @Author  : SunZehua
# @Email   : sunzh857@nenu.edu.cn
# @File    : Wikipathway_Parser.py
# @Software: PyCharm

import gzip
import os
import re
from xml.etree.ElementTree import fromstring
from zipfile import ZipFile
from xmljson import yahoo
import configparser


def _strip_namespace(xml):
    return re.sub(' xmlns="[^"]+"', '', xml, count=1)


class WikipathwayParser:
    def __init__(self, config, section_name):
        self.config = config
        self.section_name = section_name

    def read_and_index_pathways(self, infile):
        if os.path.isdir(infile):
            for child in os.listdir(infile):
                c = os.path.join(infile, child)
                if child.endswith(".zip"):
                    yield from self.read_and_index_wikipathways_zipfile(c)
                else:
                    yield from self.read_and_index_wikipathways_file(c)
        elif infile.endswith(".zip"):
            yield from self.read_and_index_wikipathways_zipfile(infile)
        else:
            yield from self.read_and_index_wikipathways_file(infile)

    def read_and_index_wikipathways_zipfile(self, zipfile):
        with ZipFile(zipfile) as myzip:
            for fname in myzip.namelist():
                with myzip.open(fname) as jfile:
                    xml = jfile.read()
                    if not isinstance(xml, str):
                        xml = xml.decode('utf-8')
                    yield from self.read_and_index_wikipathways_xml(xml)

    def read_and_index_wikipathways_file(self, infile):
        if infile.endswith(".gz"):
            with gzip.open(infile, 'rt') as f:
                xmls = f.read()
        else:
            with open(infile, 'r') as f:
                xmls = f.read()
        yield from self.read_and_index_wikipathways_xml(xmls)

    def read_and_index_wikipathways_xml(self, xml):
        xml = _strip_namespace(xml)
        pathway = yahoo.data(fromstring(xml))["Pathway"]
        for a in ["Biopax", "BiopaxRef", "Graphics", "Shape", "Group", "InfoBox"]:
            if a in pathway:
                del pathway[a]
        for a in ["Interaction", "DataNode", "Label"]:
            if a in pathway:
                for i in pathway[a]:
                    if isinstance(i, str):
                        continue
                    if "Graphics" in i:
                        del i["Graphics"]
                    if "GraphId" in i:
                        del i["GraphId"]
        yield pathway


if __name__ == '__main__':
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb_test.config'
    config.read(cfgfile)
    infile = os.path.join(config.get('wikipathway', 'data_path_1'), 'wikipathways-20231010-gpml-Homo_sapiens.zip')

    parser = WikipathwayParser(config, 'wikipathway')
    print(f"Processing file: {infile}")
    for root, dirs, files in os.walk(os.path.dirname(infile)):
        for f in files:
            fi = os.path.join(root, f)
            print(f"Processing file: {fi}")
            for record in parser.read_and_index_pathways(fi):
                print("First record parsed:", record)
                break
            break
