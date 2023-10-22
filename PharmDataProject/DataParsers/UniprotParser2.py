# -*- ecoding: utf-8 -*-
# @Author: zhaojingtong
# @Time  : 2022/10/02 16:43
# @Email: 2665109868@qq.com
# @function
# @LastEditTime: 2023-09-30 10:10:10
# @LastEditors: Juncong Chen

from typing import Optional, List, Any, Dict

from xml.etree import ElementTree as ET
from PharmDataProject.Utilities.FileDealers.FileSystem import *
import xmltodict
import configparser
from PharmDataProject.Utilities.Database.dbutils import DBconnection
def _strip(elem, extract_name: str) -> str:
    """
    return: extract tag_name or namespace and return it
    """
    t = elem.tag
    idx = t.rfind('}')
    if idx != -1:
        if extract_name == 'tag_name':
            return t[idx + 1:]
        elif extract_name == 'namespace':
            return t[1:idx]

class UniprotParser:
    """
    parse xml file
    """
    def __IterRead(file_path: str, records_tag: Optional[str]) -> iter:
        """
        function：Returns the iterator of the specified tag
        return: return the iter of elem
        """
        if file_is_exists.__func__(file_path):
            with open(file_path, 'rb') as xml_file:
                context = iter(ET.iterparse(xml_file, events=('start', 'end')))
                _, root = next(context)
                for event, elem in context:

                    t_name = _strip(elem, 'tag_name')
                    if root is None:
                        root = elem
                    if event == 'end' and t_name == records_tag:
                        yield elem
                        if root != elem:
                            root.clear()  # Free memory
        else:
            raise Exception("FileNotFound")

    @staticmethod
    def parse_xml_all(file_path: str, records_tag: Optional[str]) -> dict:
        """
        func:Extract all data of the specified tag
        return: dict
        """
        elems_iter = UniprotParser.__IterRead(file_path, records_tag)
        for elem in elems_iter:
            namespace = _strip(elem, 'namespace')
            ET.register_namespace('', namespace)
            xml_data = ET.tostring(elem).decode()
            xml2dict = xmltodict.parse(xml_data)
            cleanXml2dict = UniprotParser.__remover(xml2dict)
            yield cleanXml2dict

    @classmethod
    def __remover(self,data):
        '''
        func:Remove specific characters like @ and #
        param data:Specify data set
        return:Any
        '''
        if isinstance(data, list):
            new_list = []
            for item in data:
                new_list.append(self.__remover(item))
            return new_list
        elif isinstance(data, dict):
            new_dict = {}
            for key, value in data.items():
                if key[0] in ["@", "#"]:
                    new_key = key[1:]
                else:
                    new_key = key
                new_dict[new_key] = self.__remover(value)
            return new_dict
        else:
            return data

    def save_mongodb(self, batch_size=1000):
        config = configparser.ConfigParser()
        cfgfile = '../conf/drugkb.config'
        config.read(cfgfile)
        infile = config.get('uniprot', 'data_path_1') + 'uniprot_sprot.xml'
        dics_data = UniprotParser.parse_xml_all(infile, 'entry')
        db = DBconnection(cfgfile, config.get('uniprot', 'db_name'),
                          config.get('uniprot', 'col_name_1'))

        batch = []
        for dic_data in dics_data:
            protein_data = dic_data['entry']
            batch.append(protein_data)

            if len(batch) == batch_size:
                db.collection.insert_many(batch)
                batch = []
        # Insert the remaining records
        if batch:
            db.collection.insert_many(batch)

if __name__ == '__main__':
    uniprot_parser = UniprotParser()
    uniprot_parser.save_mongodb(batch_size=1000)