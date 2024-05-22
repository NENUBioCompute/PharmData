# -*- coding: utf-8 -*-
# @Author: zhaojingtong
# @Time  : 2022/10/02 16:43
# @Email: 2665109868@qq.com
# @function
# @LastEditTime: 2023-09-30 10:10:10
# @LastEditors: Juncong Chen

from typing import Optional
from xml.etree import ElementTree as ET
import xmltodict
import configparser
import os
import gzip


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
    return t


class UniprotParser:
    """
    parse xml file
    """

    @staticmethod
    def __IterRead(file_path: str, records_tag: Optional[str]) -> iter:
        """
        function：Returns the iterator of the specified tag
        return: return the iter of elem
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        open_func = gzip.open if file_path.endswith('.gz') else open

        with open_func(file_path, 'rb') as xml_file:
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
    def __remover(self, data):
        """
        func:Remove specific characters like @ and #
        param data:Specify data set
        return: Any
        """
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


# Test logic in ifmain
if __name__ == '__main__':
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb_test.config'
    config.read(cfgfile)
    infile = config.get('uniprot', 'data_path_1') + 'uniprot_sprot.xml.gz'

    uniprot_parser = UniprotParser()
    print(f"Processing file: {infile}")
    for root, dirs, files in os.walk(os.path.dirname(infile)):
        for f in files:
            fi = os.path.join(root, f)
            print(f"Processing file: {fi}")
            for record in uniprot_parser.parse_xml_all(fi, 'entry'):
                print("First record parsed:", record)
                break
            break
