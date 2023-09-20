# -*- ecoding: utf-8 -*-
# @Author: zhaojingtong
# @Time  : 2022/10/02 16:43
# @Email: 2665109868@qq.com
# @function

from typing import Optional

from xml.etree import ElementTree as ET
from PharmDataProject.Utilities.FileDealers.FileSystem import *
import xmltodict


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


class XMLDealer:
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
        elems_iter = XMLDealer.__IterRead(file_path, records_tag)

        for elem in elems_iter:
            namespace = _strip(elem, 'namespace')
            ET.register_namespace('', namespace)
            xml_data = ET.tostring(elem).decode()
            xml2dict = xmltodict.parse(xml_data)
            yield xml2dict


if __name__ == '__main__':

    dics_data = XMLDealer.parse_xml_all('D:/Users/赵靖通/Desktop/uniprot_sprot.xml', 'entry')
    for dic_data in dics_data:
        print(dic_data)
