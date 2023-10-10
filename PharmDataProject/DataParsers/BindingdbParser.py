"""
  -*- encoding: utf-8 -*-
  @Author: zhuliujinxiang
  @Time  : 2023/10/10 21:53
  @Email: deepwind32@163.com
  @function 
"""
import logging
import csv


class BindingdbParser:
    def __init__(self, csv_path: str, all_field=True):
        self.csv_path = csv_path
        self.csv_property = {"newline": '\n', "encoding": 'utf-8', "delimiter": ','}
        self.selected_fields = []
        self.removed_fields = []
        self.all_field = all_field

    def set_csv_property(self, newline='\n', encoding='utf-8', delimiter=',') -> None:
        """
        Set the read properties of the CSV file
        :param newline: Delimiter between lines in the CSV file, default "\\n"
        :param encoding: CSV file encoding, default is "utf-8"
        :param delimiter: Delimiter between fields in the CSV file, default is ","
        :return: None
        """
        self.csv_property = {"newline": newline, "encoding": encoding, "delimiter": delimiter}

    def set_fields(self, fields_needed: list[str] = None, fields_removed: list[str] = None) -> None:
        """
        Set the fields that you want to keep or delete.
        :param fields_needed: Field to be reserved. The format is [field_name1, field_name2].
        :param fields_removed: Field to be deleted. The format is [field_name1, field_name2].
        :return: None
        """
        if fields_needed and fields_removed:
            logging.warning(
                "Parameter error! The fields that need to be kept and the fields that need to be deleted cannot be given at the same time!")
            raise SyntaxError(
                "The fields that need to be kept and the fields that need to be deleted cannot be given at the same time!")
        if not (fields_needed or fields_removed):
            return
        if fields_needed:
            self.selected_fields = fields_needed
        elif fields_removed:
            self.removed_fields = fields_removed

    def parse(self) -> iter:
        """
        对指定CSV文件中的数据进行解析。
        :return: None
        """
        with open(self.csv_path, 'r', newline=self.csv_property["newline"],
                  encoding=self.csv_property["encoding"]) as csvfile:
            db_reader = csv.reader(csvfile, delimiter=self.csv_property["delimiter"])
            header = next(db_reader)
            field_allowed = list(range(len(header)))

            if not self.all_field:
                try:
                    if self.selected_fields:
                        field_allowed = []
                        for field in self.selected_fields:
                            field_allowed.append(header.index(field))
                    elif self.removed_fields:
                        field_allowed = [i for i in field_allowed if
                                         i not in [header.index(field) for field in self.removed_fields]]
                except ValueError as e:
                    logging.warning(e)
                    raise ValueError("指定的字段不存在") from e

            header = [header[i] for i in field_allowed]
            logging.info(f"保留的字段为{header}")
            for row in db_reader:
                drug = dict(zip(header, [row[i] for i in field_allowed]))
                yield drug


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%m/%d %I:%M:%S')
    csv_path = r"D:\my files\Work\当前项目\2023 PharmRG\drug database\data\OFFSIDES.csv"
    iter_drug = BindingdbParser(csv_path).parse()
    for index, item in enumerate(iter_drug):
        if index == 5:
            break
        print(item)
