"""
  -*- encoding: utf-8 -*-
  @Author: evie
  @Time  : ${DATA}
  @Email: 2762376919@qq.com
  @function
"""
import json
import configparser
import os


class KEGGParsers:
    @staticmethod
    def parse_entry_data(entry_data):
        lines = entry_data.strip().split('\n')
        entry_info = {}
        current_key = None
        current_value = []
        for line in lines:
            if line.strip() == "///":
                break
            elif line.startswith(" "):
                current_value.append(line.strip())
            else:
                if current_key:
                    entry_info[current_key] = current_value[0] if len(current_value) == 1 else current_value
                if line.strip():
                    current_key, *current_value = line.strip().split(None, 1)
                    current_value = [current_value[0]] if current_value else []
        if current_key:
            entry_info[current_key] = current_value[0] if len(current_value) == 1 else current_value
        return entry_info

    @staticmethod
    def parse_file(file_path):
        with open(file_path, 'r') as file:
            data = file.read()
        entries = data.strip().split("\n///\n")
        return KEGGParsers.parse_entry_data(entries[0]) if entries else {}


if __name__ == "__main__":
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb_test.config'
    config.read(cfgfile)

    for i in range(1, int(config.get('kegg', 'data_path_num')) + 1):
        data_path = config.get('kegg', 'data_path_' + str(i))
        file_list = os.listdir(data_path)
        if not file_list:
            print(f"No files found in the directory: {data_path}")
            continue

        for file_name in file_list:
            file_path = os.path.join(data_path, file_name)
            print(f"Parsing file: {file_path}")
            parsed_data = KEGGParsers.parse_file(file_path)

            if parsed_data:
                print(parsed_data)  # 打印第一个解析结果
            else:
                print(f"No data parsed from file: {file_path}")

    print("Finished testing parsing for all sources.")
