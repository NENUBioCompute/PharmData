
"""
  -*- encoding: utf-8 -*-
  @Author: panyifan
  @Time  : ${DATA}
  @Email: 2762376919@qq.com
  @function
"""
import json
class Parse:
    def parse_entry_data(entry_data):
        lines = entry_data.strip().split('\n')
        i=0
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
                    if len(current_value) == 1:
                        if i==0:
                            entry_info[current_key] = current_value[0].split(' ')[0]
                            i+=1
                        else:
                            entry_info[current_key] = current_value[0]
                    else:
                        entry_info[current_key] = current_value

                current_key, *current_value = line.strip().split(None, 1)
                if current_value:
                    current_value = [current_value[0]]
                else:
                    current_value = []

        #last one
        if current_key:
            if len(current_value) == 1:
                entry_info[current_key] = current_value[0]
            else:
                entry_info[current_key] = current_value

        return entry_info

    # put in json
    def convert_to_json(entries):
        json_data = []
        for entry in entries:
            entry_info = Parse.parse_entry_data(entry)
            json_data.append(entry_info)
        return json_data

    # json file
    def save_to_json_file(json_data, file_path):
        with open(file_path, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)



if __name__ == "__main__":
    with open("../../data/H01476.txt", "r", encoding="latin1") as file:
        data = file.read()
        entries = data.strip().split("\n///\n")
        # JSON格式
        json_data = Parse.convert_to_json(entries)
       # 保存到JSON文件
        Parse.save_to_json_file(json_data, '../../json/H01476.json')
