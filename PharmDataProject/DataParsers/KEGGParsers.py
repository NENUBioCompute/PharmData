"""
  -*- encoding: utf-8 -*-
  @Author: evie
  @Time  : ${DATA}
  @Email: 2762376919@qq.com
  @function
"""
import json
from PharmDataProject.DataSources.KEGGDownloader import KEGGDownloader
import configparser
from PharmDataProject.Utilities.Database.dbutils import DBconnection
import requests
import time
import random
class KEGGParsers:
    # parse drugs、compounds、disease、pathway
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

    # parse ddi
    def ddi_parse(drugs):
        for drug in drugs:
            url = 'https://rest.kegg.jp/ddi/' + drug
            try:
                response = requests.get(url)
                response.raise_for_status()
                html = response.text   #获取ddi数据
                lines = html.split('\n')
                drug_id = lines[0].split()[0].split(":")[1]
                print(drug_id)
                result = []
                for line in lines:
                    parts = line.split()
                    if len(parts) >= 4:
                        dr_value = parts[1].split(":")[1]
                        parameter = parts[2]
                        if "Enzyme:" in line:
                            enzyme_part = " ".join(parts[3:])
                            enzyme_values = enzyme_part.split("Enzyme:")[1].strip().split(" / ")
                        else:
                            enzyme_values = "unclassified"

                        interaction_entry = {
                            "dr_ids": dr_value,
                            "parameter": parameter,
                            "enzyme": enzyme_values
                        }

                        result.append(interaction_entry)

                data_dict = {
                    "drug id": drug_id,
                    "interaction": result
                }

                # KEGGtoMongo.save(data_dict, "PharmRG", "59.73.198.168", 27017, "KEGG_DDI", "readwrite", "readwrite")

                db.collection.insert_one(data_dict)
            except requests.exceptions.RequestException as e:
                pass
    # put in json
    def convert_to_json(entries):
        json_data = []
        for entry in entries:
            entry_info = KEGGParsers.parse_entry_data(entry)
            json_data.append(entry_info)
        return json_data[0]
    # json file
    def save_to_json_file(json_data, file_path):
        with open(file_path, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)


if __name__ == "__main__":

    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb.config'
    config.read(cfgfile)

    for i in range(1, int(config.get('kegg', 'data_path_num'))):
        db = DBconnection(cfgfile, config.get('kegg', 'db_name'),
                          config.get('kegg', 'col_name_' + str(i + 1)))
        database_name = config.get('kegg','source_url_'+str(i+1))
        data_save_path = config.get('kegg','json_path_'+str(i+1))
        if database_name=='ddi':
            drugs=KEGGDownloader.get_id("drug")
            KEGGParsers.ddi_parse(drugs);
        else:
            generator = KEGGDownloader.download(database_name)
            # use generator get next data
            json_data_list = []
            try:
                while True:
                    data = next(generator)
                    entries = data.strip().split("\n///\n")
                    # 装到一个JSON格式
                    json_data = KEGGParsers.convert_to_json(entries)
                    sleep_time = random.uniform(4, 9)
                    time.sleep(sleep_time)
                    print(json_data)
                    # KEGGParsers.save_to_json_file(json_data,data_save_path)
                    json_data_list.append(json_data)
                    db.collection.insert_one(json_data)
                    # print(json_data)
            except StopIteration:
                # 将列表转换为 JSON 字符串
                json_string = json.dumps(json_data_list, indent=2)  # indent参数用于缩进格式化，可选

                # 将 JSON 字符串写入文件
                with open(data_save_path, 'w') as json_file:
                    json_file.write(json_string)
                pass  # 生成器结束

