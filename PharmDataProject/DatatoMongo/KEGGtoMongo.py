import configparser
import os
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.KEGGParsers import KEGGParsers


class KeggtoMongo:
    def __init__(self, db):
        self.db = db

    def insert_into_mongodb(self, data):
        self.db.collection.insert_one(data)

    def process_files(self, folder_path):
        file_list = os.listdir(folder_path)
        if not file_list:
            print(f"No files found in the directory: {folder_path}")
            return

        for file_name in file_list:
            file_path = os.path.join(folder_path, file_name)
            print(f"Parsing file: {file_path}")
            parsed_data = KEGGParsers.parse_file(file_path)

            if parsed_data:
                self.insert_into_mongodb(parsed_data)
                print(f"Inserted data from {file_path} into MongoDB")
                # break  # 只插入每个文件夹解析出来的第一个字典
            else:
                print(f"No data parsed from file: {file_path}")


if __name__ == "__main__":
    config_path = '../conf/drugkb_test.config'
    config = configparser.ConfigParser()
    config.read(config_path)

    for i in range(1, int(config.get('kegg', 'data_path_num')) + 1):
        db = DBconnection(config_path, config.get('kegg', 'db_name'),
                          config.get('kegg', 'col_name_' + str(i)))
        json_data_path = config.get('kegg', 'data_path_' + str(i))

        kegg_to_mongo = KeggtoMongo(db)
        kegg_to_mongo.process_files(json_data_path)
        print(f"Finished processing files for path: {json_data_path}")
