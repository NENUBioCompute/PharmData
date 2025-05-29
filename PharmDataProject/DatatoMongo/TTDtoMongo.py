import configparser
import time
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.TTDParser import TtdParser

class TTDtoMongo:
    def __init__(self, config_path='../conf/drugkb_test.config'):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.section = 'ttd'
        self.db_name = self.config.get(self.section, 'db_name')
        self.tables = self.config.get(self.section, 'tables')[1:-1].split(',')
        self.data_paths = [self.config.get(self.section, f'data_path_{i + 1}') for i in range(int(self.config.get(self.section, 'data_path_num')))]
        self.col_names = [self.config.get(self.section, f'col_name_{i + 1}') for i in range(int(self.config.get(self.section, 'col_num')))]

    def save_to_mongo(self, collection_name, data):
        db = DBconnection(self.config_path, self.db_name, collection_name)
        db.collection.insert_one(data)
    def parse_and_insert_all_entries(self, table_name, data_path, col_name):
        parser = TtdParser(config_path=self.config_path)
        for parsed_data in parser.parse(table_name):
            self.save_to_mongo(col_name, parsed_data)

    def run(self):
        for idx, table_name in enumerate(self.tables):
            data_path = self.data_paths[idx]
            col_name = self.col_names[idx]
            print(f"正在解析文件: {data_path}")
            self.parse_and_insert_all_entries(table_name, data_path, col_name)


if __name__ == '__main__':

    ttd_to_mongo = TTDtoMongo()
    ttd_to_mongo.run()  # 正常入库方法，插入所有条目
