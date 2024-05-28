import configparser
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.DGIDBParser import DGIDBParser

class DGItoMongo:
    def __init__(self, config_path='../conf/drugkb_test.config'):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.db_name = self.config.get('dgidb', 'db_name')  # 直接读取唯一的 db_name
        self.data_paths = [self.config.get('dgidb', f'data_path_{i + 1}') for i in range(int(self.config.get('dgidb', 'data_path_num')))]
        self.col_names = [self.config.get('dgidb', f'col_name_{i + 1}') for i in range(int(self.config.get('dgidb', 'col_num')))]

    def parse_and_insert_all_entries(self, data_path, col_name):
        """ Parse data from the given path and insert all entries into MongoDB. """
        parser = DGIDBParser(self.config_path)
        data = parser.parse(data_path)

        if data:
            db_connection = DBconnection(self.config_path, self.db_name, col_name)
            db_connection.collection.insert_many(data)  # 插入所有条目
            print(f"All entries successfully inserted into MongoDB (db: {self.db_name}, collection: {col_name}) from {data_path}.")
            db_connection.collection.database.client.close()
        else:
            print(f"No data parsed from {data_path}.")

    def run(self):
        """ Read configuration and process all data paths. """
        for data_path, col_name in zip(self.data_paths, self.col_names):
            print(f"Processing data path: {data_path}")
            self.parse_and_insert_all_entries(data_path, col_name)

    def test(self):
        """ Test method to parse and insert the first entry of all data paths. """
        for data_path, col_name in zip(self.data_paths, self.col_names):
            print(f"Testing data path: {data_path}")
            parser = DGIDBParser(self.config_path)
            data = parser.parse(data_path)
            if data:
                first_entry = data[0]  # 获取第一个条目
                print(f"First entry from {data_path}: {first_entry}")
                db_connection = DBconnection(self.config_path, self.db_name, col_name)
                db_connection.collection.insert_one(first_entry)
                print(f"First entry successfully inserted into MongoDB (db: {self.db_name}, collection: {col_name}) from {data_path} for testing.")
                db_connection.collection.database.client.close()
            else:
                print(f"No data parsed from {data_path}.")

if __name__ == '__main__':
    dgi_to_mongo = DGItoMongo(config_path='../conf/drugkb_test.config')
    dgi_to_mongo.run()  # 正常入库方法，插入所有条目
    # dgi_to_mongo.test()  # 测试方法，入库所有解析路径的第一个条目
