import configparser
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.PharmRGKBParser import PharmRGKBParser

class Pharmgkb_Mongo:
    def __init__(self, config_path='../conf/drugkb_test.config'):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.section = 'pharmgkb'
        self.db_name = self.config.get(self.section, 'db_name')
        self.tables = self.config.get(self.section, 'tables')[1:-1].split(',')
        self.col_names = [self.config.get(self.section, f'col_name_{i + 1}') for i in range(int(self.config.get(self.section, 'col_num')))]

    def _convert_keys_to_str(self, data):
        """Recursively convert dictionary keys to strings."""
        if isinstance(data, list):
            return [self._convert_keys_to_str(item) for item in data]
        elif isinstance(data, dict):
            return {str(k): self._convert_keys_to_str(v) for k, v in data.items()}
        else:
            return data

    def parse_and_insert_all_entries(self, col_name):
        """Parse data from the given path and insert all entries into MongoDB."""
        pharmGKBParser = PharmRGKBParser(self.config_path)
        parsed_data = pharmGKBParser.start()

        table_name = self.tables[self.col_names.index(col_name)]
        data = parsed_data[table_name]

        db_connection = DBconnection(self.config_path, self.db_name, col_name)

        if isinstance(data, list):
            for entry in data:
                entry = self._convert_keys_to_str(entry)
                db_connection.collection.insert_one(entry)
        else:
            data = self._convert_keys_to_str(data)
            db_connection.collection.insert_one(data)

        print(f"All entries successfully inserted into MongoDB (db: {self.db_name}, collection: {col_name}).")
        db_connection.collection.database.client.close()

    def run(self):
        """Read configuration and process all data paths."""
        for col_name in self.col_names:
            print(f"Processing collection: {col_name}")
            self.parse_and_insert_all_entries(col_name)

    def test(self):
        """Test method to parse and insert the first entry of all data paths."""
        pharmGKBParser = PharmRGKBParser(self.config_path)
        parsed_data = pharmGKBParser.start()

        for col_name in self.col_names:
            print(f"Testing collection: {col_name}")

            table_name = self.tables[self.col_names.index(col_name)]
            data = parsed_data[table_name]

            if isinstance(data, list) and data:
                first_entry = data[0]
            else:
                first_entry = next(iter(data.values())) if isinstance(data, dict) else data

            if first_entry:
                first_entry = self._convert_keys_to_str(first_entry)
                print(f"First entry for collection {col_name}: {first_entry}")
                db_connection = DBconnection(self.config_path, self.db_name, col_name)
                db_connection.collection.insert_one(first_entry)
                print(f"First entry successfully inserted into MongoDB (db: {self.db_name}, collection: {col_name}) for testing.")
                db_connection.collection.database.client.close()
            else:
                print(f"No data parsed for collection: {col_name}.")

if __name__ == "__main__":
    config_path = '../conf/drugkb_test.config'
    pharmgkb_mongo = Pharmgkb_Mongo(config_path)
    pharmgkb_mongo.run()  # 正常入库方法，插入所有条目
    # pharmgkb_mongo.test()  # 测试方法，入库所有解析路径的第一个条目
