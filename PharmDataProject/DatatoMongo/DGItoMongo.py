import configparser
import os
import sys
import json
from PharmDataProject.Utilities.Database.dbutils import DBconnection

class DGItoMongo:
    def __init__(self, config_path='../conf/drugkb.config'):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.db_connection = DBconnection(
            config_path,
            self.config.get('dgidb', 'db_name'),
            self.config.get('dgidb', 'col_name_1')
        )

    def to_mongo(self):
        # Assuming the structure of the JSON path is set correctly in the config
        json_file_path = os.path.join(
            self.config.get('dgidb', 'json_path_1'),
            self.config.get('dgidb', 'data_path_1').split('/')[-1].replace('.csv', '.json')
        )

        if not os.path.exists(json_file_path):
            print(f"DGIDB file not found at {json_file_path}")
            sys.exit(1)
        else:
            with open(json_file_path, 'r', encoding="utf-8") as jf:
                data = json.load(jf)
                self.db_connection.collection.insert_many(data)
            print(f"Data successfully inserted into MongoDB from {json_file_path}")

    def close(self):
        self.db_connection.client.close()
        print("Database connection closed.")


if __name__ == '__main__':
    dgi_to_mongo = DGItoMongo()
    dgi_to_mongo.to_mongo()
    dgi_to_mongo.close()
