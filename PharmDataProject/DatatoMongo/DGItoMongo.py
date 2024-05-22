import configparser
import os
import sys
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.DGIDBParser import DGIDBParser

class DGItoMongo:
    def __init__(self, config_path='../conf/drugkb_test.config'):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.db_connection = DBconnection(
            config_path,
            self.config.get('dgidb', 'db_name'),
            self.config.get('dgidb', 'col_name_1')
        )

    def insert_data(self, data):
        """ Insert data into MongoDB. """
        self.db_connection.collection.insert_many(data)
        print(f"Data successfully inserted into MongoDB.")

    def close(self):
        # Use the correct method to close the connection
        self.db_connection.collection.database.client.close()
        print("Database connection closed.")

if __name__ == '__main__':
    # Initialize the parser and database inserter
    config = configparser.ConfigParser()
    config.read('../conf/drugkb_test.config')

    for i in range(int(config.get('dgidb', 'data_path_num'))):
        data_path = config.get('dgidb', 'data_path_' + str(i + 1))
        print(f"Processing data path: {data_path}")

        parser = DGIDBParser(data_path)
        data = parser.parse()

        if data:
            dgi_to_mongo = DGItoMongo()
            dgi_to_mongo.insert_data(data)  # Insert parsed data into MongoDB
            dgi_to_mongo.close()
            break  # Process only the first dataset for this example
        else:
            print("No data parsed.")
