import configparser
import os
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.BrendaParser import BRENDAParser

class BRENDAtoMongo:
    def __init__(self, config_path='../conf/drugkb_test.config'):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.db_connection = DBconnection(
            config_path,
            self.config.get('brenda', 'db_name'),
            self.config.get('brenda', 'col_name_1')
        )

    def insert_dict(self, data_dict):
        """ Insert a single dictionary into MongoDB. """
        self.db_connection.collection.insert_one(data_dict)

    def close(self):
        self.db_connection.my_db.client.close()
        print("Database connection closed.")


if __name__ == '__main__':
    # Initialize the parser and database inserter
    parser = BRENDAParser()
    mongo_inserter = BRENDAtoMongo()

    # Parse the file and insert each dictionary into MongoDB
    config = configparser.ConfigParser()
    config.read('../conf/drugkb_test.config')
    fin_txt = os.path.join(config.get('brenda', 'data_path_1').replace('.tar.gz', ''))

    for data_dict in parser.doParse(fin_txt):
        mongo_inserter.insert_dict(data_dict)  # Insert each parsed dict into MongoDB
        break  # Just for testing, insert only the first dictionary

    mongo_inserter.close()
