import configparser
import os
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.BrendaParser import BRENDAParser

class BRENDAtoMongo:
    def __init__(self):
        self.cfgfile = "../conf/drugkb_test.config"
        self.config = configparser.ConfigParser()
        self.config.read(self.cfgfile)
        self.db_connection = DBconnection(
            self.cfgfile,
            self.config.get('brenda', 'db_name'),
            self.config.get('brenda', 'col_name_1')
        )
        self.fin_txt = os.path.join(self.config.get('brenda', 'data_path_1').replace('.tar.gz', ''))

    def insert_dict(self, data_dict):
        """ Insert a single dictionary into MongoDB. """
        self.db_connection.collection.insert_one(data_dict)

    def save_mongodb(self):
        parser = BRENDAParser()
        for data_dict in parser.doParse(self.fin_txt ):
            mongo_inserter.insert_dict(data_dict)  # Insert each parsed dict into MongoDB
            # break  # Just for testing, insert only the first dictionary


if __name__ == '__main__':
    # Initialize the parser and database inserter

    mongo_inserter = BRENDAtoMongo()
    mongo_inserter.save_mongodb()

