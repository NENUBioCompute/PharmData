import json
import configparser
from tqdm import tqdm
from PharmDataProject.DataParsers.BindingdbParser import BindingDbParser
from PharmDataProject.Utilities.Database.dbutils import DBconnection
import os
class BindingdbToMongo:
    def __init__(self):
        self.cfgfile = "../conf/drugkb_test.config"
        self.config = configparser.ConfigParser()
        self.config.read(self.cfgfile)

        self.db_name = self.config.get('bindingdb', 'db_name')
        self.col_name = self.config.get('bindingdb', 'col_name_1')
        self.db = DBconnection(self.cfgfile, self.db_name, self.col_name)
        self.parser = BindingDbParser()

    def save_mongodb(self):
        """
        Save data to MongoDB
        """
        # clear the collection

        # Print the TSV files

        self.db.collection.delete_many({})
        bindingdb_generator = self.parser.parse_bindingdb()

        # Reset generator
        bindingdb_generator = self.parser.parse_bindingdb()

        for row in tqdm(bindingdb_generator, desc="Inserting into MongoDB"):
            self.db.collection.insert_one(row)


if __name__ == '__main__':

    bindingdb_to_mongo = BindingdbToMongo()

    bindingdb_to_mongo.save_mongodb()


