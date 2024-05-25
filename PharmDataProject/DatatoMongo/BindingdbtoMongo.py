import json
import configparser
from tqdm import tqdm
from PharmDataProject.DataParsers.BindingdbParser import BindingDbParser
from PharmDataProject.Utilities.Database.dbutils import DBconnection


class BindingdbToMongo:
    def __init__(self, config_path):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.path = self.config.get('bindingdb', 'data_path_1')
        self.db_name = self.config.get('bindingdb', 'db_name')
        self.col_name = self.config.get('bindingdb', 'col_name_1')
        self.db = DBconnection(config_path, self.db_name, self.col_name)
        self.parser = BindingDbParser()

    def save_mongodb(self):
        """
        Save data to MongoDB
        """
        # clear the collection
        self.db.collection.delete_many({})
        bindingdb_generator = self.parser.parse_bindingdb(path=self.path)

        # Count total items to be inserted
        total_items = sum(1 for _ in bindingdb_generator)

        # Reset generator
        bindingdb_generator = self.parser.parse_bindingdb(path=self.path)

        for row in tqdm(bindingdb_generator, total=total_items, desc="Inserting into MongoDB"):
            self.db.collection.insert_one(row)


if __name__ == '__main__':
    cfgfile = "../conf/drugkb_test.config"
    bindingdb_to_mongo = BindingdbToMongo(cfgfile)

    # Test by inserting only one document and then break
    bindingdb_generator = bindingdb_to_mongo.parser.parse_bindingdb(path=bindingdb_to_mongo.path)
    bindingdb_to_mongo.db.collection.delete_many({})
    for i, row in enumerate(bindingdb_generator):
        bindingdb_to_mongo.db.collection.insert_one(row)
        print(f"Inserted first document: {row}")
        break

    # Uncomment the following line for full insertion with progress bar
    # bindingdb_to_mongo.save_mongodb()
