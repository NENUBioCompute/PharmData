from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.UniprotParser import UniprotParser
import configparser
import os

class UniprotToMongo:
    def __init__(self,config_path):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(self.config_path)
        self.infile = self.config.get('uniprot', 'data_path_1') + 'uniprot_sprot.xml.gz'
        self.config = configparser.ConfigParser()
        self.config.read(self.config_path)
        self.db = DBconnection(
            self.config_path,
            self.config.get('uniprot', 'db_name'),
            self.config.get('uniprot', 'col_name_1')
        )

    def save_to_mongodb(self):
        uniprot_parser = UniprotParser()
        print('begin')
        for root, dirs, files in os.walk(os.path.dirname(self.infile)):
            for f in files:
                fi = os.path.join(root, f)

                for record in uniprot_parser.parse_xml_all(fi, 'entry'):
                    self.db.collection.insert_one(record['entry'])



    def save_batch_to_mongodb(self, records: list):
        if records:
            self.db.collection.insert_many(records)

if __name__ == '__main__':
    config_path = '../conf/drugkb_test.config'
    uniprot_to_mongo= UniprotToMongo()

    uniprot_to_mongo.save_to_mongodb(config_path)