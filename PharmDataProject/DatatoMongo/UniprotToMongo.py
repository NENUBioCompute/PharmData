from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.UniprotParser import UniprotParser
import configparser
import os

class UniprotToMongo:
    def __init__(self, config_path: str):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.db = DBconnection(
            config_path,
            self.config.get('uniprot', 'db_name'),
            self.config.get('uniprot', 'col_name_1')
        )

    def save_to_mongodb(self, record: dict):
        self.db.collection.insert_one(record)

    def save_batch_to_mongodb(self, records: list):
        if records:
            self.db.collection.insert_many(records)

if __name__ == '__main__':
    config_path = '../conf/drugkb_test.config'
    config = configparser.ConfigParser()
    config.read(config_path)
    infile = config.get('uniprot', 'data_path_1') + 'uniprot_sprot.xml.gz'

    uniprot_parser = UniprotParser()
    uniprot_to_mongo = UniprotToMongo(config_path)

    print(f"Processing file: {infile}")
    for root, dirs, files in os.walk(os.path.dirname(infile)):
        for f in files:
            fi = os.path.join(root, f)
            print(f"Processing file: {fi}")
            for record in uniprot_parser.parse_xml_all(fi, 'entry'):
                print("First record parsed:", record)
                uniprot_to_mongo.save_to_mongodb(record['entry'])
                break
            break
