import configparser
from PharmDataProject.DataParsers.BiogridParser import BiogridParser
from PharmDataProject.Utilities.Database.dbutils import DBconnection


class BiogridToMongo:
    def __init__(self):
        self.confile = '../conf/drugkb_test.config'
        self.config = configparser.ConfigParser()
        self.config.read(self.confile)
        self.db_connection = DBconnection(
            self.confile,
            self.config.get('biogrid', 'db_name'),
            self.config.get('biogrid', 'col_name_1')
        )
        self.biogrid_parser = BiogridParser(self.confile)
        self.data_path = self.config.get('biogrid', 'data_path_1')

    def to_mongo(self):
        self.biogrid_parser.to_csv(self.data_path)
        dict_data_generator = self.biogrid_parser.parse(self.data_path)

        # 获取第一个条目并插入到 MongoDB
        for entry in dict_data_generator:
            self.db_connection.collection.insert_one(entry)

    def close(self):
        # 关闭 MongoClient
        self.db_connection.collection.database.client.close()
        print("Database connection closed.")


if __name__ == '__main__':
    biogrid_to_mongo = BiogridToMongo()
    biogrid_to_mongo.to_mongo()


