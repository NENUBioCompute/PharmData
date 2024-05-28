import configparser
from PharmDataProject.DataParsers.BiogridParser import BiogridParser
from PharmDataProject.Utilities.Database.dbutils import DBconnection


class BiogridToMongo:
    def __init__(self, config_path='../conf/drugkb_test.config'):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.db_connection = DBconnection(
            config_path,
            self.config.get('biogrid', 'db_name'),
            self.config.get('biogrid', 'col_name_1')
        )
        self.biogrid_parser = BiogridParser(config_path)

    def to_mongo(self, data_path):
        self.biogrid_parser.to_csv(data_path)
        dict_data_generator = self.biogrid_parser.parse(data_path)

        # 获取第一个条目并插入到 MongoDB
        for entry in dict_data_generator:
            self.db_connection.collection.insert_one(entry)
            print(f"Data successfully inserted into MongoDB from {data_path}")

    def close(self):
        # 关闭 MongoClient
        self.db_connection.collection.database.client.close()
        print("Database connection closed.")


if __name__ == '__main__':
    biogrid_to_mongo = BiogridToMongo()

    for i in range(int(biogrid_to_mongo.config.get('biogrid', 'data_path_num'))):
        data_path = biogrid_to_mongo.config.get('biogrid', 'data_path_' + str(i + 1))
        biogrid_to_mongo.to_mongo(data_path)

    print("Successfully finished!")

    biogrid_to_mongo.close()
