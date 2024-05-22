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

    def to_mongo(self, data_path):
        biogrid_parser = BiogridParser()
        dict_data_generator = biogrid_parser.parse(data_path)

        # 获取第一个条目
        first_entry = next(dict_data_generator)

        # 插入第一个条目
        self.db_connection.collection.insert_one(first_entry)
        print(f"Data successfully inserted into MongoDB from {data_path}")

    def close(self):
        # 关闭 MongoClient
        self.db_connection.collection.database.client.close()
        print("Database connection closed.")


if __name__ == '__main__':
    config_path = '../conf/drugkb_test.config'
    biogrid_to_mongo = BiogridToMongo(config_path=config_path)

    for i in range(int(biogrid_to_mongo.config.get('biogrid', 'data_path_num'))):
        data_path = biogrid_to_mongo.config.get('biogrid', 'data_path_' + str(i + 1))
        biogrid_to_mongo.to_mongo(data_path)

        # 只处理一个文件并插入一个条目，添加 break
        break

    biogrid_to_mongo.close()
