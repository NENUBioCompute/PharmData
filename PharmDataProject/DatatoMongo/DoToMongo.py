import configparser
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.DoParser import DoParser

class DoToMongo:
    def __init__(self, cfgfile):
        self.config = configparser.ConfigParser()
        self.config.read(cfgfile)
        self.db_connection = DBconnection(cfgfile, self.config.get('do', 'db_name'), self.config.get('do', 'col_name_1'))

    def insert_dict(self, data_dict):
        """Insert a single dictionary into MongoDB."""
        self.db_connection.collection.insert_one(data_dict)

    def close(self):
        """Close the MongoDB connection."""
        self.db_connection.my_db.client.close()  # 关闭数据库连接
    def save_to_mongo(self):
        for i in range(0, int(self.config.get('do', 'data_path_num'))):
            datapath = self.config.get('do', 'data_path_' + str(i + 1))
            parser = DoParser(datapath)

            mongo_saver = DoToMongo(cfgfile)

            for data_dict in parser.parse():
                mongo_saver.insert_dict(data_dict)  # Insert each parsed dict into MongoDB

            mongo_saver.close()

if __name__ == '__main__':

    cfgfile = '../conf/drugkb_test.config'


    dotomongo = DoToMongo(cfgfile)
    dotomongo.save_to_mongo()


