import configparser
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.DoParser import DoParser

class DoToMongo:
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def insert_dict(self, data_dict):
        """Insert a single dictionary into MongoDB."""
        self.db_connection.collection.insert_one(data_dict)

    def close(self):
        """Close the MongoDB connection."""
        self.db_connection.my_db.client.close()  # 关闭数据库连接

if __name__ == '__main__':
    config = configparser.ConfigParser()
    cfgfile = '../conf/drugkb_test.config'
    config.read(cfgfile)

    for i in range(0, int(config.get('do', 'data_path_num'))):
        datapath = config.get('do', 'data_path_' + str(i + 1))
        parser = DoParser(datapath)

        db_connection = DBconnection(cfgfile, config.get('do', 'db_name'), config.get('do', 'col_name_' + str(i + 1)))
        mongo_saver = DoToMongo(db_connection)

        for data_dict in parser.parse():
            mongo_saver.insert_dict(data_dict)  # Insert each parsed dict into MongoDB
            break  # Just for testing, insert only the first dictionary

        mongo_saver.close()
        break  # 成功打印并插入后退出循环
