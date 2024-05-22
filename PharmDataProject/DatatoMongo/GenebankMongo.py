import configparser
import os
from PharmDataProject.Utilities.Database.dbutils import DBconnection
from PharmDataProject.DataParsers.GenbankParser import GenbankParser


class GenebankMongo:
    def __init__(self, config_path='../conf/drugkb_test.config'):
        self.config_path = config_path  # 添加 config_path 属性
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

    def genebank_to_mongo(self):
        for i in range(int(self.config.get('genebank', 'data_path_num'))):
            data_path = self.config.get('genebank', 'data_path_' + str(i + 1))
            db = DBconnection(self.config_path, self.config.get('genebank', 'db_name'),
                              self.config.get('genebank', 'col_name_' + str(i + 1)))

            # 创建解析器实例并调用解析函数
            parser = GenbankParser(data_path)
            for data_dict in parser.parse():
                db.collection.insert_one(data_dict)
                print(f"Data from {data_path} successfully inserted into MongoDB.")
                return db  # 插入第一个数据后返回数据库连接对象

    @staticmethod
    def close(db_connection):
        """ 关闭 MongoDB 连接 """
        db_connection.collection.database.client.close()
        print("数据库连接已关闭。")


if __name__ == '__main__':
    def ensure_unzipped(data_path):
        gz_files = [f for f in os.listdir(data_path) if f.endswith('.gz')]
        for gz_file in gz_files:
            target_file = os.path.join(data_path, gz_file[:-3])
            if not os.path.exists(target_file):
                print(f"Unzipping {gz_file}...")
                GenbankParser.un_gz(os.path.join(data_path, gz_file))
            else:
                print(f"File {target_file} already exists, skipping unzipping.")


    genebank_mongo = GenebankMongo()
    db_connection = None

    for i in range(int(genebank_mongo.config.get('genebank', 'data_path_num'))):
        data_path = genebank_mongo.config.get('genebank', 'data_path_' + str(i + 1))

        ensure_unzipped(data_path)  # 确保文件已解压

        db = DBconnection(genebank_mongo.config_path, genebank_mongo.config.get('genebank', 'db_name'),
                          genebank_mongo.config.get('genebank', 'col_name_' + str(i + 1)))

        # 创建解析器实例并调用解析函数
        parser = GenbankParser(data_path)
        for data_dict in parser.parse():
            db.collection.insert_one(data_dict)
            print(f"Data from {data_path} successfully inserted into MongoDB.")
            db_connection = db  # 保存数据库连接对象
            break  # 插入第一个数据后停止循环
        break  # 处理完第一个文件后停止循环

    if db_connection:
        GenebankMongo.close(db_connection)
